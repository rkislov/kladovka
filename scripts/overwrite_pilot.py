#!/usr/bin/env python3
"""Fixed-key unique overwrites, memory sampling and durable replica verification."""
import argparse
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timezone
import hashlib
import json
from pathlib import Path
import platform
import shutil
import subprocess
import tempfile
import time

from botocore.exceptions import ClientError, BotoCoreError

from load_pilot import servers, require, process_usage
from workload_pilot import payload, identity


def run(args, report, root):
    controls = {}
    with servers(Path(args.binary).resolve(), 'consensus', root, args.size, controls) as (client, _, topology, _):
        report['topology'] = topology
        client.create_bucket(Bucket='plateau')
        origin = time.monotonic()
        expected = {}
        samples = report['samples'] = []
        checks = report['checks'] = []

        def sample(phase):
            rss, cpu = process_usage(controls['pids']())
            nodes = []
            for i in range(3):
                base = root / f'data-{i}' / '.consensus'
                chunks = list((base / 'chunks').iterdir())
                sizes = []
                for path in chunks:
                    try:
                        sizes.append(path.stat().st_size)
                    except FileNotFoundError:
                        pass  # Concurrent prune; this is an approximate resource sample.
                nodes.append(dict(chunks=len(sizes), chunk_bytes=sum(sizes),
                                  raft_bytes=(base / 'raft.db').stat().st_size))
            samples.append(dict(seconds=time.monotonic()-origin, phase=phase, rss_mib=rss/1024,
                                cpu_seconds=cpu, nodes=nodes))

        def states():
            return [controls['admin'](i, '/cluster')['consensus'] for i in range(3)]

        def boundary():
            leaders = [s for s in states() if s['state'] == 'Leader']
            require(len(leaders) == 1, 'expected one leader')
            return leaders[0]['applied_index']

        def verify(label, minimum):
            start, stable = time.monotonic(), 0
            while time.monotonic()-start < 180:
                try:
                    current = states()
                    ready = all(s['applied_index'] >= minimum and not s['repair_pending'] and not s['repair_error'] for s in current)
                    if ready:
                        client.head_bucket(Bucket='plateau')
                except (OSError, ValueError, KeyError, ClientError, BotoCoreError):
                    ready = False
                stable = stable+1 if ready else 0
                if stable == 2:
                    break
                time.sleep(.2)
            require(stable == 2, 'replica catch-up timed out: '+label)
            for key, meta in expected.items():
                for i in range(3):
                    path = root / f'data-{i}' / 'plateau' / key
                    require(hashlib.sha256(path.read_bytes()).hexdigest() == meta['sha256'], 'physical replica differs')
                    sidecar = json.loads(path.with_name(key+'.kladovka-meta.json').read_text())
                    require(sidecar['etag'] == meta['etag'] and sidecar['sha256'] == meta['sha256'], 'sidecar differs')
                response = client.get_object(Bucket='plateau', Key=key)
                with response['Body'] as stream:
                    require(hashlib.file_digest(stream, 'sha256').hexdigest() == meta['sha256'], 'GET differs')
                require(response['ETag'] == meta['etag'], 'GET ETag differs')
            listing = client.list_objects_v2(Bucket='plateau')
            require(not listing.get('IsTruncated') and {x['Key'] for x in listing.get('Contents', [])} == set(expected), 'LIST differs')
            checks.append(dict(label=label, seconds=time.monotonic()-start, boundary=minimum,
                               copies=len(expected)*3, gets=len(expected), states=current))
            print('verified '+label, flush=True)

        generation = 0
        def wave():
            nonlocal generation
            generation += 1
            def put(i):
                key = f'key-{i:04}'
                body = payload(f'overwrite/{generation}/{i}', args.size)
                meta = identity(body)
                response = client.put_object(Bucket='plateau', Key=key, Body=body)
                require(response['ETag'] == meta['etag'], 'PUT ETag differs')
                return key, meta
            with ThreadPoolExecutor(max_workers=4) as pool:
                expected.update(pool.map(put, range(args.keys)))

        sample('start')
        deadline = time.monotonic()+args.seconds
        while time.monotonic() < deadline:
            wave()
            sample('write')
            print(json.dumps(dict(generation=generation, **samples[-1])), flush=True)
        report['generations'] = generation
        verify('continuous overwrites', boundary())
        deadline = time.monotonic()+args.idle_seconds
        while time.monotonic() < deadline:
            sample('idle')
            time.sleep(min(2, max(0, deadline-time.monotonic())))
        sample('idle')
        verify('idle', boundary())
        if platform.system() == 'Darwin':
            report['vmmap'] = [subprocess.run(['vmmap', '-summary', str(pid)], text=True, capture_output=True, timeout=30).__dict__
                               for pid in controls['pids']()]
        leader = next(i for i, s in enumerate(states()) if s['state'] == 'Leader')
        offline = next(i for i in range(3) if i not in (topology['target_node'], leader))
        controls['stop'](offline)
        # The failure transition is outside the measured write phase. Require a
        # routable quorum before issuing new writes; never retry measured PUTs.
        deadline = time.monotonic()+30
        while True:
            try:
                client.head_bucket(Bucket='plateau')
                break
            except (ClientError, BotoCoreError):
                require(time.monotonic() < deadline, 'offline quorum not ready')
                time.sleep(.2)
        wave()
        wave()
        live_states = [controls['admin'](i, '/cluster')['consensus'] for i in range(3) if i != offline]
        leaders = [s for s in live_states if s['state'] == 'Leader']
        require(len(leaders) == 1, 'offline quorum has no leader')
        minimum = leaders[0]['applied_index']
        controls['start'](offline)
        verify('offline follower latest generations', minimum)
        minimum = boundary()
        for i in range(3):
            controls['stop'](i)
        (root / f'data-{offline}' / 'plateau' / 'key-0000').unlink()
        for i in range(3):
            controls['start'](i)
        verify('full restart and missing cache recovery', minimum)
        report['total_generations'] = generation


def main():
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument('--binary', required=True)
    p.add_argument('--output', required=True)
    p.add_argument('--keys', type=int, default=32)
    p.add_argument('--size', type=int, default=1 << 20)
    p.add_argument('--seconds', type=int, default=600)
    p.add_argument('--idle-seconds', type=int, default=75)
    args = p.parse_args()
    require(4 <= args.keys <= 256 and 1 <= args.size <= 1 << 20 and 1 <= args.seconds <= 3600 and 1 <= args.idle_seconds <= 300, 'invalid workload')
    binary = Path(args.binary).resolve()
    report = dict(status='running', started_utc=datetime.now(timezone.utc).isoformat(), host=platform.platform(),
                  settings=vars(args), binary_sha256=hashlib.sha256(binary.read_bytes()).hexdigest(),
                  harness_sha256=hashlib.sha256(Path(__file__).read_bytes()).hexdigest())
    with Path(args.output).open('x') as out:
        try:
            with tempfile.TemporaryDirectory(prefix='kladovka-overwrite-') as tmp:
                require(shutil.disk_usage(tmp).free > 12 << 30, 'need 12 GiB temporary space')
                run(args, report, Path(tmp))
            report['status'] = 'passed'
        except BaseException as error:
            report.update(status='failed', error=str(error))
            raise
        finally:
            report['finished_utc'] = datetime.now(timezone.utc).isoformat()
            json.dump(report, out, indent=2)
            out.write('\n')


if __name__ == '__main__':
    main()
