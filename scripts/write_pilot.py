#!/usr/bin/env python3
"""Repeated sustained unique PUTs on disposable three-node consensus clusters."""
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
import threading
import time

from load_pilot import percentile, process_usage, require, servers
from workload_pilot import identity, payload


def run(binary, root, args, result):
    controls = {}
    with servers(binary, 'consensus', root, args.size, controls) as (client, pids, topology, _):
        result.update(topology)
        client.create_bucket(Bucket='pilot')
        # Client generation and checksums are excluded from PUT measurements.
        bodies = [payload(f'sustained/{i}', args.size) for i in range(args.objects + args.warmup)]
        metas = [identity(b) for b in bodies]
        keys = [f'objects/{i:06}' for i in range(len(bodies))]
        origin = time.monotonic()
        stopped = threading.Event()
        resources, observations, monitor_errors = [], [], []
        phase = ['warmup']

        def sample_resource(label):
            rss, cpu = process_usage(pids)
            resources.append(dict(seconds=time.monotonic()-origin, phase=label,
                                  rss_mib=rss/1024, cpu_seconds=cpu))

        def monitor():
            counter = 0
            while not stopped.is_set():
                try:
                    sample_resource(phase[0])
                    if counter % 4 == 0:
                        states = [controls['admin'](i, '/cluster')['consensus'] for i in range(3)]
                        observations.append(dict(seconds=time.monotonic()-origin, phase=phase[0],
                            nodes=[{k: s[k] for k in ('state', 'applied_index', 'repair_pending', 'repair_error')}
                                   for s in states]))
                    counter += 1
                except Exception as error:
                    monitor_errors.append(type(error).__name__)
                stopped.wait(0.5)

        def put(i):
            start = time.monotonic()
            error = None
            try:
                response = client.put_object(Bucket='pilot', Key=keys[i], Body=bodies[i])
                require(response['ETag'] == metas[i]['etag'], 'PUT ETag differs')
            except Exception as exc:
                error = type(exc).__name__
            end = time.monotonic()
            return dict(index=i, start=start-origin, end=end-origin, ms=(end-start)*1000, error=error)

        def writes(indices):
            with ThreadPoolExecutor(max_workers=4) as pool:
                return list(pool.map(put, indices))

        thread = threading.Thread(target=monitor, daemon=True)
        thread.start()
        try:
            warmup = writes(range(args.warmup))
            require(all(x['error'] is None for x in warmup), 'warmup failed')
            phase[0] = 'put'
            sample_resource('put')
            start = time.monotonic()
            samples = writes(range(args.warmup, len(bodies)))
            elapsed = time.monotonic()-start
            sample_resource('put')
            result['put'] = dict(seconds=elapsed, mib_per_second=args.objects*args.size/(1 << 20)/elapsed,
                p50_ms=percentile([s['ms'] for s in samples], 50),
                p95_ms=percentile([s['ms'] for s in samples], 95),
                p99_ms=percentile([s['ms'] for s in samples], 99), samples=samples)
            require(all(x['error'] is None for x in samples), 'PUT failed')
            phase[0] = 'drain'
            states = [controls['admin'](i, '/cluster')['consensus'] for i in range(3)]
            leaders = [s for s in states if s['state'] == 'Leader']
            require(len(leaders) == 1, 'no single leader')
            boundary = leaders[0]['applied_index']
            start, stable = time.monotonic(), 0
            while time.monotonic()-start < 300:
                states = [controls['admin'](i, '/cluster')['consensus'] for i in range(3)]
                ready = all(s['applied_index'] >= boundary and not s['repair_pending'] and not s['repair_error']
                            for s in states)
                stable = stable+1 if ready else 0
                if stable == 2:
                    break
                time.sleep(0.2)
            require(stable == 2, 'replicas did not drain')
            result['drain'] = dict(seconds=time.monotonic()-start, boundary=boundary, states=states)
            phase[0] = 'idle'
            # Observe normal runtime scavenging; no forced GC or process restart.
            stopped.wait(args.idle_seconds)
            sample_resource('idle')
            phase[0] = 'verify'
            for i, key in enumerate(keys):
                for node in range(3):
                    path = root / f'data-{node}' / 'pilot' / key
                    with path.open('rb') as stream:
                        digest = hashlib.file_digest(stream, 'sha256').hexdigest()
                    require(path.stat().st_size == args.size and digest == metas[i]['sha256'],
                            'replica content differs')
                response = client.get_object(Bucket='pilot', Key=key)
                with response['Body'] as stream:
                    digest = hashlib.file_digest(stream, 'sha256').hexdigest()
                require(digest == metas[i]['sha256'] and response['ETag'] == metas[i]['etag'], 'GET differs')
            result['verified_copies'] = len(keys)*3
            result['verified_gets'] = len(keys)
        finally:
            stopped.set()
            thread.join(timeout=15)
            require(not thread.is_alive(), 'monitor did not stop')
            result.update(resources=resources, observations=observations, monitor_errors=monitor_errors)
        require(not monitor_errors, 'resource monitoring failed')
        result['rss_peak_mib'] = max(x['rss_mib'] for x in resources)
        result['put_rss_peak_mib'] = max(x['rss_mib'] for x in resources if x['phase'] == 'put')
        result['idle_rss_last_mib'] = [x['rss_mib'] for x in resources if x['phase'] == 'idle'][-1]
        print(json.dumps({k: result[k] for k in ('rss_peak_mib', 'put_rss_peak_mib', 'idle_rss_last_mib')} |
                         {k: v for k, v in result['put'].items() if k != 'samples'}), flush=True)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--binary', required=True)
    parser.add_argument('--output', required=True)
    parser.add_argument('--runs', type=int, default=3)
    parser.add_argument('--objects', type=int, default=256)
    parser.add_argument('--warmup', type=int, default=16)
    parser.add_argument('--size', type=int, default=1 << 20)
    parser.add_argument('--idle-seconds', type=int, default=30)
    args = parser.parse_args()
    if not (1 <= args.runs <= 10 and 4 <= args.objects <= 1024 and 1 <= args.warmup <= 64 and
            1 <= args.size <= 8 << 20 and 1 <= args.idle_seconds <= 300 and
            (args.objects+args.warmup)*args.size <= 1 << 30):
        parser.error('invalid or excessive disposable workload')
    binary = Path(args.binary).resolve()
    report = dict(schema=1, status='running', started_utc=datetime.now(timezone.utc).isoformat(),
                  binary_version=subprocess.check_output([str(binary), '-version'], text=True).strip(),
                  binary_sha256=hashlib.sha256(binary.read_bytes()).hexdigest(),
                  harness_sha256=hashlib.sha256(Path(__file__).read_bytes()).hexdigest(),
                  fixture_sha256=hashlib.sha256(Path(__file__).with_name('load_pilot.py').read_bytes()).hexdigest(),
                  host=platform.platform(), settings={k: v for k, v in vars(args).items()
                                                    if k not in ('binary','output')}, runs=[])
    with Path(args.output).open('x') as out:
        try:
            for i in range(args.runs):
                with tempfile.TemporaryDirectory(prefix='kladovka-write-') as tmp:
                    root = Path(tmp)
                    require(shutil.disk_usage(root).free > (args.objects+args.warmup)*args.size*18+(1 << 30),
                            'insufficient temporary disk budget')
                    result = dict(number=i+1)
                    report['runs'].append(result)
                    print(f'run {i+1}/{args.runs}', flush=True)
                    run(binary, root, args, result)
            report['status'] = 'passed'
        except BaseException as error:
            report.update(status='failed', error=type(error).__name__+': '+str(error))
            raise
        finally:
            report['finished_utc'] = datetime.now(timezone.utc).isoformat()
            json.dump(report, out, indent=2)
            out.write('\n')


if __name__ == '__main__':
    main()
