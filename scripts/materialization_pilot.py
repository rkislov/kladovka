#!/usr/bin/env python3
"""Verify complete consensus cache catch-up on three disposable loopback processes."""
import argparse
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timezone
import hashlib
import json
from pathlib import Path
import platform
import subprocess
import tempfile
import time

from load_pilot import servers, require


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--binary', default='./bin/kladovka')
    parser.add_argument('--objects', type=int, default=256)
    parser.add_argument('--timeout', type=float, default=180)
    parser.add_argument('--output', required=True)
    args = parser.parse_args()
    require(16 <= args.objects <= 5000, 'objects must be 16..5000')
    require(1 <= args.timeout <= 1800, 'timeout must be 1..1800 seconds')
    binary = Path(args.binary).resolve()
    output = Path(args.output).resolve()
    report = {'started_utc': datetime.now(timezone.utc).isoformat(), 'status': 'running',
              'binary_sha256': hashlib.sha256(binary.read_bytes()).hexdigest(),
              'binary_version': subprocess.check_output([str(binary), '-version'], text=True).strip(),
              'harness_sha256': hashlib.sha256(Path(__file__).read_bytes()).hexdigest(),
              'fixture_sha256': hashlib.sha256(Path(__file__).with_name('load_pilot.py').read_bytes()).hexdigest(),
              'host': platform.platform(), 'objects': args.objects, 'object_bytes': 4096,
              'concurrency': 4, 'tls': False, 'audit': False, 'phases': []}
    # Reserve before any work: never overwrite a previous report.
    with output.open('x') as destination:
        try:
            with tempfile.TemporaryDirectory(prefix='kladovka-materialization-') as temporary:
                root = Path(temporary)
                controls = {}
                with servers(binary, 'consensus', root, 4096, controls) as (client, _, topology, _):
                    report['topology'] = topology
                    target = topology['target_node']
                    # Keep the client's routing node alive while a different follower is offline.
                    offline = next(i for i in range(3) if i not in (target, topology['initial_leader']))
                    online = True
                    client.create_bucket(Bucket='catchup')
                    expected = {}
                    deleted = set()

                    def put(key, body, content_type='application/octet-stream'):
                        response = client.put_object(Bucket='catchup', Key=key, Body=body, ContentType=content_type)
                        return {'body': body, 'etag': response['ETag'], 'content_type': content_type}

                    def watermark():
                        states = [controls['admin'](i, '/cluster')['consensus'] for i in range(3)
                                  if i != offline or online]
                        leaders = [s for s in states if s['state'] == 'Leader']
                        require(len(leaders) == 1, 'expected one leader at write boundary')
                        return leaders[0]['applied_index']

                    def wait_caught_up(label, minimum_index, start=None):
                        start = time.monotonic() if start is None else start
                        observations = []
                        phase = {'name': label, 'minimum_applied_index': minimum_index, 'observations': observations}
                        report['phases'].append(phase)
                        deadline = start + args.timeout
                        stable = 0
                        while time.monotonic() < deadline:
                            states = []
                            try:
                                states = [controls['admin'](i, '/cluster')['consensus'] for i in range(3)]
                                ready = all(s['applied_index'] >= minimum_index and s['repair_pending'] == 0
                                            and not s['repair_error'] for s in states)
                                if ready:
                                    controls['admin'](target, '/api/polki')
                            except (OSError, ValueError, KeyError):
                                ready = False
                            observations.append({'elapsed_seconds': round(time.monotonic()-start, 3),
                                                 'nodes': [{k: s[k] for k in ('applied_index', 'repair_pending', 'repair_error')}
                                                           for s in states]})
                            stable = stable + 1 if ready else 0
                            if stable >= 2:
                                # Independent physical verification, not another authoritative GET.
                                for i in range(3):
                                    shelf = root / f'data-{i}' / 'catchup'
                                    actual = {p.name for p in shelf.iterdir() if not p.name.endswith('.kladovka-meta.json')}
                                    require(actual == set(expected), f'node {i}: unexpected/missing cache keys')
                                    for key, value in expected.items():
                                        body = (shelf / key).read_bytes()
                                        require(body == value['body'], f'node {i}: bytes differ for {key}')
                                        meta = json.loads((shelf / (key+'.kladovka-meta.json')).read_text())
                                        require(meta['sha256'] == hashlib.sha256(body).hexdigest(), 'cache hash differs')
                                        require(meta['etag'] == value['etag'] and meta['content_type'] == value['content_type'],
                                                'cache metadata differs')
                                    require(not any((shelf / key).exists() for key in deleted), 'deleted object resurrected')
                                phase.update(seconds=round(time.monotonic()-start, 3), verified_nodes=3,
                                             verified_objects_per_node=len(expected), verified_tombstones_per_node=len(deleted))
                                print(f'PASS {label}: {phase["seconds"]}s, {len(expected)} live objects/node', flush=True)
                                return
                            time.sleep(0.2)
                        raise RuntimeError('catch-up timed out: '+label)

                    for i in range(8):
                        key = f'seed-{i:03}'
                        expected[key] = put(key, (f'seed-{i}'.encode()*1024)[:4096])
                    wait_caught_up('seed', watermark())
                    controls['stop'](offline)
                    online = False
                    def write(index):
                        key = f'item-{args.objects-index:05}'
                        body = (hashlib.sha256(key.encode()).digest()*128)
                        return key, put(key, body)
                    with ThreadPoolExecutor(max_workers=4) as pool:
                        expected.update(pool.map(write, range(args.objects)))
                    # Same bytes with a different Content-Type must update the local sidecar.
                    key = 'seed-000'
                    expected[key] = put(key, expected[key]['body'], 'text/plain')
                    for key in ('seed-001', 'seed-002', 'seed-003'):
                        client.delete_object(Bucket='catchup', Key=key)
                        deleted.add(key)
                        del expected[key]
                    boundary = watermark()
                    resumed = time.monotonic()
                    controls['start'](offline)
                    online = True
                    wait_caught_up('offline replica catch-up after writes stop', boundary, resumed)
                    # A complete cluster restart discards every in-memory acknowledgement.
                    for i in range(3):
                        controls['stop'](i)
                    (root / f'data-{offline}' / 'catchup' / 'seed-000').unlink()
                    resumed = time.monotonic()
                    for i in range(3):
                        controls['start'](i)
                    wait_caught_up('full restart and missing cache file recovery', boundary, resumed)
                    # Verify the authoritative read path still serves all committed bytes.
                    for key, value in expected.items():
                        response = client.get_object(Bucket='catchup', Key=key)
                        with response['Body'] as body:
                            require(body.read() == value['body'], 'authoritative GET differs')
            report['status'] = 'passed'
        except Exception as error:
            report['status'] = 'failed'
            report['error'] = str(error)
            raise
        finally:
            report['finished_utc'] = datetime.now(timezone.utc).isoformat()
            json.dump(report, destination, ensure_ascii=False, indent=2)
            destination.write('\n')


if __name__ == '__main__':
    main()
