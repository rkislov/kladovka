#!/usr/bin/env python3
"""Cold/mixed LIST and unique large-object workloads on disposable servers only."""
import argparse
from datetime import datetime, timezone
import hashlib
from importlib.metadata import version
import json
import os
from pathlib import Path
import platform
import shutil
import subprocess
import tempfile
import threading
import time

from load_pilot import ValidationError, measure, percentile, require, servers


def payload(key, size):
    # Different deterministic bytes in every object AND every 256KiB chunk.
    return hashlib.shake_256(key.encode()).digest(size)


def identity(body):
    return {'size': len(body), 'sha256': hashlib.sha256(body).hexdigest(),
            'etag': '"'+hashlib.md5(body, usedforsecurity=False).hexdigest()+'"'}


def validate_listing(response, keys, identities=None, own=None, grouped=False):
    if grouped:
        wanted = sorted({key[:key.index('/', len('catalog/'))+1] for key in keys})
        actual = [value['Prefix'] for value in response.get('CommonPrefixes', [])]
        require(not response.get('Contents'), 'unexpected grouped objects')
    else:
        wanted = keys
        actual = [value['Key'] for value in response.get('Contents', [])]
        require(not response.get('CommonPrefixes'), 'unexpected flat groups')
        for value in response.get('Contents', []):
            require(value['Size'] == 4096, 'LIST object size differs')
            if identities is not None:
                require(value['ETag'] in identities[value['Key']], 'LIST unknown generation')
            if own and value['Key'] == own[0]:
                require(value['ETag'] == own[1], 'LIST lost acknowledged overwrite')
    require(actual == wanted[:100], 'LIST keys/order differ')
    require(response['KeyCount'] == min(100, len(wanted)), 'LIST KeyCount differs')
    require(response['IsTruncated'] == (len(wanted) > 100), 'LIST truncation differs')


def summarize_events(events, operation):
    selected = [e for e in events if e['operation'] == operation]
    good = [e['ms'] for e in selected if e['error'] is None]
    errors = {}
    for event in selected:
        if event['error']:
            errors[event['error']] = errors.get(event['error'], 0)+1
    writes = [e for e in events if e['operation'] == 'PUT']
    return {'requests': len(selected), 'successes': len(good), 'errors': errors,
            'p50_ms': percentile(good, 50), 'p95_ms': percentile(good, 95),
            'p99_ms': percentile(good, 99),
            'overlapped_put_requests': sum(any(w['start_seconds'] < e['end_seconds'] and
                 e['start_seconds'] < w['end_seconds'] for w in writes) for e in selected)
                 if operation != 'PUT' else None,
            'samples': selected}


def run_mode(binary, mode, root, args, run):
    controls = {}
    with servers(binary, mode, root, max(args.large_bytes), controls) as (client, _, topology, _):
        run.update(topology)
        client.create_bucket(Bucket='pilot')
        keys = sorted(f'catalog/g-{i % 10:02}/object-{i:06}' for i in range(args.catalog_objects))
        bodies = {key: payload('initial/'+key, 4096) for key in keys}
        expected = {key: identity(body) for key, body in bodies.items()}

        def phase(name, count, concurrency, fn):
            result = measure(name, count, concurrency, fn, controls['pids']())
            run['phases'].append(result)
            print(f'{mode} {name} C={concurrency}: p95={result["p95_ms"]}ms '
                  f'RSS={result["rss_sum_peak_mib"]}MiB errors={result["errors"]}', flush=True)
            require(not result['errors'] and not result['resource_sampling_errors'], 'phase failed: '+name)
            return result

        def put(key, body):
            response = client.put_object(Bucket='pilot', Key=key, Body=body,
                                         ContentType='application/octet-stream')
            require(response['ETag'] == expected_etags[key], 'PUT ETag differs')

        expected_etags = {key: meta['etag'] for key, meta in expected.items()}
        phase('catalog PUT seed', len(keys), 4, lambda i: put(keys[i], bodies[keys[i]]))

        def listing(grouped=False, allowed=None, own=None):
            options = {'Bucket': 'pilot', 'Prefix': 'catalog/', 'MaxKeys': 100}
            if grouped:
                options['Delimiter'] = '/'
            response = client.list_objects_v2(**options)
            validate_listing(response, keys, allowed, own, grouped)

        def restart():
            for i in range(topology['nodes']):
                controls['stop'](i)
            for i in range(topology['nodes']):
                controls['start'](i)
            deadline = time.monotonic()+60
            while True:
                try:
                    for i in range(topology['nodes']):
                        controls['admin'](i, '/api/polki')
                    return
                except (OSError, ValueError):
                    if time.monotonic() >= deadline:
                        raise RuntimeError('restart readiness timed out')
                    time.sleep(0.1)

        allowed = {key: {meta['etag']} for key, meta in expected.items()}
        for sample in range(args.cold_samples):
            restart()
            # No LIST warm-up: readiness enumerates shelves, not objects.
            result = phase('first LIST after process restart', 1, 1,
                           lambda _: listing(allowed=allowed))
            result['restart_number'] = sample+1
        for concurrency in (1, 4):
            for grouped in (False, True):
                listing(grouped, allowed)
                phase('warm LIST '+('grouped' if grouped else 'flat'), args.samples, concurrency,
                      lambda _, grouped=grouped: listing(grouped, allowed))

        def mutations(name, cycles, concurrency):
            chosen = keys[:cycles]  # Each cycle owns a different key on the first page.
            new_bodies = {key: payload(name+'/'+key, 4096) for key in chosen}
            newer = {key: identity(body) for key, body in new_bodies.items()}
            allowed = {key: {meta['etag']} for key, meta in expected.items()}
            for key, meta in newer.items():
                allowed[key].add(meta['etag'])
            # Shared dictionaries are read-only while workers run.
            expected_etags.update({key: meta['etag'] for key, meta in newer.items()})
            events, lock, epoch = [], threading.Lock(), time.perf_counter()

            def timed(operation, fn):
                start, error = time.perf_counter(), None
                try:
                    fn()
                except Exception as exc:
                    error = type(exc).__name__
                    raise
                finally:
                    end = time.perf_counter()
                    with lock:
                        events.append({'operation': operation, 'start_seconds': start-epoch,
                                       'end_seconds': end-epoch, 'ms': (end-start)*1000, 'error': error})

            def cycle(index):
                key = chosen[index]
                timed('PUT', lambda: put(key, new_bodies[key]))
                timed('LIST flat', lambda: listing(False, allowed, (key, newer[key]['etag'])))
                timed('LIST grouped', lambda: listing(True, allowed))

            try:
                result = phase(name, cycles, concurrency, cycle)
            finally:
                # Preserve individual timings even if phase validation fails.
                if run['phases'] and run['phases'][-1]['operation'] == name:
                    run['phases'][-1]['components'] = {op: summarize_events(events, op)
                                                      for op in ('PUT', 'LIST flat', 'LIST grouped')}
            expected.update(newer)
            bodies.update(new_bodies)
            listing(False, {key: {meta['etag']} for key, meta in expected.items()})
            return result

        mutations('sequential PUT then LIST', min(args.samples, len(keys), 100), 1)
        mutations('mixed PUT and LIST', min(args.mixed_cycles, len(keys), 100), 4)

        # Separate concurrent create/delete cycles exercise changing membership.
        # Each worker owns a prefix, making exact LIST assertions possible without
        # pretending that a multi-page traversal is a snapshot during writes.
        events, lock, epoch = [], threading.Lock(), time.perf_counter()
        churn_count = min(args.mixed_cycles, 20)
        churn_bodies = [payload(f'churn/{i}', 4096) for i in range(churn_count)]
        def churn(index):
            prefix = f'churn/worker-{index:03}/'
            key = prefix+'object'
            etag = identity(churn_bodies[index])['etag']
            def timed(op, fn):
                start, error = time.perf_counter(), None
                try:
                    fn()
                except Exception as exc:
                    error = type(exc).__name__
                    raise
                finally:
                    end = time.perf_counter()
                    with lock:
                        events.append({'operation': op, 'start_seconds': start-epoch,
                                       'end_seconds': end-epoch, 'ms': (end-start)*1000, 'error': error})
            def check(present):
                response = client.list_objects_v2(Bucket='pilot', Prefix=prefix, MaxKeys=100)
                contents = response.get('Contents', [])
                require([v['Key'] for v in contents] == ([key] if present else []), 'churn visibility differs')
                require(response['KeyCount'] == int(present) and not response['IsTruncated']
                        and not response.get('CommonPrefixes'), 'churn page shape differs')
                if present:
                    require(contents[0]['ETag'] == etag and contents[0]['Size'] == 4096, 'churn metadata differs')
            timed('PUT', lambda: client.put_object(Bucket='pilot', Key=key, Body=churn_bodies[index]))
            timed('LIST present', lambda: check(True))
            timed('DELETE', lambda: client.delete_object(Bucket='pilot', Key=key))
            timed('LIST absent', lambda: check(False))
        try:
            phase('mixed create-delete LIST', churn_count, 4, churn)
        finally:
            if run['phases'] and run['phases'][-1]['operation'] == 'mixed create-delete LIST':
                run['phases'][-1]['components'] = {op: summarize_events(events, op)
                    for op in ('PUT', 'LIST present', 'DELETE', 'LIST absent')}
        require(not client.list_objects_v2(Bucket='pilot', Prefix='churn/').get('Contents'),
                'churn keys remain after writers stopped')

        def get(key, meta):
            response = client.get_object(Bucket='pilot', Key=key)
            digest, size = hashlib.sha256(), 0
            with response['Body'] as stream:
                while chunk := stream.read(1 << 20):
                    digest.update(chunk)
                    size += len(chunk)
            require(size == meta['size'] and digest.hexdigest() == meta['sha256'], 'GET bytes differ')
            require(response['ETag'] == meta['etag'], 'GET ETag differs')

        phase('verify overwritten catalog GET', min(args.mixed_cycles, len(keys), 100), 4,
              lambda i: get(keys[i], expected[keys[i]]))
        # Include the entire paginated catalogue, not only the mutated first page.
        def traverse(_):
            found, token, pages = [], None, 0
            while True:
                options = {'Bucket': 'pilot', 'Prefix': 'catalog/', 'MaxKeys': 100}
                if token:
                    options['ContinuationToken'] = token
                response = client.list_objects_v2(**options)
                for value in response.get('Contents', []):
                    require(value['ETag'] == expected[value['Key']]['etag'], 'final catalogue generation differs')
                    found.append(value['Key'])
                pages += 1
                require(pages <= (len(keys)+99)//100, 'pagination loop')
                if not response['IsTruncated']:
                    break
                token = response['NextContinuationToken']
            require(found == keys, 'final traversal skipped or repeated keys')
        phase('final catalogue traversal', 1, 1, traverse)
        del bodies

        large_manifest = {}
        for size in args.large_bytes:
            for concurrency in (1, 4):
                large_keys = [f'large/{size}/c-{concurrency}/object-{i:04}' for i in range(args.large_count)]
                # Generation and hashing are outside measured PUT; never reuse chunks across keys.
                large_bodies = [payload(key, size) for key in large_keys]
                metas = [identity(body) for body in large_bodies]
                expected_etags.update({key: meta['etag'] for key, meta in zip(large_keys, metas)})
                large_manifest.update(zip(large_keys, metas))
                result = phase(f'unique {size}B PUT', args.large_count, concurrency,
                               lambda i: put(large_keys[i], large_bodies[i]))
                result['payload_mib_per_second'] = round(size*result['successes_per_second']/(1 << 20), 3)
                del large_bodies
                result = phase(f'unique {size}B GET', args.large_count, concurrency,
                               lambda i: get(large_keys[i], metas[i]))
                result['payload_mib_per_second'] = round(size*result['successes_per_second']/(1 << 20), 3)
        run['large_manifest'] = large_manifest
        if mode == 'consensus':
            states = [controls['admin'](i, '/cluster')['consensus'] for i in range(3)]
            leaders = [s for s in states if s['state'] == 'Leader']
            require(len(leaders) == 1, 'expected a leader at drain boundary')
            boundary = leaders[0]['applied_index']
            start, stable = time.monotonic(), 0
            drain = {'minimum_applied_index': boundary, 'observations': []}
            run['drain'] = drain
            while time.monotonic()-start < 300:
                states = [controls['admin'](i, '/cluster')['consensus'] for i in range(3)]
                drain['observations'].append({'seconds': time.monotonic()-start,
                    'nodes': [{key: s[key] for key in ('applied_index', 'repair_pending', 'repair_error')} for s in states]})
                ready = all(s['applied_index'] >= boundary and s['repair_pending'] == 0 and not s['repair_error'] for s in states)
                stable = stable+1 if ready else 0
                if stable >= 2:
                    drain['seconds'] = time.monotonic()-start
                    break
                time.sleep(0.2)
            else:
                raise RuntimeError('materialization did not drain after large writes')
        # Verify all unique large local copies after writers have stopped.
        for node in range(topology['nodes']):
            for key, meta in large_manifest.items():
                path = root / f'data-{node}' / 'pilot' / key
                digest = hashlib.sha256()
                with path.open('rb') as stream:
                    while chunk := stream.read(1 << 20):
                        digest.update(chunk)
                require(path.stat().st_size == meta['size'] and digest.hexdigest() == meta['sha256'],
                        'large materialized replica differs')
        run['verified_large_copies'] = len(large_manifest)*topology['nodes']


def parse_args():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--binary', default='./bin/kladovka')
    parser.add_argument('--mode', choices=('legacy', 'consensus', 'both'), default='both')
    parser.add_argument('--catalog-objects', type=int, default=1000)
    parser.add_argument('--cold-samples', type=int, default=3)
    parser.add_argument('--samples', type=int, default=40)
    parser.add_argument('--mixed-cycles', type=int, default=80)
    parser.add_argument('--large-bytes', default='1048576,8388608')
    parser.add_argument('--large-count', type=int, default=8)
    parser.add_argument('--output', required=True)
    args = parser.parse_args()
    try:
        args.large_bytes = [int(size) for size in args.large_bytes.split(',')]
    except ValueError:
        parser.error('large-bytes must be comma-separated integers')
    if not args.large_bytes or sorted(set(args.large_bytes)) != args.large_bytes or not 1 <= min(args.large_bytes) <= max(args.large_bytes) <= 16 << 20:
        parser.error('large-bytes must be increasing, unique and 1..16MiB')
    if not (10 <= args.catalog_objects <= 10000 and 1 <= args.cold_samples <= 10 and
            1 <= args.samples <= 1000 and 1 <= args.mixed_cycles <= 100 and 1 <= args.large_count <= 32):
        parser.error('invalid workload bounds')
    if sum(args.large_bytes)*args.large_count*2 > 512 << 20:
        parser.error('large payload limited to 512MiB per node')
    return args


def main():
    args = parse_args()
    binary, output = Path(args.binary).resolve(), Path(args.output).resolve()
    report = {'schema': 1, 'status': 'running', 'started_utc': datetime.now(timezone.utc).isoformat(),
              'binary_version': subprocess.check_output([str(binary), '-version'], text=True).splitlines()[0],
              'binary_sha256': hashlib.sha256(binary.read_bytes()).hexdigest(),
              'harness_sha256': hashlib.sha256(Path(__file__).read_bytes()).hexdigest(),
              'fixture_sha256': hashlib.sha256(Path(__file__).with_name('load_pilot.py').read_bytes()).hexdigest(),
              'host': {'os': platform.platform(), 'logical_cpus': os.cpu_count()},
              'client': {'python': platform.python_version(), 'boto3': version('boto3'), 'botocore': version('botocore')},
              'settings': {k: v for k, v in vars(args).items() if k not in ('binary', 'output')},
              'method': 'Closed loop. Process restart, not OS cache eviction. First LIST has no warmup; startup excluded. '
                        'Mixed cycles: one overwrite then flat and grouped LIST; separate immutable key per cycle, stable key set. '
                        'Separate create/delete cycles use private prefixes and verify acknowledged presence/absence. '
                        'Subrequest intervals record actual PUT/LIST overlap. Large payloads SHAKE256 per key, generation excluded, '
                        'streaming SHA256 GET validation included. RSS=sum(server processes), sampled ~200ms; excludes client/OS cache. '
                        'TLS/audit off; SDK retries off. Client and all servers share one host/disk. No performance gate.',
              'runs': []}
    with output.open('x') as file:
        try:
            for mode in (['legacy', 'consensus'] if args.mode == 'both' else [args.mode]):
                with tempfile.TemporaryDirectory(prefix='kladovka-workload-') as temporary:
                    root = Path(temporary)
                    nodes = 3 if mode == 'consensus' else 1
                    logical = args.catalog_objects*12288 + sum(args.large_bytes)*args.large_count*2
                    require(shutil.disk_usage(root).free > logical*nodes*6+(512 << 20), 'insufficient disk budget')
                    run = {'mode': mode, 'phases': []}
                    report['runs'].append(run)
                    run_mode(binary, mode, root, args, run)
            report['status'] = 'passed'
        except BaseException as error:
            report.update(status='failed', failure={'type': type(error).__name__, 'message': str(error)})
            raise
        finally:
            report['finished_utc'] = datetime.now(timezone.utc).isoformat()
            json.dump(report, file, ensure_ascii=False, indent=2)
            file.write('\n')
            print('Report:', output, flush=True)


if __name__ == '__main__':
    main()
