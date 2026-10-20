#!/usr/bin/env python3
"""Bounded S3 load pilot on disposable loopback servers; never accepts an endpoint."""
import argparse
import base64
from concurrent.futures import ThreadPoolExecutor
from contextlib import contextmanager
from datetime import datetime, timezone
import hashlib
from importlib.metadata import version
import json
import math
import os
from pathlib import Path
import platform
import shutil
import socket
import subprocess
import tempfile
import threading
import time
from urllib.request import Request, ProxyHandler, build_opener

import boto3
from botocore.config import Config
from botocore.exceptions import ClientError


class ValidationError(Exception):
    pass


def require(condition, message):
    if not condition:
        raise ValidationError(message)


def percentile(values, percent):
    """Nearest-rank percentile; None for no successful requests."""
    if not values:
        return None
    ordered = sorted(values)
    return ordered[max(0, math.ceil(len(ordered) * percent / 100) - 1)]


def cpu_seconds(value):
    days, clock = value.split('-', 1) if '-' in value else ('0', value)
    parts = [float(part) for part in clock.split(':')]
    result = 0.0
    for part in parts:
        result = result * 60 + part
    return int(days) * 86400 + result


def process_usage(pids):
    result = subprocess.run(['ps', '-o', 'rss=', '-o', 'time=', '-p', ','.join(map(str, pids))],
                            capture_output=True, text=True, check=True, timeout=3)
    rows = [line.split() for line in result.stdout.splitlines() if line.strip()]
    require(len(rows) == len(pids), 'a server exited during resource sampling')
    return sum(int(row[0]) for row in rows), sum(cpu_seconds(row[1]) for row in rows)


def measure(name, count, concurrency, operation, pids):
    samples, resource_samples, sampling_errors = [], [], []
    stopped = threading.Event()

    def sample():
        try:
            rss, cpu = process_usage(pids)
            resource_samples.append((time.monotonic(), rss, cpu))
        except (OSError, subprocess.SubprocessError, ValidationError, ValueError):
            sampling_errors.append('resource_sample_failed')

    def monitor():
        while not stopped.wait(0.2):
            sample()

    def run_one(index):
        start = time.perf_counter()
        code = None
        try:
            operation(index)
        except ClientError as error:
            code = error.response['Error']['Code']
        except Exception as error:
            code = type(error).__name__
        return {'ms': round((time.perf_counter() - start) * 1000, 4), 'error': code}

    sample()
    monitor_thread = threading.Thread(target=monitor, daemon=True)
    monitor_thread.start()
    start = time.perf_counter()
    try:
        with ThreadPoolExecutor(max_workers=concurrency) as executor:
            samples = list(executor.map(run_one, range(count)))
    finally:
        elapsed = time.perf_counter() - start
        stopped.set()
        monitor_thread.join(timeout=4)
        sample()
    success = [s['ms'] for s in samples if s['error'] is None]
    errors = {}
    for s in samples:
        if s['error']:
            errors[s['error']] = errors.get(s['error'], 0) + 1
    return {'operation': name, 'requests': count, 'concurrency': concurrency,
            'successes': len(success), 'errors': errors, 'elapsed_seconds': round(elapsed, 4),
            'successes_per_second': round(len(success) / elapsed, 3),
            'p50_ms': percentile(success, 50), 'p95_ms': percentile(success, 95),
            'p99_ms': percentile(success, 99),
            'rss_sum_peak_mib': round(max(s[1] for s in resource_samples) / 1024, 3) if resource_samples else None,
            'server_cpu_seconds': round(resource_samples[-1][2] - resource_samples[0][2], 3) if len(resource_samples) >= 2 else None,
            'resource_samples': len(resource_samples), 'resource_sampling_errors': len(sampling_errors),
            'samples': samples}


@contextmanager
def servers(binary, mode, root, object_bytes):
    count = 1 if mode == 'legacy' else 3
    sockets, processes, logs = [], [], []
    access, secret = 'load-pilot', 'disposable-load-pilot-secret'
    try:
        for _ in range(count * 2):
            sock = socket.socket()
            sock.bind(('127.0.0.1', 0))
            sockets.append(sock)
        addresses = ['127.0.0.1:' + str(s.getsockname()[1]) for s in sockets]
        http, raft = addresses[:count], addresses[count:]
        peers = dict(zip(http, raft))
        for sock in sockets:
            sock.close()
        urls = ['http://' + address for address in http]
        for index in range(count):
            # Do not inherit production settings, credentials, proxies or audit paths.
            env = {k: os.environ[k] for k in ('PATH', 'LANG', 'LC_ALL') if k in os.environ}
            env.update({'CLUSTER_MODE': mode, 'DATA_ROOT': str(root / f'data-{index}'),
                        'LISTEN_ADDR': http[index], 'NODE_ID': http[index], 'NODE_URL': urls[index],
                        'ACCESS_KEY': access, 'SECRET_KEY': secret, 'REGION': 'us-east-1',
                        'PEER_NODES': ','.join(url for i, url in enumerate(urls) if i != index),
                        'REPLICATION_FACTOR': str(count), 'CLUSTER_TOKEN': 'disposable-load-pilot-cluster-token',
                        'CLUSTER_VERIFY_TLS': 'true', 'HEALTH_INTERVAL_SEC': '1',
                        'RAFT_CLUSTER_ID': 'disposable-load-pilot', 'RAFT_BOOTSTRAP': str(mode == 'consensus').lower(),
                        'RAFT_LISTEN_ADDR': raft[index], 'RAFT_PEERS': json.dumps(peers),
                        'MAX_OBJECT_SIZE': str(max(object_bytes, 1 << 20)), 'MAX_TEMP_BYTES': str(max(object_bytes * 8, 16 << 20)),
                        'MAX_CONCURRENT_UPLOADS': '4', 'MIN_FREE_DISK_BYTES': str(256 << 20),
                        'VERIFY_OBJECT_CHECKSUM': 'true', 'CLEANUP_INTERVAL_SEC': '600'})
            log = (root / f'node-{index}.log').open('w')
            logs.append(log)
            processes.append(subprocess.Popen([str(binary)], cwd=root, env=env, stdout=log, stderr=log))
        auth = 'Basic ' + base64.b64encode(f'{access}:{secret}'.encode()).decode()

        admin_client = build_opener(ProxyHandler({}))

        def admin(index, path):
            with admin_client.open(Request(urls[index] + path, headers={'Authorization': auth}), timeout=2) as response:
                return json.load(response)

        deadline = time.monotonic() + 40
        leader = 0
        while True:
            require(all(p.poll() is None for p in processes), 'pilot server exited during startup')
            try:
                for i in range(count):
                    admin(i, '/api/polki')
                if mode == 'consensus':
                    leaders = [i for i in range(count) if admin(i, '/cluster')['consensus']['state'] == 'Leader']
                    require(len(leaders) == 1, 'waiting for a single leader')
                    leader = leaders[0]
                break
            except (OSError, ValueError, ValidationError):
                if time.monotonic() >= deadline:
                    raise RuntimeError('pilot readiness timed out')
                time.sleep(0.1)
        target = (leader + 1) % count if mode == 'consensus' else 0
        client = boto3.session.Session().client('s3', endpoint_url=urls[target],
                    aws_access_key_id=access, aws_secret_access_key=secret,
                    config=Config(signature_version='s3v4', region_name='us-east-1',
                        s3={'addressing_style': 'path'}, proxies={},
                        request_checksum_calculation='when_required', response_checksum_validation='when_required',
                        connect_timeout=3, read_timeout=30, max_pool_connections=8,
                        retries={'total_max_attempts': 1}))
        try:
            yield client, [p.pid for p in processes], {'nodes': count, 'replication_factor': count,
                  'route': 'follower to leader' if mode == 'consensus' else 'direct',
                  'initial_leader': leader, 'target_node': target}, lambda: admin(target, '/cluster')
        finally:
            client.close()
    finally:
        for sock in sockets:
            sock.close()
        for process in processes:
            if process.poll() is None:
                process.terminate()
        for process in processes:
            try:
                process.wait(timeout=35)
            except subprocess.TimeoutExpired:
                process.kill()
                process.wait(timeout=5)
        for log in logs:
            log.close()


def object_key(index):
    return f'catalog/group-{index % 100:03d}/object-{index:07d}'


def run_mode(binary, mode, root, args, report):
    data = bytes(range(256)) * (args.object_bytes // 256) + bytes(range(args.object_bytes % 256))
    with servers(binary, mode, root, args.object_bytes) as (client, pids, topology, cluster_status):
        report.update(topology)
        client.create_bucket(Bucket='pilot')
        previous = 0
        for total in args.stages:
            stage = {'objects': total, 'phases': []}
            report['stages'].append(stage)

            def phase(name, requests, concurrency, operation):
                result = measure(name, requests, concurrency, operation, pids)
                stage['phases'].append(result)
                print(f'{mode} N={total} {name} C={concurrency}: p95={result["p95_ms"]}ms '
                      f'RSS={result["rss_sum_peak_mib"]}MiB errors={result["errors"]}', flush=True)
                require(not result['errors'], f'{mode}/{name}: operation or validation failed; see JSON')
                require(result['resource_sampling_errors'] == 0, 'resource sampling failed; see JSON')
                return result

            phase('PUT seed', total - previous, 4,
                  lambda i: client.put_object(Bucket='pilot', Key=object_key(previous + i), Body=data))
            expected = {object_key(i) for i in range(total)}
            listing_expectations = {}
            for prefix, delimiter in [('catalog/', None), ('catalog/', '/'), ('catalog/group-000/', None)]:
                matching = sorted(key for key in expected if key.startswith(prefix))
                listing_expectations[prefix, delimiter] = (
                    sorted({prefix + key[len(prefix):].split(delimiter, 1)[0] + delimiter for key in matching})
                    if delimiter else matching)


            def get(index):
                response = client.get_object(Bucket='pilot', Key=object_key((index * 7919) % total))
                with response['Body'] as body:
                    require(body.read() == data, 'GET bytes differ')

            def listing(prefix, delimiter=None):
                options = {'Bucket': 'pilot', 'Prefix': prefix, 'MaxKeys': 100}
                if delimiter is not None:
                    options['Delimiter'] = delimiter
                response = client.list_objects_v2(**options)
                wanted = listing_expectations[prefix, delimiter]
                if delimiter:
                    actual = [p['Prefix'] for p in response.get('CommonPrefixes', [])]
                    require(not response.get('Contents'), 'unexpected object in grouped fixture')
                else:
                    actual = [o['Key'] for o in response.get('Contents', [])]
                    require(not response.get('CommonPrefixes'), 'unexpected group in flat listing')
                require(actual == wanted[:100], 'LIST contents differ')
                require(response['KeyCount'] == min(100, len(wanted)), 'LIST count differs')
                require(response['IsTruncated'] == (len(wanted) > 100), 'LIST truncation differs')
                return response

            # One warm-up per operation, excluded from latency samples.
            get(0)
            for prefix, delimiter in [('catalog/', None), ('catalog/', '/'), ('catalog/group-000/', None)]:
                listing(prefix, delimiter)
            for concurrency in (1, 4):
                phase('GET', args.samples * 4, concurrency, get)
                phase('LIST flat', args.samples, concurrency, lambda _: listing('catalog/'))
                phase('LIST grouped', args.samples, concurrency, lambda _: listing('catalog/', '/'))
                phase('LIST narrow', args.samples, concurrency, lambda _: listing('catalog/group-000/'))

            def traversal(_):
                found, token, pages, seen = [], None, 0, set()
                while True:
                    options = {'Bucket': 'pilot', 'Prefix': 'catalog/', 'MaxKeys': 100}
                    if token:
                        options['ContinuationToken'] = token
                    result = client.list_objects_v2(**options)
                    found.extend(o['Key'] for o in result.get('Contents', []))
                    pages += 1
                    require(pages <= math.ceil(total / 100) + 1, 'pagination loop')
                    if not result['IsTruncated']:
                        break
                    token = result['NextContinuationToken']
                    require(token not in seen, 'repeated cursor')
                    seen.add(token)
                require(found == sorted(expected), 'full traversal skipped or repeated keys')
                stage['traversal_pages'] = pages
            phase('LIST full traversal', 1, 1, traversal)
            stage['cluster_after'] = cluster_status().get('consensus', {}) if mode == 'consensus' else {}
            previous = total


def parse_args():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--binary', default='./bin/kladovka')
    parser.add_argument('--mode', choices=('legacy', 'consensus', 'both'), default='both')
    parser.add_argument('--stages', default='100,1000,5000')
    parser.add_argument('--object-bytes', type=int, default=4096)
    parser.add_argument('--samples', type=int, default=40, help='LIST requests per phase; GET uses four times this count')
    parser.add_argument('--output', required=True, help='new JSON file; existing files are never overwritten')
    args = parser.parse_args()
    try:
        args.stages = [int(value) for value in args.stages.split(',')]
    except ValueError:
        parser.error('stages must be comma-separated integers')
    if not args.stages or sorted(set(args.stages)) != args.stages or not 1 <= args.stages[0] <= args.stages[-1] <= 100000:
        parser.error('stages must be strictly increasing, positive, at most 100000')
    if not 1 <= args.object_bytes <= 16 << 20 or not 1 <= args.samples <= 1000:
        parser.error('object-bytes must be 1..16MiB; samples 1..1000')
    if args.stages[-1] * args.object_bytes > 2 << 30:
        parser.error('pilot payload is limited to 2GiB per node')
    return args


def main():
    args = parse_args()
    binary, output = Path(args.binary).resolve(), Path(args.output).resolve()
    modes = ['legacy', 'consensus'] if args.mode == 'both' else [args.mode]
    require(binary.is_file(), 'build the binary first')
    require(not output.exists(), 'output already exists; choose a new file')
    report = {'schema': 1, 'started_utc': datetime.now(timezone.utc).isoformat(), 'status': 'running',
              'host': {'os': platform.system(), 'release': platform.release(), 'architecture': platform.machine(),
                       'logical_cpus': os.cpu_count()},
              'binary_sha256': hashlib.sha256(binary.read_bytes()).hexdigest(),
              'binary_version': subprocess.check_output([str(binary), '-version'], text=True).splitlines()[0],
              'harness_sha256': hashlib.sha256(Path(__file__).read_bytes()).hexdigest(),
              'client': {'python': platform.python_version(), 'boto3': version('boto3'), 'botocore': version('botocore')},
              'settings': {'stages': args.stages, 'object_bytes': args.object_bytes, 'list_samples': args.samples,
                           'payload_pattern': 'identical across keys; consensus chunks can deduplicate',
                           'max_keys': 100, 'groups': 100, 'retries': 0, 'tls': False, 'audit': False,
                           'max_concurrent_uploads': 4, 'checksum_verification': True},
              'method': 'Closed loop; request latency includes client signing, transfer and validation, excludes executor queue. Warm-up excluded. Success percentiles use nearest rank. RSS is sampled sum of server processes every 200ms, not an exact peak. CPU is summed server process time. Client and servers share one host and disk; cache not cleared. Full traversal is one observation, not a latency distribution.',
              'runs': []}
    try:
        for mode in modes:
            with tempfile.TemporaryDirectory(prefix='kladovka-load-') as temporary:
                root = Path(temporary)
                count = 1 if mode == 'legacy' else 3
                required = args.stages[-1] * (args.object_bytes + 8192) * count * 4 + (512 << 20)
                require(shutil.disk_usage(root).free > required, 'insufficient free space for pilot budget')
                run = {'mode': mode, 'stages': []}
                report['runs'].append(run)
                run_mode(binary, mode, root, args, run)
        report['status'] = 'passed'
    except BaseException as error:
        report['status'] = 'failed'
        report['failure'] = {'type': type(error).__name__, 'message': str(error)}
        raise
    finally:
        report['finished_utc'] = datetime.now(timezone.utc).isoformat()
        with output.open('x') as file:
            json.dump(report, file, ensure_ascii=False, indent=2)
            file.write('\n')
        print('Report:', output, flush=True)


if __name__ == '__main__':
    main()
