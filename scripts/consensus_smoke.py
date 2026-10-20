#!/usr/bin/env python3
"""Three real processes: durable catch-up, leader failure, tombstones and quorum loss.
All files, ports and credentials belong to this temporary loopback-only test.
"""
import argparse
import base64
import hashlib
import json
import os
from pathlib import Path
import socket
import subprocess
import tempfile
import time
import urllib.error
import urllib.request
import xml.etree.ElementTree as ET


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--binary', default='./bin/kladovka')
    args = parser.parse_args()
    binary = str(Path(args.binary).resolve())
    with tempfile.TemporaryDirectory(prefix='kladovka-consensus-') as temporary:
        root = Path(temporary)
        reservations = []
        for _ in range(11):
            sock = socket.socket()
            sock.bind(('127.0.0.1', 0))
            reservations.append(sock)
        ports = [sock.getsockname()[1] for sock in reservations]
        ids = [f'127.0.0.1:{port}' for port in ports[:3]]
        urls = ['http://'+node for node in ids]
        peers = dict(zip(ids, [f'127.0.0.1:{port}' for port in ports[3:6]]))
        for sock in reservations:
            sock.close()
        processes = [None]*3
        replacements = {}
        data_roots = [root/f'data-{i}' for i in range(3)]
        logs = [(root/f'node-{i}.log').open('w') for i in range(3)]
        admin = 'Basic '+base64.b64encode(b'operator:consensus-test-password').decode()

        def request(i, path, method='GET', body=None, with_headers=False, extra_headers=None):
            headers = {'Authorization': admin, 'Content-Type': 'application/json'}
            headers.update(extra_headers or {})
            if isinstance(body, dict):
                body = json.dumps(body).encode()
            try:
                with urllib.request.urlopen(urllib.request.Request(urls[i]+path, data=body, headers=headers, method=method), timeout=8) as response:
                    result = (response.status, response.read())
                    return (*result, response.headers) if with_headers else result
            except urllib.error.HTTPError as error:
                result = (error.code, error.read())
                return (*result, error.headers) if with_headers else result

        def wait(description, predicate, timeout=30):
            end = time.monotonic()+timeout
            while time.monotonic() < end:
                try:
                    result = predicate()
                    if result is not None and result is not False:
                        return result
                except (OSError, ValueError, KeyError):
                    pass
                time.sleep(0.1)
            raise RuntimeError('timed out: '+description)

        def start(i, bootstrap):
            env = {**os.environ, 'CLUSTER_MODE': 'consensus', 'RAFT_CLUSTER_ID': 'isolated-smoke-cluster',
                   'RAFT_PEERS': json.dumps(peers), 'RAFT_BOOTSTRAP': str(bootstrap).lower(),
                   'RAFT_JOIN': str(i in replacements).lower(), 'RAFT_REPLACEMENT_ADDR': replacements.get(i, ''),
                   'RAFT_LISTEN_ADDR': replacements.get(i, peers.get(ids[i], "")), 'RAFT_TLS_CERT_FILE': '', 'RAFT_TLS_KEY_FILE': '', 'RAFT_TLS_CA_FILE': '',
                   'LISTEN_ADDR': ids[i], 'NODE_ID': ids[i], 'NODE_URL': urls[i], 'DATA_ROOT': str(data_roots[i]),
                   'PEER_NODES': ','.join(url for j, url in enumerate(urls[:3]) if j != i), 'REPLICATION_FACTOR': '3',
                   'REGION': 'us-east-1', 'CLUSTER_TOKEN': 'consensus-test-cluster-token', 'CLUSTER_VERIFY_TLS': 'true',
                   'ACCESS_KEY': 'smoke', 'SECRET_KEY': 'consensus-test-s3-secret', 'ADMIN_USER': 'operator',
                   'ADMIN_PASSWORD': 'consensus-test-password', 'ACCESS_POLICY_FILE': '', 'AUDIT_LOG_FILE': '',
                   'TLS_CERT_FILE': '', 'TLS_KEY_FILE': '', 'TLS_MIN_VERSION': '1.2', 'HEALTH_INTERVAL_SEC': '1',
                   'MAX_OBJECT_SIZE': str(16 << 20), 'MAX_TEMP_BYTES': str(32 << 20), 'MAX_CONCURRENT_UPLOADS': '4',
                   'MIN_FREE_DISK_BYTES': '0', 'STORAGE_QUOTA_BYTES': '0', 'VERIFY_OBJECT_CHECKSUM': 'true',
                   'MULTIPART_TTL_SEC': '86400', 'CLEANUP_INTERVAL_SEC': '600', 'SESSION_TTL_SEC': '28800'}
            processes[i] = subprocess.Popen([binary], env=env, stdout=logs[i], stderr=logs[i])
            wait('node health', lambda: request(i, '/healthz')[0] == 200)

        def stop(i):
            process = processes[i]
            if process is not None:
                process.terminate()
                try:
                    process.wait(timeout=40)
                except subprocess.TimeoutExpired:
                    process.kill()
                    process.wait()
                    raise RuntimeError('node did not stop')
                processes[i] = None

        def leader():
            for i, process in enumerate(processes):
                if process is not None:
                    status, body = request(i, '/cluster')
                    if status == 200 and json.loads(body)['consensus']['state'] == 'Leader':
                        if request(i, '/api/polki')[0] == 200:
                            return i
            return None

        def write(i, key, data):
            status, body = request(i, '/api/object/demo/'+key, 'PUT', data)
            assert status == 200, (status, body)

        try:
            for i in range(3):
                start(i, True)
            current = wait('initial leader', leader)
            status, body = request((current+1)%3, '/api/polki', 'POST', {'name': 'demo'})
            assert status == 201, (status, body)
            payload = b'consensus-block-data\n'*40000
            write((current+1)%3, 'kept', payload)
            write(current, 'deleted', b'old value')
            for i in range(3):
                status, body = request(i, '/api/object/demo/kept')
                assert status == 200 and hashlib.sha256(body).digest() == hashlib.sha256(payload).digest()
                wait('materialized replica', lambda i=i: (root/f'data-{i}'/'demo'/'kept').exists())
            print('PASS three-node streaming write, quorum commit, forwarding and verified reads')
            status, initiated = request((current+1)%3, '/api/object/demo/multipart?uploads', 'POST', b'')
            assert status == 200, (status, initiated)
            upload_id = ET.fromstring(initiated).findtext('.//{*}UploadId')
            part1, part2 = b'm'*(5 << 20), b'last multipart part'
            etags = []
            for number, part in enumerate([part1, part2], 1):
                status, body, headers = request((current+1)%3, f'/api/object/demo/multipart?uploadId={upload_id}&partNumber={number}', 'PUT', part, with_headers=True)
                assert status == 200, (status, body)
                etags.append(headers['ETag'])
            complete = '<CompleteMultipartUpload>'+''.join(f'<Part><PartNumber>{n}</PartNumber><ETag>{etag}</ETag></Part>' for n, etag in enumerate(etags, 1))+'</CompleteMultipartUpload>'
            status, body = request((current+1)%3, f'/api/object/demo/multipart?uploadId={upload_id}', 'POST', complete.encode())
            assert status == 200, (status, body)
            status, body, headers = request((current+2)%3, '/api/object/demo/multipart', with_headers=True)
            assert status == 200 and body == part1+part2 and headers['ETag'].endswith('-2"')
            status, body = request((current+2)%3, '/api/object/demo/multipart', extra_headers={'Range': 'bytes=0-15'})
            assert status == 206 and body == part1[:16]
            status, body = request((current+2)%3, '/api/stats')
            stats = json.loads(body)
            assert status == 200 and stats['consensus']['mode'] == 'consensus' and stats['consensus']['quorum'] == 2 and stats['objects_count'] == 3
            status, body = request(current, '/metrics')
            assert status == 200 and b'kladovka_consensus_enabled 1' in body and b'kladovka_consensus_quorum_required 2' in body
            for metric in (b'kladovka_storage_observation_ok 1', b'kladovka_storage_available_bytes ', b'kladovka_consensus_voters 3', b'kladovka_consensus_target_rf 3', b'kladovka_consensus_membership_pending 0'):
                assert metric in body, metric
            print('PASS multipart across a follower endpoint, ETag/Range, shared catalog and consensus metrics')
            aws_env = {**{key: value for key, value in os.environ.items() if not key.startswith('AWS_')},
                       'AWS_ACCESS_KEY_ID': 'smoke', 'AWS_SECRET_ACCESS_KEY': 'consensus-test-s3-secret',
                       'AWS_DEFAULT_REGION': 'us-east-1', 'AWS_CONFIG_FILE': str(root/'aws-config'),
                       'AWS_SHARED_CREDENTIALS_FILE': str(root/'aws-credentials'), 'AWS_EC2_METADATA_DISABLED': 'true',
                       'AWS_REQUEST_CHECKSUM_CALCULATION': 'when_required', 'AWS_RESPONSE_CHECKSUM_VALIDATION': 'when_required', 'AWS_PAGER': ''}
            def aws(*arguments):
                return subprocess.check_output(['aws', '--endpoint-url', urls[(current+1)%3], *arguments], env=aws_env, text=True, stderr=subprocess.STDOUT, timeout=30)
            source = root/'signed-source.bin'
            source.write_bytes(payload)
            aws('s3api', 'put-object', '--bucket', 'demo', '--key', 'signed/object name.bin', '--body', str(source))
            aws('s3api', 'get-object', '--bucket', 'demo', '--key', 'signed/object name.bin', str(root/'signed-download.bin'))
            assert (root/'signed-download.bin').read_bytes() == payload
            signed_url = aws('s3', 'presign', 's3://demo/signed/object name.bin', '--expires-in', '300').strip()
            with urllib.request.urlopen(signed_url, timeout=10) as response:
                assert response.read() == payload
            aws('s3api', 'delete-object', '--bucket', 'demo', '--key', 'signed/object name.bin')
            print('PASS AWS CLI SigV4 and presigned GET through a follower with an escaped object key')
            offline = (current+1)%3
            stop(offline)
            status, body = request(current, '/api/object/demo/deleted', 'DELETE')
            assert status == 204, (status, body)
            write(current, 'while-offline', b'catch up after restart')
            start(offline, False)
            wait('replica catches up', lambda: (root/f'data-{offline}'/'demo'/'while-offline').read_bytes() == b'catch up after restart')
            wait('tombstone materialized', lambda: not (root/f'data-{offline}'/'demo'/'deleted').exists())
            assert request(offline, '/api/object/demo/deleted')[0] == 404
            print('PASS persisted catch-up and deletion recovery after node restart')
            current = wait('leader before failover', leader)
            stop(current)
            replacement = wait('automatic failover', leader)
            write(replacement, 'after-failover', b'new leader accepted')
            status, body = request(replacement, '/api/object/demo/kept')
            assert status == 200 and body == payload
            start(current, False)
            wait('old leader catches up', lambda: (root/f'data-{current}'/'demo'/'after-failover').read_bytes() == b'new leader accepted')
            assert request(current, '/api/object/demo/deleted')[0] == 404
            print('PASS leader failure, new leader writes, acknowledged data retained, no resurrection')
            current = wait('leader before node replacement', leader)
            lost = (current+1)%3
            stop(lost)
            data_roots[lost] = root/f'replacement-{lost}'
            replacements[lost] = f'127.0.0.1:{ports[6]}'
            start(lost, False)
            candidate = json.loads(request(lost, '/cluster')[1])['consensus']['raft_id']
            status, body = request(current, '/api/cluster/replace', 'POST', {'node_id': ids[lost], 'raft_id': candidate})
            assert status == 202, (status, body)
            def replaced():
                state = json.loads(request(current, '/cluster')[1])['consensus']
                return state['replacement'] and state['replacement']['done'] and state['voters'] == 3
            wait('empty-server replacement and promotion', replaced, timeout=60)
            assert request(lost, '/api/object/demo/kept')[1] == payload
            assert request(lost, '/api/object/demo/deleted')[0] == 404
            for i in range(3):
                stop(i)
            for i in range(3):
                start(i, False)
            current = wait('full restart after membership replacement', leader)
            assert request(current, '/api/object/demo/kept')[1] == payload
            print('PASS empty-server replacement, fresh Raft identity, restored quorum and full cluster restart')
            for i in (3, 4):
                ids.append(f'127.0.0.1:{ports[7+(i-3)*2]}')
                urls.append('http://'+ids[i])
                processes.append(None)
                data_roots.append(root/f'data-{i}')
                logs.append((root/f'node-{i}.log').open('w'))
                replacements[i] = f'127.0.0.1:{ports[8+(i-3)*2]}'
                start(i, False)
                candidate = json.loads(request(i, '/cluster')[1])['consensus']['raft_id']
                assert request(i, '/api/object/demo/kept')[0] == 503
                operation = {'request_id': f'add-{i}', 'http_url': urls[i], 'raft_id': candidate}
                status, body = request(current, '/api/cluster/add', 'POST', operation)
                assert status == 202, (status, body)
                def changed():
                    state = json.loads(request(current, '/cluster')[1])['consensus']
                    return state['membership_change'] and state['membership_change']['done']
                wait('add voter', changed, timeout=60)
                assert request(i, '/api/object/demo/kept')[1] == payload
                assert request(current, '/api/cluster/add', 'POST', operation)[0] == 202
            assert json.loads(request(current, '/api/stats')[1])['replication_factor'] == 5
            for i in (4, 3):
                status, body = request(current, '/api/cluster/remove', 'POST', {'request_id': f'remove-{i}', 'node_id': ids[i]})
                assert status == 202, (status, body)
                wait('remove voter', changed, timeout=60)
                stop(i)
            for i in range(3):
                stop(i)
            for i in range(3):
                start(i, False)
            current = wait('restart after expansion and removal', leader)
            assert request(current, '/api/object/demo/kept')[1] == payload
            assert json.loads(request(current, '/api/stats')[1])['replication_factor'] == 3
            print('PASS HTTP membership 3→4→5→4→3, routing, data verification, idempotency and restart')
            survivor = wait('leader before quorum loss', leader)
            for i in range(3):
                if i != survivor:
                    stop(i)
            assert request(survivor, '/api/object/demo/no-quorum', 'PUT', b'reject')[0] == 503
            assert request(survivor, '/api/object/demo/kept')[0] == 503
            assert request(survivor, '/healthz')[0] == 200
            print('PASS minority rejects writes and stale reads while liveness remains available')
        except Exception:
            for i, log in enumerate(logs):
                log.flush()
                print(f'node {i}:\n'+(root/f'node-{i}.log').read_text())
            raise
        finally:
            for i in range(len(processes)):
                stop(i)
            for log in logs:
                log.close()


if __name__ == '__main__':
    main()
