#!/usr/bin/env python3
"""AWS CLI + stopped-server backup/restore smoke test, exclusively in temporary data roots."""
import argparse
import base64
import hashlib
import json
import os
from pathlib import Path
import shutil
import socket
import subprocess
import tempfile
import time
import urllib.error
import urllib.request


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--binary', default='./bin/kladovka')
    parser.add_argument('--access-policy', action='store_true', help='test separate admin and per-user S3 keys')
    args = parser.parse_args()
    binary = str(Path(args.binary).resolve())
    if not shutil.which('aws'):
        raise SystemExit('AWS CLI is required to run the integration smoke test')
    with tempfile.TemporaryDirectory(prefix='kladovka-smoke-') as tmp:
        root = Path(tmp)
        with socket.socket() as sock:
            sock.bind(('127.0.0.1', 0))
            port = sock.getsockname()[1]
        endpoint = f'http://127.0.0.1:{port}'
        env = {**{key: value for key, value in os.environ.items() if not key.startswith('AWS_')}, 'AWS_ACCESS_KEY_ID': 'smoke', 'AWS_SECRET_ACCESS_KEY': 'smoke-secret-for-tests',
               'AWS_DEFAULT_REGION': 'us-east-1', 'AWS_CONFIG_FILE': str(root/'aws-config'),
               'AWS_SHARED_CREDENTIALS_FILE': str(root/'aws-credentials'), 'AWS_EC2_METADATA_DISABLED': 'true',
               'AWS_REQUEST_CHECKSUM_CALCULATION': 'when_required', 'AWS_RESPONSE_CHECKSUM_VALIDATION': 'when_required',
               'ACCESS_POLICY_FILE': '', 'ADMIN_USER': '', 'ADMIN_PASSWORD': '', 'AWS_PAGER': '', 'LISTEN_ADDR': f'127.0.0.1:{port}', 'NODE_URL': endpoint, 'NODE_ID': 'smoke',
               'ACCESS_KEY': 'smoke', 'SECRET_KEY': 'smoke-secret-for-tests', 'REGION': 'us-east-1',
               'PEER_NODES': '', 'REPLICATION_FACTOR': '1', 'CLUSTER_TOKEN': 'smoke-cluster-token',
               'HEALTH_INTERVAL_SEC': '1', 'TLS_MIN_VERSION': '1.2', 'CLUSTER_VERIFY_TLS': 'true',
               'TLS_CERT_FILE': '', 'TLS_KEY_FILE': '', 'MIN_FREE_DISK_BYTES': '0',
               'MAX_OBJECT_SIZE': str(16 << 20), 'MAX_TEMP_BYTES': str(32 << 20),
               'STORAGE_QUOTA_BYTES': str(64 << 20), 'MAX_CONCURRENT_UPLOADS': '4',
               'VERIFY_OBJECT_CHECKSUM': 'true', 'MULTIPART_TTL_SEC': '86400', 'CLEANUP_INTERVAL_SEC': '600'}
        (root/'aws-config').write_text('[default]\nregion = us-east-1\ns3 =\n    addressing_style = path\n    signature_version = s3v4\n')
        auth = 'Basic ' + base64.b64encode(b'smoke:smoke-secret-for-tests').decode()
        if args.access_policy:
            policy_path = root/'access.json'
            policy = {'version': 1, 'users': [
                {'id': 'automation', 'role': 'admin', 'keys': [{'access_key': 'smoke', 'secret_key': 'smoke-secret-for-tests'}]},
                {'id': 'reader', 'role': 'reader', 'polki': ['documents'], 'keys': [{'access_key': 'read', 'secret_key': 'reader-secret-for-tests'}]},
            ]}
            policy_path.write_text(json.dumps(policy))
            policy_path.chmod(0o600)
            env.update(ACCESS_POLICY_FILE=str(policy_path), ADMIN_USER='operator', ADMIN_PASSWORD='operator-password-tests')
            auth = 'Basic ' + base64.b64encode(b'operator:operator-password-tests').decode()
        process = None
        log = (root/'server.log').open('w')

        def start(data):
            nonlocal process
            process = subprocess.Popen([binary], env={**env, 'DATA_ROOT': str(data)}, stdout=log, stderr=log)
            deadline = time.monotonic() + 15
            while time.monotonic() < deadline:
                if process.poll() is not None:
                    raise RuntimeError('test server exited at startup')
                try:
                    with urllib.request.urlopen(endpoint+'/healthz', timeout=1) as response:
                        if response.status == 200:
                            return
                except OSError:
                    time.sleep(0.05)
            raise RuntimeError('test server did not become ready')

        def stop():
            nonlocal process
            if process is not None:
                process.terminate()
                try:
                    process.wait(timeout=35)
                except subprocess.TimeoutExpired:
                    process.kill()
                    process.wait()
                    raise RuntimeError('graceful shutdown timed out')
                process = None

        def aws(*arguments, credentials=None):
            result = subprocess.run(['aws', '--endpoint-url', endpoint, *arguments], env={**env, **(credentials or {})},
                                    capture_output=True, text=True, timeout=90)
            if result.returncode:
                raise RuntimeError(f'AWS {arguments[:2]} failed: {result.stderr}')
            return result.stdout

        def request(path):
            with urllib.request.urlopen(urllib.request.Request(endpoint+path, headers={'Authorization': auth}), timeout=10) as response:
                return response.read()

        try:
            start(root/'data')
            aws('s3api', 'create-bucket', '--bucket', 'documents')
            small = root/'small.txt'
            small.write_bytes(b'backup and restore verification\n')
            aws('s3', 'cp', str(small), 's3://documents/nested/readme.txt')
            large = root/'large.bin'
            large.write_bytes(bytes(range(256)) * 32768 + b'multipart tail')
            aws('s3', 'cp', str(large), 's3://documents/large.bin')
            listing = json.loads(aws('s3api', 'list-objects-v2', '--bucket', 'documents'))
            assert len(listing['Contents']) == 2
            aws('s3api', 'get-object', '--bucket', 'documents', '--key', 'large.bin', '--range', 'bytes=0-15', str(root/'range.bin'))
            assert (root/'range.bin').read_bytes() == large.read_bytes()[:16]
            signed = aws('s3', 'presign', 's3://documents/nested/readme.txt', '--expires-in', '300').strip()
            with urllib.request.urlopen(signed, timeout=10) as response:
                assert response.read() == small.read_bytes()
            assert 'Обзор хранилища'.encode() in request('/ui/')
            assert len(request('/ui/app.css')) > 10000
            assert json.loads(request('/api/stats'))['objects_count'] == 2
            if args.access_policy:
                reader = {'AWS_ACCESS_KEY_ID': 'read', 'AWS_SECRET_ACCESS_KEY': 'reader-secret-for-tests'}
                reader_url = aws('s3', 'presign', 's3://documents/nested/readme.txt', '--expires-in', '300', credentials=reader).strip()
                with urllib.request.urlopen(reader_url, timeout=10) as response:
                    assert response.read() == small.read_bytes()
                policy['users'][1]['keys'] = []
                replacement = root/'access.new'
                replacement.write_text(json.dumps(policy))
                replacement.chmod(0o600)
                replacement.replace(policy_path)
                try:
                    urllib.request.urlopen(reader_url, timeout=10)
                except urllib.error.HTTPError as error:
                    assert error.code == 403
                else:
                    raise AssertionError('revoked presigned URL was accepted')
                print('PASS separate admin credentials, reader presigned GET, live key revocation')
            print('PASS AWS CLI: bucket, PUT, automatic multipart, LIST, Range, presigned GET, UI, stats')
            stop()
            shutil.copytree(root/'data', root/'backup')
            shutil.copytree(root/'backup', root/'restored')
            start(root/'restored')
            for key, original in [('nested/readme.txt', small), ('large.bin', large)]:
                target = root/('downloaded-'+original.name)
                aws('s3', 'cp', 's3://documents/'+key, str(target))
                assert hashlib.sha256(target.read_bytes()).digest() == hashlib.sha256(original.read_bytes()).digest()
                aws('s3api', 'delete-object', '--bucket', 'documents', '--key', key)
            aws('s3api', 'delete-bucket', '--bucket', 'documents')
            print('PASS stopped-server backup → restore → SHA-256 verification → nested deletion')
        except Exception:
            log.flush()
            print((root/'server.log').read_text())
            raise
        finally:
            stop()
            log.close()


if __name__ == '__main__':
    main()
