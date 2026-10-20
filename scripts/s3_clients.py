#!/usr/bin/env python3
"""SDK contracts invoked only against the smoke tests' disposable servers."""
import io
from datetime import timedelta
from importlib.metadata import version
from urllib.parse import urlsplit
from urllib.request import urlopen

import boto3
from botocore.config import Config
from botocore.exceptions import ClientError
from boto3.s3.transfer import TransferConfig
from minio import Minio
import urllib3


def run(endpoint, access_key, secret_key):
    host = urlsplit(endpoint)
    assert host.hostname == '127.0.0.1', 'SDK tests require a disposable loopback endpoint'
    config = Config(signature_version='s3v4', region_name='us-east-1',
                    s3={'addressing_style': 'path'},
                    request_checksum_calculation='when_required',
                    response_checksum_validation='when_required',
                    connect_timeout=3, read_timeout=30, retries={'total_max_attempts': 1})
    b = boto3.client('s3', endpoint_url=endpoint, aws_access_key_id=access_key,
                     aws_secret_access_key=secret_key, config=config)
    m = Minio(host.netloc, access_key=access_key, secret_key=secret_key,
              secure=False, region='us-east-1',
              http_client=urllib3.PoolManager(timeout=urllib3.Timeout(connect=3, read=30), retries=False))
    bucket = 'sdk-contracts'
    key = 'nested/отчёт + 100%.txt'
    small = 'Проверка обмена данными между SDK\n'.encode()
    large = bytes(range(256)) * 24576 + b'multipart tail'

    def boto_read(k, **kwargs):
        response = b.get_object(Bucket=bucket, Key=k, **kwargs)
        with response['Body'] as body:
            return body.read()

    def minio_read(k, **kwargs):
        response = m.get_object(bucket, k, **kwargs)
        try:
            return response.read()
        finally:
            response.close()
            response.release_conn()

    def rejected(code, operation):
        try:
            operation()
        except ClientError as error:
            assert error.response['Error']['Code'] == code, error
        else:
            raise AssertionError('expected '+code)

    b.create_bucket(Bucket=bucket)
    assert m.bucket_exists(bucket)
    assert bucket in [x.name for x in m.list_buckets()]
    b.put_object(Bucket=bucket, Key=key, Body=small, ContentType='text/plain')
    assert minio_read(key) == small
    assert m.stat_object(bucket, key).size == len(small)
    assert boto_read(key, Range='bytes=1-7') == small[1:8]
    assert minio_read(key, offset=1, length=7) == small[1:8]
    for signed in (b.generate_presigned_url('get_object', Params={'Bucket': bucket, 'Key': key}, ExpiresIn=60),
                   m.presigned_get_object(bucket, key, expires=timedelta(seconds=60))):
        with urlopen(signed, timeout=10) as response:
            assert response.read() == small
    m.put_object(bucket, 'empty', io.BytesIO(b''), 0)
    assert boto_read('empty') == b''
    # Explicit region avoids GetBucketLocation, which is not implemented yet.
    # Both transfer managers must perform actual multipart (> 5 MiB) transfers.
    transfer = TransferConfig(multipart_threshold=5 << 20, multipart_chunksize=5 << 20,
                              max_concurrency=2, use_threads=True)
    b.upload_fileobj(io.BytesIO(large), bucket, 'boto-multipart', Config=transfer)
    assert b.head_object(Bucket=bucket, Key='boto-multipart')['ETag'].endswith('-2"')
    assert minio_read('boto-multipart') == large
    m.put_object(bucket, 'minio-multipart', io.BytesIO(large), len(large), part_size=5 << 20, num_parallel_uploads=2)
    assert boto_read('minio-multipart') == large
    assert b.head_object(Bucket=bucket, Key='minio-multipart')['ETag'].endswith('-2"')
    literals = {'literal/a%2Fb': b'encoded slash', 'literal/a/b': b'actual slash', 'literal/%25': b'encoded percent'}
    for name, content in literals.items():
        b.put_object(Bucket=bucket, Key=name, Body=content)
    for name, content in literals.items():
        assert minio_read(name) == content
    expected = {key, 'empty', 'boto-multipart', 'minio-multipart', *literals}
    pages = list(b.get_paginator('list_objects_v2').paginate(Bucket=bucket, PaginationConfig={'PageSize': 1}))
    listed = [obj['Key'] for page in pages for obj in page.get('Contents', [])]
    assert len(pages) == len(expected) and len(listed) == len(expected) and set(listed) == expected, pages
    assert {obj.object_name for obj in m.list_objects(bucket, recursive=True)} == expected
    # Unsupported writes must fail explicitly and leave the source untouched.
    rejected('NotImplemented', lambda: b.copy_object(Bucket=bucket, Key='copy', CopySource={'Bucket': bucket, 'Key': key}))
    rejected('NotImplemented', lambda: b.delete_objects(Bucket=bucket, Delete={'Objects': [{'Key': key}]}))
    assert boto_read(key) == small
    rejected('NoSuchKey', lambda: b.get_object(Bucket=bucket, Key='copy'))
    upload = b.create_multipart_upload(Bucket=bucket, Key='aborted')['UploadId']
    b.abort_multipart_upload(Bucket=bucket, Key='aborted', UploadId=upload)
    rejected('NoSuchKey', lambda: b.get_object(Bucket=bucket, Key='aborted'))
    for k in expected:
        m.remove_object(bucket, k)
    assert not list(m.list_objects(bucket, recursive=True))
    m.remove_bucket(bucket)
    b.close()
    print(f'PASS boto3 {version("boto3")}, botocore {version("botocore")}, minio {version("minio")}: '
          'cross-client bytes, escaped keys, empty objects, Range, presigned GET, multipart, pagination, abort, deletion and explicit unsupported writes', flush=True)
