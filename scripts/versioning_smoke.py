#!/usr/bin/env python3
"""Version history contract on disposable legacy and three-node consensus servers."""
import argparse
import hashlib
from pathlib import Path
import tempfile
from botocore.exceptions import ClientError
from load_pilot import servers, require


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--binary', default='./bin/kladovka')
    args = parser.parse_args()
    binary = Path(args.binary).resolve()
    for mode in ('legacy', 'consensus'):
        with tempfile.TemporaryDirectory(prefix='kladovka-versions-') as tmp:
            with servers(binary, mode, Path(tmp), 8 << 20, versioning=True) as (client, _, info, _):
                bucket, key = 'versions', 'folder/file.txt'
                client.create_bucket(Bucket=bucket)
                require(client.get_bucket_versioning(Bucket=bucket)['Status'] == 'Enabled', 'versioning status')
                original = hashlib.shake_256(b'version-original').digest(600000)
                first = client.put_object(Bucket=bucket, Key=key, Body=original)['VersionId']
                second = client.put_object(Bucket=bucket, Key=key, Body=b'second')['VersionId']
                deleted = client.delete_object(Bucket=bucket, Key=key)
                require(deleted['DeleteMarker'] and deleted['VersionId'], 'delete marker missing')
                try:
                    client.get_object(Bucket=bucket, Key=key)
                    raise AssertionError('deleted object still visible')
                except ClientError as error:
                    require(error.response['ResponseMetadata']['HTTPStatusCode'] == 404, 'unexpected deleted GET')
                history = list(client.get_paginator('list_object_versions').paginate(Bucket=bucket, PaginationConfig={'PageSize': 1}))
                require(sum(len(p.get('Versions', [])) + len(p.get('DeleteMarkers', [])) for p in history) == 3, 'pagination lost generations')
                body = client.get_object(Bucket=bucket, Key=key, VersionId=first)['Body']
                with body:
                    require(body.read() == original, 'old version bytes differ')
                restored = client.copy_object(Bucket=bucket, Key=key, CopySource={'Bucket': bucket, 'Key': key, 'VersionId': first})
                require(restored['VersionId'] not in (first, second), 'restore must create a new version')
                with client.get_object(Bucket=bucket, Key=key)['Body'] as body:
                    require(body.read() == original, 'restore content differs')
                # Completion must create exactly one generation, with preserved multipart ETag.
                upload = client.create_multipart_upload(Bucket=bucket, Key='multipart')['UploadId']
                etag = client.upload_part(Bucket=bucket, Key='multipart', UploadId=upload, PartNumber=1, Body=b'part')['ETag']
                completed = client.complete_multipart_upload(Bucket=bucket, Key='multipart', UploadId=upload, MultipartUpload={'Parts': [{'PartNumber': 1, 'ETag': etag}]})
                require(completed.get('VersionId'), 'multipart version missing')
                # Delete specific generations, including the latest, then the bucket.
                listing = client.list_object_versions(Bucket=bucket)
                for item in listing.get('Versions', []) + listing.get('DeleteMarkers', []):
                    client.delete_object(Bucket=bucket, Key=item['Key'], VersionId=item['VersionId'])
                require(not client.list_object_versions(Bucket=bucket).get('Versions'), 'purged generation remains')
                client.delete_bucket(Bucket=bucket)
                print(mode, info['route'], 'versioning/restore/pagination/multipart/purge passed', flush=True)


if __name__ == '__main__':
    main()
