"""Reject incorrect benchmark results rather than publishing fast but stale LISTs."""
import unittest

from load_pilot import ValidationError
from workload_pilot import identity, payload, summarize_events, validate_listing


class WorkloadContractTest(unittest.TestCase):
    def test_unique_payloads_do_not_repeat_chunks(self):
        left = payload('object-a', 3*256*1024)
        right = payload('object-b', len(left))
        self.assertEqual(left, payload('object-a', len(left)))
        chunks = [data[i:i+256*1024] for data in (left, right) for i in range(0, len(data), 256*1024)]
        self.assertEqual(len(set(chunks)), 6)
        self.assertNotEqual(identity(left)['sha256'], identity(right)['sha256'])

    def test_acknowledged_write_must_be_visible(self):
        key = 'catalog/g-00/object-000'
        response = {'Contents': [{'Key': key, 'Size': 4096, 'ETag': 'old'}],
                    'KeyCount': 1, 'IsTruncated': False}
        allowed = {key: {'old', 'new'}}
        validate_listing(response, [key], allowed)
        with self.assertRaises(ValidationError):
            validate_listing(response, [key], allowed, (key, 'new'))
        response['Contents'][0]['ETag'] = 'unknown'
        with self.assertRaises(ValidationError):
            validate_listing(response, [key], allowed)

    def test_missing_duplicate_and_wrong_truncation_rejected(self):
        keys = [f'catalog/g-00/{i:04}' for i in range(101)]
        response = {'Contents': [{'Key': key, 'Size': 4096, 'ETag': 'e'} for key in keys[:100]],
                    'KeyCount': 100, 'IsTruncated': True}
        validate_listing(response, keys)
        response['IsTruncated'] = False
        with self.assertRaises(ValidationError):
            validate_listing(response, keys)
        response['IsTruncated'] = True
        response['Contents'][-1]['Key'] = keys[0]
        with self.assertRaises(ValidationError):
            validate_listing(response, keys)
        response['Contents'].pop()
        with self.assertRaises(ValidationError):
            validate_listing(response, keys)

    def test_grouped_count_and_contents(self):
        keys = ['catalog/g-00/a', 'catalog/g-00/b', 'catalog/g-01/a']
        response = {'CommonPrefixes': [{'Prefix': 'catalog/g-00/'}, {'Prefix': 'catalog/g-01/'}],
                    'KeyCount': 2, 'IsTruncated': False}
        validate_listing(response, keys, grouped=True)
        response['KeyCount'] = 3
        with self.assertRaises(ValidationError):
            validate_listing(response, keys, grouped=True)

    def test_actual_overlap_and_errors_are_preserved(self):
        events = [{'operation': op, 'start_seconds': start, 'end_seconds': end,
                   'ms': (end-start)*1000, 'error': error}
                  for op, start, end, error in [('PUT', 1, 2, None), ('LIST', 1.5, 2.5, None),
                                                ('LIST', 2, 3, None), ('LIST', 3, 4, 'timeout')]]
        result = summarize_events(events, 'LIST')
        self.assertEqual(result['requests'], 3)
        self.assertEqual(result['successes'], 2)
        self.assertEqual(result['overlapped_put_requests'], 1)
        self.assertEqual(result['errors'], {'timeout': 1})
        self.assertEqual(len(result['samples']), 3)


if __name__ == '__main__':
    unittest.main()
