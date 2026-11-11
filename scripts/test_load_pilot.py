"""Checks that the pilot reports latency and failures without hiding errors."""
import unittest
from unittest.mock import patch

from load_pilot import ValidationError, cpu_seconds, measure, percentile


class PilotStatisticsTest(unittest.TestCase):
    def test_nearest_rank_and_empty(self):
        self.assertIsNone(percentile([], 95))
        self.assertEqual(percentile([9, 1, 3, 2], 50), 2)
        self.assertEqual(percentile(list(range(1, 101)), 95), 95)
        self.assertEqual(percentile([1, 2], 99), 2)

    def test_cpu_time_formats(self):
        self.assertEqual(cpu_seconds('00:01.25'), 1.25)
        self.assertEqual(cpu_seconds('01:02:03'), 3723)
        self.assertEqual(cpu_seconds('2-01:02:03'), 176523)

    @patch('load_pilot.process_usage', return_value=(2048, 0.5))
    def test_error_counts_and_success_latency(self, _usage):
        def operation(index):
            if index % 2:
                raise ValidationError('fixture mismatch')
        result = measure('test', 4, 2, operation, [123])
        self.assertEqual(result['requests'], 4)
        self.assertEqual(result['successes'], 2)
        self.assertEqual(result['errors'], {'ValidationError': 2})
        self.assertEqual(len(result['samples']), 4)
        self.assertEqual(result['rss_sum_peak_mib'], 2)
        self.assertEqual(result['resource_sampling_errors'], 0)

    @patch('load_pilot.process_usage', side_effect=OSError('unavailable'))
    def test_missing_resources_not_reported_as_zero(self, _usage):
        result = measure('test', 1, 1, lambda _: None, [123])
        self.assertIsNone(result['rss_sum_peak_mib'])
        self.assertIsNone(result['server_cpu_seconds'])
        self.assertGreater(result['resource_sampling_errors'], 0)


if __name__ == '__main__':
    unittest.main()
