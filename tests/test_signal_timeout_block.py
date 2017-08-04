from collections import defaultdict
import datetime
from datetime import timedelta
from threading import Event
from nio.block.terminals import DEFAULT_TERMINAL
from nio.modules.scheduler import Job
from nio.util.threading import spawn
from nio.signal.base import Signal
from nio.testing.block_test_case import NIOBlockTestCase
from nio.testing.modules.scheduler.scheduler import JumpAheadScheduler
from ..signal_timeout_block import SignalTimeout


class TestSignalTimeout(NIOBlockTestCase):

    def test_timeout(self):
        block = SignalTimeout()
        self.configure_block(block, {
            "intervals": [
                {
                    "interval": {
                        "milliseconds": 200
                    }
                }
            ]
        })
        block.start()
        block.process_signals([Signal({'a': 'A'})])
        self.assert_num_signals_notified(0, block)
        JumpAheadScheduler.jump_ahead(0.2)
        self.assert_num_signals_notified(1, block)
        self.assertDictEqual(self.last_notified[DEFAULT_TERMINAL][0].to_dict(),
                             {'timeout': datetime.timedelta(0, 0, 200000),
                              'group': None,
                              'a': 'A'})
        block.stop()

    def test_timeout_with_signal_expression(self):
        """Timeout intervals and repeatable flags can be set by signal"""
        block = SignalTimeout()
        self.configure_block(block, {
            "intervals": [
                {
                    "interval": "{{ datetime.timedelta(seconds=$interval) }}",
                    "repeatable": "{{ $repeatable }}"
                }
            ]
        })
        block.start()
        block.process_signals([Signal({'interval': 0.2, 'repeatable': True})])
        JumpAheadScheduler.jump_ahead(0.2)
        self.assert_num_signals_notified(1, block)
        self.assertDictEqual(self.last_notified[DEFAULT_TERMINAL][0].to_dict(),
                             {'timeout': datetime.timedelta(0, 0, 200000),
                              'group': None,
                              'interval': 0.2,
                              'repeatable': True})
        JumpAheadScheduler.jump_ahead(0.2)
        self.assert_num_signals_notified(2, block)
        block.stop()

    def test_reset(self):
        """ Make sure the block can reset the intervals """
        block = SignalTimeout()
        self.configure_block(block, {
            "intervals": [
                {
                    "interval": {
                        "seconds": 1
                    }
                }
            ]
        })
        block.start()
        block.process_signals([Signal({'a': 'A'})])
        # Wait a bit before sending another signal
        JumpAheadScheduler.jump_ahead(0.6)
        block.process_signals([Signal({'b': 'B'})])
        self.assert_num_signals_notified(0, block)
        JumpAheadScheduler.jump_ahead(0.6)
        self.assert_num_signals_notified(0, block)
        JumpAheadScheduler.jump_ahead(0.6)
        self.assert_num_signals_notified(1, block)
        self.assertDictEqual(self.last_notified[DEFAULT_TERMINAL][0].to_dict(),
                             {'timeout': datetime.timedelta(seconds=1),
                              'group': None,
                              'b': 'B'})
        block.stop()

    def test_repeatable(self):
        block = SignalTimeout()
        self.configure_block(block, {
            "intervals": [
                {
                    "interval": {
                        "milliseconds": 200
                    },
                    "repeatable": True
                }
            ]
        })
        block.start()
        block.process_signals([Signal({'a': 'A'})])
        JumpAheadScheduler.jump_ahead(0.2)
        self.assert_num_signals_notified(1, block)
        self.assertDictEqual(self.last_notified[DEFAULT_TERMINAL][0].to_dict(),
                             {'timeout': datetime.timedelta(0, 0, 200000),
                              'group': None,
                              'a': 'A'})
        JumpAheadScheduler.jump_ahead(0.2)
        self.assert_num_signals_notified(2, block)
        self.assertDictEqual(self.last_notified[DEFAULT_TERMINAL][1].to_dict(),
                             {'timeout': datetime.timedelta(0, 0, 200000),
                              'group': None,
                              'a': 'A'})
        block.stop()

    def test_groups(self):
        block = SignalTimeout()
        self.configure_block(block, {
            "intervals": [
                {
                    "interval": {
                        "milliseconds": 200
                    },
                    "repeatable": True
                }
            ],
            "group_by": "{{$group}}"
        })
        block.start()
        block.process_signals([Signal({'a': 'A', 'group': 'a'})])
        block.process_signals([Signal({'b': 'B', 'group': 'b'})])
        # Wait for notifications
        JumpAheadScheduler.jump_ahead(0.2)
        self.assert_num_signals_notified(2, block)
        self.assertDictEqual(self.last_notified[DEFAULT_TERMINAL][0].to_dict(),
                             {'timeout': datetime.timedelta(0, 0, 200000),
                              'group': 'a',
                              'a': 'A'})
        self.assertDictEqual(self.last_notified[DEFAULT_TERMINAL][1].to_dict(),
                             {'timeout': datetime.timedelta(0, 0, 200000),
                              'group': 'b',
                              'b': 'B'})
        block.stop()

    def test_multiple_intervals(self):
        block = SignalTimeout()
        self.configure_block(block, {
            "intervals": [
                {
                    "interval": {
                        "milliseconds": 200
                    },
                    "repeatable": True
                },
                {
                    "interval": {
                        "milliseconds": 300
                    },
                    "repeatable": False
                }


            ]
        })
        block.start()
        block.process_signals([Signal({'a': 'A'})])
        # At time 200
        JumpAheadScheduler.jump_ahead(0.2)
        self.assert_num_signals_notified(1, block)
        self.assertDictEqual(self.last_notified[DEFAULT_TERMINAL][0].to_dict(),
                             {'timeout': datetime.timedelta(0, 0, 200000),
                              'group': None,
                              'a': 'A'})
        # At time 300
        JumpAheadScheduler.jump_ahead(0.1)
        self.assert_num_signals_notified(2, block)
        self.assertDictEqual(self.last_notified[DEFAULT_TERMINAL][1].to_dict(),
                             {'timeout': datetime.timedelta(0, 0, 300000),
                              'group': None,
                              'a': 'A'})
        # At time 400
        JumpAheadScheduler.jump_ahead(0.1)
        self.assert_num_signals_notified(3, block)
        self.assertDictEqual(self.last_notified[DEFAULT_TERMINAL][2].to_dict(),
                             {'timeout': datetime.timedelta(0, 0, 200000),
                              'group': None,
                              'a': 'A'})
        # At time 700 - only one additional signal since 300 is not repeatable
        JumpAheadScheduler.jump_ahead(0.3)
        self.assert_num_signals_notified(4, block)
        block.stop()

    def test_persistence(self):
        """Persisted timeout jobs are notified accordingly"""
        block = SignalTimeout()
        # Load from persistence
        persisted_jobs = defaultdict(dict)
        persisted_jobs[1][timedelta(seconds=0.1)] = Signal({"group": 1})
        persisted_jobs[2][timedelta(seconds=0.1)] = Signal({"group": 2})
        block._repeatable_jobs = persisted_jobs
        self.configure_block(block, {
            "intervals": [{"interval":
                            {"milliseconds": 100},
                            "repeatable": True
                            }],
            "group_by": "{{ $group }}"})
        block.start()
        self.assertEqual(len(block._jobs), 2)
        self.assertTrue(
            isinstance(block._jobs[1][timedelta(seconds=0.1)], Job))
        # Wait for the persisted signal to be notified
        JumpAheadScheduler.jump_ahead(0.1)
        self.assert_num_signals_notified(2, block)
        self.assertEqual(self.last_notified[DEFAULT_TERMINAL][0].group, 1)
        # And notified again, since the job is repeatable
        JumpAheadScheduler.jump_ahead(0.1)
        self.assert_num_signals_notified(4, block)
        self.assertEqual(self.last_notified[DEFAULT_TERMINAL][2].group, 1)
        # New groups should still be scheduled
        block.process_signals([Signal({"group": 3})])
        # So we get another notification from persistence and the new one
        JumpAheadScheduler.jump_ahead(0.1)
        self.assert_num_signals_notified(7, block)
        self.assertEqual(self.last_notified[DEFAULT_TERMINAL][4].group, 1)
        self.assertEqual(self.last_notified[DEFAULT_TERMINAL][5].group, 2)
        self.assertEqual(self.last_notified[DEFAULT_TERMINAL][6].group, 3)

    def test_persisted_jobs_always_schedule(self):
        """Persisted timeout jobs are not cancelled before they schedule"""

        class TestSignalTimeout(SignalTimeout):

            def __init__(self):
                super().__init__()
                self.event = Event()
                self.schedule_count = 0
                self.cancel_count = 0

            def _schedule_timeout_job(self, signal, key, interval, repeatable):
                super()._schedule_timeout_job(
                    signal, key, interval, repeatable)
                self.schedule_count += 1

            def _cancel_timeout_jobs(self, key):
                super()._cancel_timeout_jobs(key)
                self.cancel_count += 1

            def process_signals(self, signals):
                super().process_signals(signals)
                self.event.set()

        block = TestSignalTimeout()
        # Load from persistence
        persisted_jobs = defaultdict(dict)
        persisted_jobs[1][timedelta(seconds=0.1)] = Signal({"group": 1})
        persisted_jobs[2][timedelta(seconds=0.1)] = Signal({"group": 2})
        block._repeatable_jobs = persisted_jobs
        self.configure_block(block, {
            "intervals": [{"interval":
                            {"milliseconds": 100},
                            "repeatable": True
                            }],
            "group_by": "{{ $group }}"})
        # This signal should not cancel the persisted job before it's scheduled
        spawn(block.process_signals, [Signal({"group": 2})])
        self.assertEqual(block.schedule_count, 0)
        self.assertEqual(block.cancel_count, 0)
        block.start()
        block.event.wait(1)
        # 2 scheduled persisted jobs and one scheduled processed signal
        self.assertEqual(block.schedule_count, 3)
        # Processed signal cancels one of the scheduled jobs
        self.assertEqual(block.cancel_count, 1)
