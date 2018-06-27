from collections import defaultdict
import datetime
from datetime import timedelta
from threading import Event
from time import sleep

from nio.block.terminals import DEFAULT_TERMINAL
from nio.modules.scheduler import Job
from nio.util.threading import spawn
from nio.signal.base import Signal
from nio.testing.block_test_case import NIOBlockTestCase
from nio.testing.modules.scheduler.scheduler import JumpAheadScheduler

from ..signal_timeout_block import SignalTimeout


class TestSignalTimeout(NIOBlockTestCase):

    def jump_ahead_sleep(self, sec, dur=0.1):
        # sleep after jumping ahead to ensure signal notification happens
        JumpAheadScheduler.jump_ahead(sec)
        sleep(dur)

    def test_timeout(self):
        block = SignalTimeout()
        self.configure_block(block, {
            "intervals": [
                {
                    "interval": {
                        "seconds": 10
                    }
                }
            ]
        })
        block.start()
        block.process_signals([Signal({'a': 'A'})])
        self.assert_num_signals_notified(0, block)
        self.jump_ahead_sleep(10)
        self.assert_num_signals_notified(1, block)
        self.assertDictEqual(self.last_notified[DEFAULT_TERMINAL][0].to_dict(),
                             {'timeout': timedelta(0, 10, 0),
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
        block.process_signals([Signal({'interval': 10, 'repeatable': True})])
        self.jump_ahead_sleep(10)
        self.assert_num_signals_notified(1, block)
        self.assertDictEqual(self.last_notified[DEFAULT_TERMINAL][0].to_dict(),
                             {'timeout': timedelta(0, 10, 0),
                              'group': None,
                              'interval': 10,
                              'repeatable': True})
        self.jump_ahead_sleep(10)
        self.assert_num_signals_notified(2, block)
        block.stop()

    def test_reset(self):
        """ Make sure the block can reset the intervals """
        block = SignalTimeout()
        self.configure_block(block, {
            "intervals": [
                {
                    "interval": {
                        "seconds": 10
                    }
                }
            ]
        })
        block.start()
        block.process_signals([Signal({'a': 'A'})])
        # Wait a bit before sending another signal
        self.jump_ahead_sleep(6)
        block.process_signals([Signal({'b': 'B'})])
        self.assert_num_signals_notified(0, block)
        self.jump_ahead_sleep(6)
        self.assert_num_signals_notified(0, block)
        self.jump_ahead_sleep(6)
        self.assert_num_signals_notified(1, block)
        self.assertDictEqual(self.last_notified[DEFAULT_TERMINAL][0].to_dict(),
                             {'timeout': timedelta(seconds=10),
                              'group': None,
                              'b': 'B'})
        block.stop()

    def test_repeatable(self):
        block = SignalTimeout()
        self.configure_block(block, {
            "intervals": [
                {
                    "interval": {
                        "seconds": 10
                    },
                    "repeatable": True
                }
            ]
        })
        block.start()
        block.process_signals([Signal({'a': 'A'})])
        self.jump_ahead_sleep(10)
        self.assert_num_signals_notified(1, block)
        self.assertDictEqual(self.last_notified[DEFAULT_TERMINAL][0].to_dict(),
                             {'timeout': timedelta(0, 10, 0),
                              'group': None,
                              'a': 'A'})
        self.jump_ahead_sleep(10)
        self.assert_num_signals_notified(2, block)
        self.assertDictEqual(self.last_notified[DEFAULT_TERMINAL][1].to_dict(),
                             {'timeout': timedelta(0, 10, 0),
                              'group': None,
                              'a': 'A'})
        block.stop()

    def test_groups(self):
        block = SignalTimeout()
        self.configure_block(block, {
            "intervals": [
                {
                    "interval": {
                        "seconds": 10
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
        self.jump_ahead_sleep(10)
        self.assert_num_signals_notified(2, block)
        self.assert_signal_notified(Signal({
            'timeout': timedelta(0, 10, 0),
            'group': 'a',
            'a': 'A'}))
        self.assert_signal_notified(Signal({
            'timeout': timedelta(0, 10, 0),
            'group': 'b',
            'b': 'B'}))
        block.stop()

    def test_multiple_intervals(self):
        block = SignalTimeout()
        self.configure_block(block, {
            "intervals": [
                {
                    "interval": {
                        "seconds": 20
                    },
                    "repeatable": True
                },
                {
                    "interval": {
                        "seconds": 30
                    },
                    "repeatable": False
                }


            ]
        })
        block.start()
        block.process_signals([Signal({'a': 'A'})])
        # At time 20
        self.jump_ahead_sleep(20)
        self.assert_num_signals_notified(1, block)
        self.assertDictEqual(self.last_notified[DEFAULT_TERMINAL][0].to_dict(),
                             {'timeout': timedelta(0, 20, 0),
                              'group': None,
                              'a': 'A'})
        # At time 30
        self.jump_ahead_sleep(10)
        self.assert_num_signals_notified(2, block)
        self.assertDictEqual(self.last_notified[DEFAULT_TERMINAL][1].to_dict(),
                             {'timeout': timedelta(0, 30, 0),
                              'group': None,
                              'a': 'A'})
        # At time 40
        self.jump_ahead_sleep(10)
        self.assert_num_signals_notified(3, block)
        self.assertDictEqual(self.last_notified[DEFAULT_TERMINAL][2].to_dict(),
                             {'timeout': timedelta(0, 20, 0),
                              'group': None,
                              'a': 'A'})
        # At time 70 - only one additional signal since 30 is not repeatable
        self.jump_ahead_sleep(30)
        self.assert_num_signals_notified(4, block)
        block.stop()

    def test_persistence(self):
        """Persisted timeout jobs are notified accordingly"""
        block = SignalTimeout()
        # Load from persistence
        persisted_jobs = defaultdict(dict)
        persisted_jobs[1][timedelta(seconds=10)] = Signal({"group": 1})
        persisted_jobs[2][timedelta(seconds=10)] = Signal({"group": 2})
        block._repeatable_jobs = persisted_jobs
        self.configure_block(block, {
            "intervals": [{
                "interval": {"seconds": 10},
                "repeatable": True
            }],
            "group_by": "{{ $group }}"})
        block.start()
        self.assertEqual(len(block._jobs), 2)
        self.assertTrue(
            isinstance(block._jobs[1][timedelta(seconds=10)], Job))
        # Wait for the persisted signal to be notified
        self.jump_ahead_sleep(10)
        self.assert_num_signals_notified(2, block)
        self.assert_signal_notified(Signal({
            'timeout': timedelta(0, 10, 0),
            'group': 1}))
        self.assert_signal_notified(Signal({
            'timeout': timedelta(0, 10, 0),
            'group': 2}))
        # And notified again, since the job is repeatable
        self.jump_ahead_sleep(10)
        self.assert_num_signals_notified(4, block)
        self.assertEqual(self.last_notified[DEFAULT_TERMINAL][2].group, 1)
        # New groups should still be scheduled
        block.process_signals([Signal({"group": 3})])
        # So we get another notification from persistence and the new one
        self.jump_ahead_sleep(10)
        self.assert_num_signals_notified(7, block)
        self.assert_signal_notified(Signal({
            'timeout': timedelta(0, 10, 0),
            'group': 3}))

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
        persisted_jobs[1][timedelta(seconds=10)] = Signal({"group": 1})
        persisted_jobs[2][timedelta(seconds=10)] = Signal({"group": 2})
        block._repeatable_jobs = persisted_jobs
        self.configure_block(block, {
            "intervals": [{
                "interval": {"seconds": 10},
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
