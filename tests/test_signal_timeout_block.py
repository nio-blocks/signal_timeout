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
        block.process_signals([Signal({"a": "A"})])
        self.assert_num_signals_notified(0, block)
        self.jump_ahead_sleep(10)
        self.assert_num_signals_notified(1, block)
        self.assertDictEqual(self.last_notified[DEFAULT_TERMINAL][0].to_dict(),
                             {"timeout": str(timedelta(0, 10, 0)),
                              "group": None,
                              "a": "A"})
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
        block.process_signals([Signal({"interval": 10, "repeatable": True})])
        self.jump_ahead_sleep(10)
        self.assert_num_signals_notified(1, block)
        self.assertDictEqual(self.last_notified[DEFAULT_TERMINAL][0].to_dict(),
                             {"timeout": str(timedelta(0, 10, 0)),
                              "group": None,
                              "interval": 10,
                              "repeatable": True})
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
        block.process_signals([Signal({"a": "A"})])
        # Wait a bit before sending another signal
        self.jump_ahead_sleep(6)
        block.process_signals([Signal({"b": "B"})])
        self.assert_num_signals_notified(0, block)
        self.jump_ahead_sleep(6)
        self.assert_num_signals_notified(0, block)
        self.jump_ahead_sleep(6)
        self.assert_num_signals_notified(1, block)
        self.assertDictEqual(self.last_notified[DEFAULT_TERMINAL][0].to_dict(),
                             {"timeout": str(timedelta(seconds=10)),
                              "group": None,
                              "b": "B"})
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
        block.process_signals([Signal({"a": "A"})])
        self.jump_ahead_sleep(10)
        self.assert_num_signals_notified(1, block)
        self.assertDictEqual(self.last_notified[DEFAULT_TERMINAL][0].to_dict(),
                             {"timeout": str(timedelta(0, 10, 0)),
                              "group": None,
                              "a": "A"})
        self.jump_ahead_sleep(10)
        self.assert_num_signals_notified(2, block)
        self.assertDictEqual(self.last_notified[DEFAULT_TERMINAL][1].to_dict(),
                             {"timeout": str(timedelta(0, 10, 0)),
                              "group": None,
                              "a": "A"})
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
        block.process_signals([Signal({"a": "A", "group": "a"})])
        block.process_signals([Signal({"b": "B", "group": "b"})])
        # Wait for notifications
        self.jump_ahead_sleep(10)
        self.assert_num_signals_notified(2, block)
        self.assert_signal_notified(Signal({
            "timeout": str(timedelta(0, 10, 0)),
            "group": "a",
            "a": "A"}))
        self.assert_signal_notified(Signal({
            "timeout": str(timedelta(0, 10, 0)),
            "group": "b",
            "b": "B"}))
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
        block.process_signals([Signal({"a": "A"})])
        # At time 20
        self.jump_ahead_sleep(20)
        self.assert_num_signals_notified(1, block)
        self.assertDictEqual(self.last_notified[DEFAULT_TERMINAL][0].to_dict(),
                             {"timeout": str(timedelta(0, 20, 0)),
                              "group": None,
                              "a": "A"})
        # At time 30
        self.jump_ahead_sleep(10)
        self.assert_num_signals_notified(2, block)
        self.assertDictEqual(self.last_notified[DEFAULT_TERMINAL][1].to_dict(),
                             {"timeout": str(timedelta(0, 30, 0)),
                              "group": None,
                              "a": "A"})
        # At time 40
        self.jump_ahead_sleep(10)
        self.assert_num_signals_notified(3, block)
        self.assertDictEqual(self.last_notified[DEFAULT_TERMINAL][2].to_dict(),
                             {"timeout": str(timedelta(0, 20, 0)),
                              "group": None,
                              "a": "A"})
        # At time 70 - only one additional signal since 30 is not repeatable
        self.jump_ahead_sleep(30)
        self.assert_num_signals_notified(4, block)
        block.stop()

    def test_persisted_jobs_always_schedule(self):
        """Persisted timeout jobs are not cancelled before they schedule"""

        class TestSignalTimeout(SignalTimeout):

            def __init__(self):
                super().__init__()
                self.event = Event()
                self.schedule_count = 0
                self.cancel_count = 0
                self.latest_signal = None

            def _schedule_timeout_job(self, signal, group, interval, repeatable):
                super()._schedule_timeout_job(
                    signal, group, interval, repeatable)
                self.schedule_count += 1

            def _cancel_timeout_jobs(self, group):
                super()._cancel_timeout_jobs(group)
                self.cancel_count += 1

            def process_signals(self, signals, from_test=False):
                super().process_signals(signals)
                self.latest_signal = signals[-1]
                if from_test:
                    self.event.set()

        block = TestSignalTimeout()
        # Load from persistence
        persisted_jobs = defaultdict(dict)
        persisted_jobs[1][timedelta(seconds=10)] = {
            "signal": Signal({"group": 1}), "repeatable": True}
        persisted_jobs[2][timedelta(seconds=10)] = {
            "signal": Signal({"group": 2}), "repeatable": True}
        block._jobs = persisted_jobs
        self.configure_block(block, {
            "intervals": [{
                "interval": {"seconds": 10},
                "repeatable": True
            }],
            "group_by": "{{ $group }}"})
        # This signal should not cancel the persisted job before it's scheduled
        spawn(block.process_signals, [Signal({
            "group": 2, "from_spawn": True
        })], from_test=True)
        self.assertEqual(block.schedule_count, 0)
        self.assertEqual(block.cancel_count, 0)
        block.start()
        block.event.wait(1)
        # 2 scheduled persisted jobs and one scheduled processed signal
        self.assertEqual(block.schedule_count, 3)
        # Processed signal cancels one of the scheduled jobs
        self.assertEqual(block.cancel_count, 3)
        # Make sure the last signal processed was the one from the spawn
        self.assertTrue(block.latest_signal.from_spawn)

    def test_persistence_logic(self):
        """When stopping, any job that has not yet timed out is persisted.
        Persisted jobs are rescheduled from the time of start."""

        def _configure_block():
            new_block = SignalTimeout()
            self.configure_block(new_block, {
                "intervals": [
                    {"interval": {"seconds": 10}},
                    {"interval": {"seconds": 20}, "repeatable": True}],
                "id": "foo",  # assign a known block id so it can be loaded
            })
            return new_block

        block_0 = _configure_block()
        block_0.start()
        block_0.process_signals([Signal({"pi": 3.14})])
        self.assertEqual(len(block_0._jobs[None]), 2)
        self.jump_ahead_sleep(8)
        block_0.stop()

        block_1 = _configure_block()
        block_1.start()
        self.assertEqual(len(block_1._jobs[None]), 2)
        self.assert_num_signals_notified(0, block_1)
        self.jump_ahead_sleep(2)
        # orignally scheduled timeouts should be ignored
        self.assert_num_signals_notified(0, block_1)
        self.jump_ahead_sleep(8)
        # 10 seconds after start()
        self.assert_num_signals_notified(1, block_1)
        self.assert_last_signal_notified(Signal({
            "group": None,
            "timeout": str(timedelta(seconds=10)),
            "pi": 3.14}))
        self.jump_ahead_sleep(10)
        # 20 seconds after start()
        self.assert_last_signal_notified(Signal({
            "group": None,
            "timeout": str(timedelta(seconds=20)),
            "pi": 3.14}))
        block_1.stop()

        block_2 = _configure_block()
        block_2.start()
        # since an active (repeatable) job was persisted, all
        # intervals are picked up
        self.assertEqual(len(block_2._jobs[None]), 2)
        self.jump_ahead_sleep(10)
        self.assert_num_signals_notified(1, block_2)
        self.jump_ahead_sleep(10)
        # now at 20sec interval
        self.assert_num_signals_notified(2, block_2)
        self.assert_last_signal_notified(Signal({
            "group": None,
            "timeout": str(timedelta(seconds=20)),
            "pi": 3.14}))

    def test_persistence_expiration(self):
        """When stopping, make sure timed out jobs are not persisted"""

        def _configure_block():
            new_block = SignalTimeout()
            self.configure_block(new_block, {
                "intervals": [
                    {"interval": {"seconds": 10}},
                    {"interval": {"seconds": 20}}],
                "id": "foo",  # assign a known block id so it can be loaded
            })
            return new_block

        block_0 = _configure_block()
        block_0.start()
        block_0.process_signals([Signal({"pi": 3.14})])
        self.assertEqual(len(block_0._jobs[None]), 2)
        self.jump_ahead_sleep(25)
        self.assert_num_signals_notified(2, block_0)
        block_0.stop()

        block_1 = _configure_block()
        block_1.start()
        # We won't load any persisted jobs because they timed out
        self.assertEqual(len(block_1._jobs[None]), 0)
        self.assert_num_signals_notified(0, block_1)
        self.jump_ahead_sleep(20)
        # orignally scheduled timeouts should be ignored
        self.assert_num_signals_notified(0, block_1)

    def test_persistence_exceptions(self):
        """ Handle unexpected objects returned from persistence"""
        block = SignalTimeout()
        persisted_jobs = defaultdict(dict)
        # a dictionary is handled and cast to Signal
        persisted_jobs["foo"][timedelta(seconds=10)] = {
            "signal": {"pi": 3.14, "group": "foo"}, "repeatable": False}
        # other objects are not scheduled and log an error
        persisted_jobs["bar"][timedelta(seconds=10)] = {
            "signal": 42, "repeatable": False}
        block._jobs = persisted_jobs
        self.configure_block(block, {
            "intervals": [
                {
                    "interval": {"seconds": 10},
                    "repeatable": False,
                },
            ],
            "group_by": "{{ $group }}",
        })
        block.start()
        # We have two groups but only one job is active
        self.assertEqual(len(block._jobs), 2)
        JumpAheadScheduler.jump_ahead(10)
        self.assert_num_signals_notified(1, block)

    def test_timeout_race_condition(self):
        """ Test that race conditions from timeout and process are handled """
        class SlowNotifyBlock(SignalTimeout):

            def notify_signals(self, signals):
                sleep(0.2)
                super().notify_signals(signals)

        block = SlowNotifyBlock()
        self.configure_block(block, {
            "intervals": [
                {
                    "interval": {"seconds": 10},
                    "repeatable": False,
                },
            ],
            "group_by": "{{ $group }}",
        })
        block.start()
        # Simulate a timeout happening, but this one will be slow to allow
        # another process signals call to come in while it's "timing out"
        spawn(
            block._timeout_job,
            Signal({"2pi": 6.28, "group": "foo"}),
            "foo",
            timedelta(seconds=10))
        block.process_signals([Signal({"pi": 3.14, "group": "foo"})])
        sleep(0.5)
        # After 1 second we should only have seen the signal notified
        # from the timeout job spawn call
        self.assert_num_signals_notified(1, block)
        # After 10 more seconds we shouldn't have any more notifications if
        # we keep the block alive by continuing to process signals on it
        JumpAheadScheduler.jump_ahead(5)
        block.process_signals([Signal({"pi": 3.14, "group": "foo"})])
        JumpAheadScheduler.jump_ahead(5)
        block.process_signals([Signal({"pi": 3.14, "group": "foo"})])
        sleep(0.5)
        self.assert_num_signals_notified(1, block)
        block.stop()
