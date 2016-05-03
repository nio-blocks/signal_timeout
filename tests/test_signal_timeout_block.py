import datetime
from datetime import timedelta
from collections import defaultdict
from unittest.mock import patch
from nio.common.signal.base import Signal
from nio.modules.scheduler import Job
from nio.modules.threading import Event
from nio.util.support.block_test_case import NIOBlockTestCase
from ..signal_timeout_block import SignalTimeout


class EventSignalTimeout(SignalTimeout):

    def __init__(self, event):
        super().__init__()
        self._event = event
        self.notified_signals = []

    def notify_signals(self, signals):
        super().notify_signals(signals)
        self.notified_signals = signals
        self._event.set()
        self._event.clear()


class TestSignalTimeout(NIOBlockTestCase):

    def get_test_modules(self):
        return super().get_test_modules() + ['persistence']

    def test_timeout(self):
        event = Event()
        block = EventSignalTimeout(event)
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
        event.wait(.3)
        self.assert_num_signals_notified(1, block)
        self.assertDictEqual(block.notified_signals[0].to_dict(),
                             {'timeout': datetime.timedelta(0, 0, 200000),
                              'group': 'null',
                              'a': 'A'})
        block.stop()

    def test_reset(self):
        """ Make sure the block can reset the intervals """
        event = Event()
        block = EventSignalTimeout(event)
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
        event.wait(0.6)
        block.process_signals([Signal({'b': 'B'})])
        self.assert_num_signals_notified(0, block)
        event.wait(0.6)
        self.assert_num_signals_notified(0, block)
        event.wait(0.6)
        self.assert_num_signals_notified(1, block)
        self.assertDictEqual(block.notified_signals[0].to_dict(),
                             {'timeout': datetime.timedelta(seconds=1),
                              'group': 'null',
                              'b': 'B'})
        block.stop()

    def test_repeatable(self):
        event = Event()
        block = EventSignalTimeout(event)
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
        event.wait(.3)
        self.assert_num_signals_notified(1, block)
        self.assertDictEqual(block.notified_signals[0].to_dict(),
                             {'timeout': datetime.timedelta(0, 0, 200000),
                              'group': 'null',
                              'a': 'A'})
        event.wait(.3)
        self.assert_num_signals_notified(2, block)
        self.assertDictEqual(block.notified_signals[0].to_dict(),
                             {'timeout': datetime.timedelta(0, 0, 200000),
                              'group': 'null',
                              'a': 'A'})
        block.stop()

    def test_groups(self):
        event = Event()
        block = EventSignalTimeout(event)
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
        # Wait for first notification
        event.wait(.3)
        self.assert_num_signals_notified(1, block)
        self.assertDictEqual(block.notified_signals[0].to_dict(),
                             {'timeout': datetime.timedelta(0, 0, 200000),
                              'group': 'a',
                              'a': 'A'})
        # Wait for second notificiation, it should be right after first
        event.wait(.3)
        self.assert_num_signals_notified(2, block)
        self.assertDictEqual(block.notified_signals[0].to_dict(),
                             {'timeout': datetime.timedelta(0, 0, 200000),
                              'group': 'b',
                              'b': 'B'})
        block.stop()

    def test_multiple_intervals(self):
        event = Event()
        block = EventSignalTimeout(event)
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
        event.wait(.3)
        self.assert_num_signals_notified(1, block)
        self.assertDictEqual(block.notified_signals[0].to_dict(),
                             {'timeout': datetime.timedelta(0, 0, 200000),
                              'group': 'null',
                              'a': 'A'})
        event.wait(.3)
        self.assert_num_signals_notified(2, block)
        self.assertDictEqual(block.notified_signals[0].to_dict(),
                             {'timeout': datetime.timedelta(0, 0, 300000),
                              'group': 'null',
                              'a': 'A'})
        event.wait(.3)
        self.assert_num_signals_notified(3, block)
        self.assertDictEqual(block.notified_signals[0].to_dict(),
                             {'timeout': datetime.timedelta(0, 0, 200000),
                              'group': 'null',
                              'a': 'A'})
        event.wait(.3)
        block.stop()

    def test_persist_load(self):
        event = Event()
        block = EventSignalTimeout(event)
        with patch('nio.modules.persistence.default.Persistence.has_key') \
                as has_key:
            with patch('nio.modules.persistence.default.Persistence.load') \
                    as load:
                has_key.return_value = True
                persisted_jobs = defaultdict(dict)
                persisted_jobs[1][timedelta(seconds=0.1)] = \
                    Signal({"group": 1})
                load.return_value = persisted_jobs
                self.configure_block(block, {
                    "intervals": [{"interval":
                                   {"milliseconds": 100},
                                   "repeatable": True
                                   }],
                    "group_by": "{{ $group }}"})
        block.start()
        self.assertEqual(len(block._jobs), 1)
        self.assertTrue(
            isinstance(block._jobs[1][timedelta(seconds=0.1)], Job))
        # Wait for the persisted signal to be notified
        event.wait(0.3)
        self.assert_num_signals_notified(1, block)
        self.assertEqual(block.notified_signals[0].group, 1)
        # And notified again, since the job is repeatable
        event.wait(0.3)
        self.assert_num_signals_notified(2, block)
        self.assertEqual(block.notified_signals[0].group, 1)
        # New groups should still be scheduled
        block.process_signals([Signal({"group": 2})])
        # So we get another notification from persistence
        event.wait(0.3)
        self.assert_num_signals_notified(3, block)
        self.assertEqual(block.notified_signals[0].group, 1)
        # And the new one
        event.wait(0.3)
        self.assert_num_signals_notified(4, block)
        self.assertEqual(block.notified_signals[0].group, 2)
