from ..signal_timeout_block import SignalTimeout
from nio.util.support.block_test_case import NIOBlockTestCase
from nio.common.signal.base import Signal
from nio.modules.threading import Event
import datetime


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
