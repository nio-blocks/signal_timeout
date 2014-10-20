from ..signal_timeout_block import SignalTimeout
from unittest.mock import MagicMock
from nio.util.support.block_test_case import NIOBlockTestCase
from nio.common.signal.base import Signal
from nio.modules.threading import Event
from time import sleep
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


class TestBuffer(NIOBlockTestCase):

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
        block.process_signals([Signal()])
        self.assert_num_signals_notified(0, block)
        event.wait(.3)
        self.assert_num_signals_notified(1, block)
        self.assertDictEqual(block.notified_signals[0].to_dict(),
                             {'timeout': datetime.timedelta(0, 0, 200000),
                              'group': 'null'})
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
        block.process_signals([Signal()])
        event.wait(.3)
        self.assert_num_signals_notified(1, block)
        self.assertDictEqual(block.notified_signals[0].to_dict(),
                             {'timeout': datetime.timedelta(0, 0, 200000),
                              'group': 'null'})
        event.wait(.3)
        self.assert_num_signals_notified(2, block)
        self.assertDictEqual(block.notified_signals[0].to_dict(),
                             {'timeout': datetime.timedelta(0, 0, 200000),
                              'group': 'null'})
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
        block.process_signals([Signal({'group': 'a'})])
        block.process_signals([Signal({'group': 'b'})])
        event.wait(.3)
        self.assert_num_signals_notified(1, block)
        self.assertDictEqual(block.notified_signals[0].to_dict(),
                             {'timeout': datetime.timedelta(0, 0, 200000),
                              'group': 'a'})
        event.wait(.3)
        self.assert_num_signals_notified(2, block)
        self.assertDictEqual(block.notified_signals[0].to_dict(),
                             {'timeout': datetime.timedelta(0, 0, 200000),
                              'group': 'b'})
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
        block.process_signals([Signal()])
        event.wait(.3)
        self.assert_num_signals_notified(1, block)
        self.assertDictEqual(block.notified_signals[0].to_dict(),
                             {'timeout': datetime.timedelta(0, 0, 200000),
                              'group': 'null'})
        event.wait(.3)
        self.assert_num_signals_notified(2, block)
        self.assertDictEqual(block.notified_signals[0].to_dict(),
                             {'timeout': datetime.timedelta(0, 0, 300000),
                              'group': 'null'})
        event.wait(.3)
        self.assert_num_signals_notified(3, block)
        self.assertDictEqual(block.notified_signals[0].to_dict(),
                             {'timeout': datetime.timedelta(0, 0, 200000),
                              'group': 'null'})
        block.stop()
