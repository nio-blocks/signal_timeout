from nio.common.block.base import Block
from nio.common.discovery import Discoverable, DiscoverableType
from nio.metadata.properties.timedelta import TimeDeltaProperty
from nio.metadata.properties.bool import BoolProperty
from nio.metadata.properties.list import ListProperty
from nio.metadata.properties.holder import PropertyHolder
from nio.modules.scheduler import Job
from nio.modules.threading import Lock
from nio.common.signal.base import Signal
from .mixins.group_by.group_by_block import GroupBy
from datetime import datetime


class Interval(PropertyHolder):
    interval = TimeDeltaProperty(title='Interval')
    repeatable = BoolProperty(title='Repeatable',
                              default=False)


@Discoverable(DiscoverableType.block)
class SignalTimeout(Block, GroupBy):

    """ Notifies a timeout signal when no signals have been processed
    by this block for the defined intervals.

    Properties:
        group_by (expression): The value by which signals are grouped.
        intervals (list):
            interval (timedelta): Interval to notifiy timeout signal.
            repeatable (bool): If true, notifies every interval without a sig.

    """

    intervals = ListProperty(Interval, title='Timeout Intervals')

    def __init__(self):
        Block.__init__(self)
        GroupBy.__init__(self)
        self._jobs = {}
        self._jobs_lock = Lock()

    def process_signals(self, signals):
        self.for_each_group(self.process_group, signals)

    def process_group(self, signals, key):
        with self._jobs_lock:
            self._cancel_timeout_jobs(key)
            for interval in self.intervals:
                self._schedule_timeout_job(key,
                                           interval.interval,
                                           interval.repeatable)

    def _cancel_timeout_jobs(self, key):
        jobs = self._jobs.get(key)
        if jobs:
            for job in jobs:
                jobs[job].cancel()
        else:
            self._jobs[key] = {}

    def _schedule_timeout_job(self, key, interval, repeatable):
        self._jobs[key][interval] = Job(self._timeout_job,
                                        interval,
                                        repeatable,
                                        key,
                                        interval)

    def _timeout_job(self, key, interval):
        self.notify_signals([Signal({'timeout': interval, 'group': key})])
