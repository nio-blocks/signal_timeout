from collections import defaultdict
from nio.block.base import Block
from nio.util.discovery import discoverable
from nio.properties.timedelta import TimeDeltaProperty
from nio.properties.bool import BoolProperty
from nio.properties.list import ListProperty
from nio.properties.holder import PropertyHolder
from nio.properties.version import VersionProperty
from nio.modules.scheduler import Job
from threading import Lock
from nio.signal.base import Signal
from nio.block.mixins.group_by.group_by import GroupBy


class Interval(PropertyHolder):
    interval = TimeDeltaProperty(title='Interval')
    repeatable = BoolProperty(title='Repeatable',
                              default=False)


@discoverable
class SignalTimeout(GroupBy, Block):

    """ Notifies a timeout signal when no signals have been processed
    by this block for the defined intervals.

    The timeout signal is the last signal that entered the block, with the
    added attributes *timoeut* and *group*.

    Properties:
        group_by (expression): The value by which signals are grouped.
        intervals (list):
            interval (timedelta): Interval to notifiy timeout signal.
            repeatable (bool): If true, notifies every interval without a sig.

    """

    intervals = ListProperty(Interval, title='Timeout Intervals')
    version = VersionProperty('0.1.0')

    def __init__(self):
        super().__init__()
        self._jobs = defaultdict(dict)
        self._jobs_locks = defaultdict(Lock)

    def process_signals(self, signals):
        self.for_each_group(self.process_group, signals)

    def process_group(self, signals, key):
        if len(signals) == 0:
            # No signals actually came through, do nothing
            self.logger.debug("No signals detected for {}".format(key))
            return

        # Lock around the individual group
        with self._jobs_locks[key]:
            # Cancel any existing timeout jobs, then reschedule them
            self._cancel_timeout_jobs(key)
            for interval in self.intervals():
                self._schedule_timeout_job(
                    signals[-1],
                    key,
                    interval.interval(),
                    interval.repeatable())

    def _cancel_timeout_jobs(self, key):
        """ Cancel all the timeouts for a given group """
        self.logger.debug("Cancelling jobs for {}".format(key))
        for job in self._jobs[key].values():
            job.cancel()

    def _schedule_timeout_job(self, signal, key, interval, repeatable):
        self.logger.debug("Scheduling new timeout job for group {}, interval"
                           "={} repeatable={}".format(
                               key, interval, repeatable))
        self._jobs[key][interval] = Job(
            self._timeout_job, interval, repeatable, signal, key, interval)

    def _timeout_job(self, signal, key, interval):
        """ Triggered when an interval times out """
        signal.timeout = interval
        signal.group = key
        self.notify_signals([signal])
