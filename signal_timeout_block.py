from collections import defaultdict
from nio.block.base import Block
from nio.util.discovery import discoverable
from nio.properties.timedelta import TimeDeltaProperty
from nio.properties.bool import BoolProperty
from nio.properties.list import ListProperty
from nio.properties.holder import PropertyHolder
from nio.properties.version import VersionProperty
from nio.modules.scheduler import Job
from threading import Event, Lock
from nio.signal.base import Signal
from nio.block.mixins.group_by.group_by import GroupBy
from nio.block.mixins.persistence.persistence import Persistence


class Interval(PropertyHolder):
    interval = TimeDeltaProperty(title='Interval', default={})
    repeatable = BoolProperty(title='Repeatable',
                              default=False)


@discoverable
class SignalTimeout(Persistence, GroupBy, Block):

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

    intervals = ListProperty(Interval, title='Timeout Intervals', default=[])
    version = VersionProperty('0.1.0')

    def __init__(self):
        super().__init__()
        self._jobs = defaultdict(dict)
        self._jobs_locks = defaultdict(Lock)
        self._repeatable_jobs = defaultdict(dict)
        self._persisted_jobs = None
        self._persistence_scheduled = Event()

    def persisted_values(self):
        """Use persistence mixin"""
        return ["_repeatable_jobs"]

    def configure(self, context):
        super().configure(context)
        # Save off the persisted jobs here in case self._reapeatable_jobs is
        # modifed by a processed signal before `start` schedules this.
        self._persisted_jobs = self._repeatable_jobs

    def start(self):
        super().start()
        # Schedule persisted jobs
        for key, intervals in self._persisted_jobs.items():
            with self._jobs_locks[key]:
                for interval, job in intervals.items():
                    if interval not in self._jobs[key]:
                        # On rare occassions, a new timeout job may have been
                        # scheduled before this persisted job is scheduled, so
                        # only do this if one has not been scheduled already.
                        self._schedule_timeout_job(job, key, interval, True)
        self._persistence_scheduled.set()

    def process_signals(self, signals):
        self._persistence_scheduled.wait(1)
        self.for_each_group(self.process_group, signals)

    def process_group(self, signals, key):
        if len(signals) == 0:
            # No signals actually came through, do nothing
            self.logger.debug("No signals detected for {}".format(key))
            return
        with self._jobs_locks[key]:
            # Cancel any existing timeout jobs, then reschedule them
            self._cancel_timeout_jobs(key)
            for interval in self.intervals():
                self._schedule_timeout_job(
                    signals[-1],
                    key,
                    interval.interval(signals[-1]),
                    interval.repeatable(signals[-1]))

    def _cancel_timeout_jobs(self, key):
        """ Cancel all the timeouts for a given group """
        self.logger.debug("Cancelling jobs for {}".format(key))
        for job in self._jobs[key].values():
            job.cancel()
        if key in self._repeatable_jobs:
            del self._repeatable_jobs[key]

    def _schedule_timeout_job(self, signal, key, interval, repeatable):
        self.logger.debug("Scheduling new timeout job for group {}, interval"
                           "={} repeatable={}".format(
                               key, interval, repeatable))
        self._jobs[key][interval] = Job(
            self._timeout_job, interval, repeatable, signal, key, interval)
        if repeatable:
            self._repeatable_jobs[key][interval] = signal

    def _timeout_job(self, signal, key, interval):
        """ Triggered when an interval times out """
        signal.timeout = interval
        signal.group = key
        self.notify_signals([signal])
