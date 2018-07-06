from collections import defaultdict
from datetime import timedelta
from threading import Event, Lock

from nio import Block, Signal
from nio.properties import TimeDeltaProperty, BoolProperty, ListProperty, \
    PropertyHolder, VersionProperty
from nio.modules.scheduler import Job
from nio.block.mixins import GroupBy, Persistence


class Interval(PropertyHolder):
    interval = TimeDeltaProperty(title="Interval", default={})
    repeatable = BoolProperty(title="Repeatable",
                              default=False)


class SignalTimeout(Persistence, GroupBy, Block):

    """ Notifies a timeout signal when no signals have been processed
    by this block for the defined intervals.

    The timeout signal is the last signal that entered the block, with the
    added attributes *timeout* and *group*.

    Properties:
        group_by (expression): The value by which signals are grouped.
        intervals (list):
            interval (timedelta): Interval to notifiy timeout signal.
            repeatable (bool): If true, notifies every interval without a sig.

    """

    intervals = ListProperty(Interval, title="Timeout Intervals", default=[])
    version = VersionProperty("0.2.0")

    def __init__(self):
        super().__init__()
        self._jobs = defaultdict(dict)
        self._jobs_locks = defaultdict(Lock)
        self._persistence_scheduled = Event()

    def persisted_values(self):
        """Use persistence mixin"""
        return ["_jobs"]

    def start(self):
        super().start()
        # Schedule persisted jobs
        jobs_to_load = self._jobs.copy()
        signals_to_process = []
        for group, group_items in jobs_to_load.items():
            with self._jobs_locks[group]:
                # for each group, grab the first interval's signal and use that
                for interval, timeout_job in group_items.items():
                    signal = timeout_job['signal']
                    if isinstance(signal, dict):  
                        # safepickle 0.2.0 loads Signal as dict
                        signal = Signal(signal)
                    elif not isinstance(signal, Signal):
                        self.logger.error(
                            "Persisted object is not a signal for group "
                            "{}, interval={}".format(group, interval))
                        continue
                    signals_to_process.append(signal)
                    break
        self.for_each_group(self.process_group, signals_to_process)
        self._persistence_scheduled.set()

    def stop(self):
        for group in self._jobs:
            with self._jobs_locks[group]:
                self._cancel_timeout_jobs(group)
        super().stop()

    def process_signals(self, signals):
        self._persistence_scheduled.wait(1)
        self.for_each_group(self.process_group, signals)

    def process_group(self, signals, group):
        if len(signals) == 0:
            # No signals actually came through, do nothing
            self.logger.debug("No signals detected for {}".format(group))
            return
        with self._jobs_locks[group]:
            # Cancel any existing timeout jobs, then reschedule them
            self._cancel_timeout_jobs(group)
            timeout_signal = signals[-1]
            for interval in self.intervals():
                self._schedule_timeout_job(
                    timeout_signal,
                    group,
                    interval.interval(timeout_signal),
                    interval.repeatable(timeout_signal))

    def _cancel_timeout_jobs(self, group):
        """ Cancel the timeouts for a group

            This method must be called from withinn the lock for the group.

            Args:
                group (str): The group of timeouts affected
                include_repeatable (bool): Whether to cancel repeatable jobs
        """
        self.logger.debug("Cancelling jobs for {}".format(group))
        for job in self._jobs[group].values():
            try:
                job.get('job', None).cancel()
            except AttributeError:
                pass # ignore if no job included (when coming from persistence)

    def _schedule_timeout_job(self, signal, group, interval, repeatable):
        self.logger.debug("Scheduling new timeout job for group {}, "
                          "interval={} repeatable={}".format(
                                group, interval, repeatable))
        job = Job(
            self._timeout_job, interval, repeatable, signal, group, interval)
        self._jobs[group][interval] = {
            "signal": signal,
            "job": job,
            "repeatable": repeatable,
        }

    def _timeout_job(self, signal, group, interval):
        """ Triggered when an interval times out (ie, signal not received) """
        signal.timeout = interval
        signal.group = group
        self.notify_signals([signal])
        with self._jobs_locks[group]:
            try:
                timeout_job = self._jobs[group][interval]
                if not timeout_job.get('repeatable'):
                    del self._jobs[group][interval]
            except KeyError:
                self.logger.warning("Non-existent job interval timed out")
