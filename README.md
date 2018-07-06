SignalTimeout
=============
The SignalTimeout block will emit a timeout signal when no signals have been processed for the defined `intervals`. A timeout signal is the last signal to enter the block with an added `group` attribute that specifies the group (default `None`) and a `timeout` attribute that is a python `datetime.timedelta` specifying the configured `interval` that triggered the signal.

Properties
----------
- **backup_interval**: An interval of time that specifies how often persisted data is saved.
- **group_by**: The signal attribute on the incoming signal whose values will be used to define groups on the outgoing signal.
- **intervals**: After a signal, if another one does not enter the block for this amount of time, emit a timeout signal.
- **load_from_persistence**: When stopped, the last signal for each group is saved if the group still has active jobs (one or more interval has not yet timed out). When started, if `load_from_persistence` is `True`, saved signals will be processed, starting all configured timeout intervals for that group.

Inputs
------
- **default**: Any list of signals.

Outputs
-------
- **default**: The last signal to enter the block with additional attributes **timeout** and **group**.

Commands
--------
- **groups**: Display the active groups tracked by the block

Dependencies
------------
None

