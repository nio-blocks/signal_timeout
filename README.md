SignalTimeout
=============
Notifies a timeout signal when no signals have been processed by this block for the defined `intervals`. A timeout signal is the last signal to enter the block with an added `group` attribute that specifies the group (default `None`) and a `timeout` attribute that is a python `datetime.timedelta` specifying the configured `interval` that triggered the signal.

Properties
----------
- **backup_interval**: 
- **group_by**: The value by which signals are grouped. Output signals will have `group` set to this value.
- **intervals**: After a signal, if another one does not enter the block for this amount of time, emit a timeout signal.
- **load_from_persistence**: If true, when the block is restarted it will restart with the previous amount of remaining time for the current interval

Inputs
------
Any list of signals.

Outputs
-------
The last signal to enter the block will be notified as a timeout signal. The following two attributes will also be added to the signal.
-   **timeout**: A python `datetime.timedelta` specifying the configured `interval` that triggered the timeout signal.
-   **group**: The group as defined by `group_by`.

Commands
--------
- **groups**: Display the active groups tracked by the block

Dependencies
------------
[GroupBy Block Mixin](https://github.com/nio-blocks/mixins/tree/master/group_by)
