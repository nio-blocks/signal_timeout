SignalTimeout
=======

Notifies a timeout signal when no signals have been processed by this block for the defined intervals. A timeout signal is the last signal to enter the block with and added *group* attribute that specifies the group (default 'null') and a *timeout* attribute that is a python datetime.timedelta specifying the configured interval that triggered the signal.

Group-by functionality will create timeout signals for each registered group. A signal needs to come in to the block to initialize a group and start emitting timeout signals.

Properties
--------------

-   **intervals**:
    -   **interval**: After a signal, if another one does not enter the block for this amount of time, emit a timeout signal.
    -   **repeatable**: If true, a timeout signal is emitted every interval without another input signal instead of just once.
-   **group_by**: Expression proprety. The value by which signals are grouped. Output signals will have *group* set to this value.


Dependencies
----------------
[GroupBy Block Mixin](https://github.com/nio-blocks/mixins/tree/master/group_by)

Commands
----------------
None

Input
-------
Any list of signals.

Output
---------

The last signal to enter the block will be notified as a timeout signal. The following two attributes will also be added to the signal.

-   **timeout**: A python datetime.timedelta specifying the configured **interval** that triggered the timeout signal.
-   **group**: The group as defined by **group_by**.
