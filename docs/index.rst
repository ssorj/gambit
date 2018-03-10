Gambit
======

Blocking operations take optional timeout arguments.  If the timeout
is exceeded, they raise a timeout error.

.. autoclass:: gambit.Container
.. autoclass:: gambit.Connection
.. autoclass:: gambit.Sender
.. autoclass:: gambit.Receiver
.. autoclass:: gambit.Tracker
.. autoclass:: gambit.Delivery
.. autoclass:: gambit.Source
.. autoclass:: gambit.Target
.. autoclass:: gambit.Message
   :exclude-members: address, decode, encode, recv, send, user_id, DEFAULT_PRIORITY
