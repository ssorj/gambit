Gambit
======

Blocking operations take optional timeout arguments.  If the timeout
is exceeded, they raise a timeout error.

.. module:: gambit

.. data:: IMMEDIATE

   A special timeout value that requests a null return if the desired
   result is unavailable at the time of the call.

   CONSIDER: sender.sendable() and receiver.receivable() instead

.. autoexception:: TimeoutError
   :exclude-members: args, message
   
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
