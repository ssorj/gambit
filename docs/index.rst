.. module:: gambit

Gambit
======

.. **Opening connections**

.. **Sending messages**

.. **Receiving messages**

.. **Thread safety**

.. autoclass:: gambit.Client

.. autoclass:: gambit.Connection
.. autoclass:: gambit.Session
.. autoclass:: gambit.Sender
.. autoclass:: gambit.Receiver
.. autoclass:: gambit.Source
.. autoclass:: gambit.Target

.. autoclass:: gambit.Tracker
.. autoclass:: gambit.Delivery

.. autoclass:: gambit.Message
   :exclude-members: address, decode, encode, recv, send, user_id, DEFAULT_PRIORITY

.. autoclass:: gambit.ErrorCondition
.. autoexception:: Error
   :exclude-members: args, message, with_traceback
.. autoexception:: TimeoutError
   :exclude-members: args, message, with_traceback
.. autoexception:: ConversionError
   :exclude-members: args, message, with_traceback
