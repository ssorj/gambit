.. module:: gambit

Gambit
======

.. **Opening connections**

.. **Sending messages**

.. **Receiving messages**

**Blocking**

XXX Blocking operations take optional timeout arguments.  If the timeout
is exceeded, they raise a timeout error.

XXX The endpoint-lifecycle methods `connect()`, `open_<endpoint>()`, and
`close()` start their respective operations but do not complete them.
Use `await <endpoint>.wait()` and `await close()` to block until they are
confirmed by the remote peer.

XXX There is one exception, `open_dynamic_receiver()`.  It blocks until
the remote peer provides the dynamic source address.

XXX Sender `send()` blocks until there is credit to send the message.
It does not wait until the message is acknowledged.  The `send()`
method returns a tracker, on which you can call `await_delivery()` to
block until acknowledgment.

XXX Receiver `receive()` blocks until a message is available to return.

XXX `send()` and `receive()` have non-blocking variants called
`try_send()` and `try_receive()`.  The former returns `None` if there
is no credit to send.  The latter returns `None` if there is no
delivery already present.

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
