.. module:: gambit

Gambit
======

.. **Opening connections**

.. **Sending messages**

.. **Receiving messages**

**Blocking**

Blocking operations take optional timeout arguments.  If the timeout
is exceeded, they raise a timeout error.

The endpoint-lifecycle methods `connect()`, `open_<endpoint>()`, and
`close()` start their respective operations but do not complete them.
Use `await_open()` and `await_close()` to block until they are
confirmed by the remote peer.

There is one exception, `open_dynamic_receiver()`.  It blocks until
the remote peer provides the dynamic source address.

Sender `send()` blocks until there is credit to send the message.
It does not wait until the message is acknowledged.  The `send()`
method returns a tracker, on which you can call `await_delivery()` to
block until acknowledgment.

Receiver `receive()` blocks until a message is available to return.

`send()` and `receive()` have non-blocking variants called
`try_send()` and `try_receive()`.  The former returns `None` if there
is no credit to send.  The latter returns `None` if there is no
delivery already present.

.. **Thread safety**

.. autoclass:: gambit.Container

.. autoclass:: gambit.Connection
   :exclude-members: send, default_sender
.. autoclass:: gambit.Session
.. autoclass:: gambit.Sender
   :exclude-members: send_request
.. autoclass:: gambit.Receiver
.. autoclass:: gambit.Source
.. autoclass:: gambit.Target

.. autoclass:: gambit.Tracker
.. autoclass:: gambit.Delivery

.. autoclass:: gambit.Message
   :exclude-members: address, decode, encode, recv, send, user_id, DEFAULT_PRIORITY

.. autoclass:: gambit.ErrorCondition
.. autoexception:: Error
   :exclude-members: args, message
.. autoexception:: TimeoutError
   :exclude-members: args, message
.. autoexception:: ConversionError
   :exclude-members: args, message
