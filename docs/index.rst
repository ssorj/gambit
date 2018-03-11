.. module:: gambit

Gambit
======

.. **Opening connections**

.. **Sending messages**

.. **Receiving messages**

**Blocking**

Blocking operations take optional timeout arguments.  If the timeout
is exceeded, they raise a timeout error.

The endpoint-lifecycle methods `open()` and `close()` start the
operation but do not complete them.  Use `await_open()` and
`await_close()` to block until they are confirmed by the remote peer.

Sender `send()` blocks until there is credit to send the message, but
it does not wait until the message is acknowledged.  Use
`await_delivery()` to do so.

Receiver `receive()` blocks until a message is available to return.
No further blocking is usually required.

.. **Thread safety**

.. data:: IMMEDIATE

   A special timeout value that requests a null return if the desired
   result is unavailable at the time of the call.

   CONSIDER: sender.sendable() and receiver.receivable() instead

.. autoexception:: TimeoutError
   :exclude-members: args, message

.. autoclass:: gambit.Container
.. autoclass:: gambit.Connection
.. autoclass:: gambit.Session
.. autoclass:: gambit.Sender
.. autoclass:: gambit.Receiver
.. autoclass:: gambit.Tracker
.. autoclass:: gambit.Delivery
.. autoclass:: gambit.Source
.. autoclass:: gambit.Target
.. autoclass:: gambit.Message
   :exclude-members: address, decode, encode, recv, send, user_id, DEFAULT_PRIORITY
