#
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.
#

import collections as _collections
import Queue as _queue
import sys as _sys
import threading as _threading
import traceback as _traceback

import proton as _proton
import proton.handlers as _handlers
import proton.reactor as _reactor

_log_mutex = _threading.Lock()

IMMEDIATE = 0

class Container(object):
    def __init__(self, id=None):
        self._proton_object = _reactor.Container(_Handler(self))

        if id is not None:
            self._proton_object.container_id = id

        self._operations = _queue.Queue()
        self._worker_thread = _WorkerThread(self)

        self._event_injector = _reactor.EventInjector()
        self._proton_object.selectable(self._event_injector)

        # proton endpoint => gambit endpoint
        self._endpoints = dict()

        self._connections = set()

    def __enter__(self):
        self.start()
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        self.stop()

    def start(self):
        """
        Make the container operational.  In Gambit's implementation, this starts a worker thread.
        """

        _threading.current_thread().name = "user"
        self._worker_thread.start()

    def stop(self, timeout=None):
        """
        Close any open connections and stop the container.  Blocks until all connections are closed.
        """

        for conn in self._connections:
            conn.close()

        for conn in self._connections:
            conn.await_close()

        self._worker_thread.stop()

    @property
    def id(self):
        """
        The unique identity of the container.
        """
        return self._proton_object.container_id

    def connect(self, host, port, **options):
        """
        Initiate connection open.
        Use :meth:`Connection.await_open()` to block until the remote peer confirms the open.

        :rtype: Connection
        """

        self._log("Connecting to {}:{}", host, port)

        op = _ConnectionOpen(self, host, port)
        conn = op.gambit_object_port.get()

        self._connections.add(conn)

        return conn

    def _log(self, message, *args):
        with _log_mutex:
            message = message.format(*args)
            thread = _threading.current_thread()

            _sys.stdout.write("[{:.4}:{:.4}] {}\n".format(self.id, thread.name, message))
            _sys.stdout.flush()

class _Object(object):
    def __init__(self, container, proton_object):
        self.container = container
        self._proton_object = proton_object

    def __repr__(self):
        return "{}({})".format(self.__class__.__name__, self._proton_object)

class _Operation(object):
    def __init__(self, container):
        self.container = container

        self.proton_object = None
        self.gambit_object_port = _ReturnPort()
        self.completed = _threading.Event()

        self.container._operations.put(self)
        self.container._event_injector.trigger(_reactor.ApplicationEvent("operation_enqueued"))

    def __repr__(self):
        return self.__class__.__name__

    def init(self):
        raise NotImplementedError()

    def complete(self):
        self.container._log("Completing {}", self)
        self.completed.set()

    def await_completion(self):
        self.container._log("Waiting for completion of {}", self)

        while not self.completed.wait(1):
            pass

class _Endpoint(_Object):
    def __init__(self, container, proton_object, open_operation):
        super(_Endpoint, self).__init__(container, proton_object)

        self._open_operation = open_operation
        self._close_operation = None

        self.container._endpoints[self._proton_object] = self

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        self.close()

    def close(self, error_condition=None):
        """
        Initiate close.
        Use :meth:`await_close()` to block until the remote peer confirms the close.
        """

        assert self._close_operation is None
        self._close_operation = _EndpointClose(self.container, self)

    def await_open(self, timeout=None):
        """
        Block until the remote peer confirms the open.
        """

        self._open_operation.await_completion()

    def await_close(self, timeout=None):
        """
        Block until the remote peer confirms the close.

        :rtype: ErrorCondition
        """

        assert self._close_operation is not None
        self._close_operation.await_completion()

class _EndpointClose(_Operation):
    def __init__(self, container, endpoint):
        super(_EndpointClose, self).__init__(container)

        self.endpoint = endpoint

    def init(self):
        self.endpoint._proton_object.close()
        self.proton_object = self.endpoint._proton_object

class Connection(_Endpoint):
    def __init__(self, container, proton_object, open_operation):
        super(Connection, self).__init__(container, proton_object, open_operation)

        self._default_sender = None

    def open_sender(self, address, **options):
        """
        Initiate sender open.
        Use :meth:`Sender.await_open()` to block until the remote peer confirms the open.

        :rtype: Sender
        """

        assert address is not None

        op = _SenderOpen(self.container, self, address)

        return op.gambit_object_port.get()

    def open_receiver(self, address, **options):
        """
        Initiate receiver open.
        Use :meth:`Receiver.await_open()` to block until the remote peer confirms the open.

        :rtype: Receiver
        """

        assert address is not None

        op = _ReceiverOpen(self.container, self, address)

        return op.gambit_object_port.get()

    def open_anonymous_sender(self, **options):
        """
        Initiate open of an unnamed sender.
        See :meth:`open_sender()`.

        :rtype: Sender
        """

        op = _SenderOpen(self.container, self, None)

        return op.gambit_object_port.get()

    def open_dynamic_receiver(self, timeout=None, **options):
        """
        Open a sender with a dynamic source address supplied by the remote peer.
        Blocks until the remote peer confirms the open, providing a source address.

        Note: There is no need to call `receiver.await_open()` for this operation.

        :rtype: Receiver
        """

        op = _ReceiverOpen(self.container, self, None)
        receiver = op.gambit_object_port.get()

        op.await_completion()

        return receiver

    def send(self, message, on_delivery=None, timeout=None):
        """
        Send a message using an anonymous sender.
        See :meth:`Sender.send()`.

        The supplied message must have a non-empty `to` address.

        :rtype: Tracker
        """

        assert message.to is not None

        return self.default_sender.send(message, on_delivery)

    def await_delivery(self, timeout=None):
        """
        Block until the remote peer acknowledges the most recent :meth:`send()`.
        See :meth:`Sender.await_delivery()`.
        """

        self.default_sender.await_delivery()

    @property
    def default_session(self):
        """
        The default session.
        """

    @property
    def default_sender(self):
        """
        The default sender.
        """

        if self._default_sender is None:
            self._default_sender = self.open_anonymous_sender()

        return self._default_sender

class _ConnectionOpen(_Operation):
    def __init__(self, container, host, port):
        super(_ConnectionOpen, self).__init__(container)

        self.host = host
        self.port = port

    def init(self):
        pn_container = self.container._proton_object
        conn_url = "amqp://{}:{}".format(self.host, self.port)

        self.proton_object = pn_container.connect(conn_url, allowed_mechs=b"ANONYMOUS")
        self.gambit_object_port.put(Connection(self.container, self.proton_object, self))

class Session(_Endpoint):
    pass

# class _SessionOpen(_Operation):
#     def init(self):
#         pn_container = self.container._proton_object
#         connection_url = "amqp://{}:{}".format(self.host, self.port)

#         self.proton_object = pn_container.connect(connection_url, allowed_mechs=b"ANONYMOUS")
#         self.gambit_object_port.put(Connection(self.container, self.proton_object, self))

class _Link(_Endpoint):
    def __init__(self, container, proton_object, open_operation):
        super(_Link, self).__init__(container, proton_object, open_operation)

        self._connection = self.container._endpoints[self._proton_object.connection]
        self._target = Target(self.container, self._proton_object.remote_target)
        self._source = Source(self.container, self._proton_object.remote_source)

    @property
    def connection(self):
        """
        The connection containing this sender.
        """

        return self._connection

    @property
    def source(self):
        """
        The source terminus.
        """

        return self._source

    @property
    def target(self):
        """
        The target terminus.
        """

        return self._target

class Sender(_Link):
    def __init__(self, container, proton_object, open_operation):
        super(Sender, self).__init__(container, proton_object, open_operation)

        self._connection = self.container._endpoints[self._proton_object.connection]
        self._target = Target(self.container, self._proton_object.remote_target)

        self._message_queue = _queue.Queue()
        self._tracker_port = _ReturnPort()

        self._last_tracker = None

    def send(self, message, on_delivery=None, timeout=None):
        """
        Send a message.

        Blocks until credit is available and the message can be sent.
        Use :meth:`await_delivery()` to block until the remote peer acknowledges the message.

        If set, `on_delivery(tracker)` is called after the delivery is acknowledged.

        CONSIDER: Talk about threading issues arising from use of on_delivery.

        :rtype: Tracker
        """

        #self._message_delivered.clear()

        self._message_queue.put((message, on_delivery))

        event = _reactor.ApplicationEvent("message_enqueued", subject=self._proton_object)
        self.container._event_injector.trigger(event)

        tracker = self._tracker_port.get()
        self._last_tracker = tracker

        return tracker

    def await_delivery(self, timeout=None):
        """
        Block until the remote peer acknowledges the most recent :meth:`send()`.
        """

        self._last_tracker.await_delivery()

    def send_request(self, message, receiver=None, timeout=None):
        """
        CONSIDER:

        Send a request message mapped to a receiver for collecting the response.
        Use :meth:`Receiver.receive()` to get responses.

        The `reply_to` address of `message` is set to the source address of the receiver.

        If `receiver` is none, a receiver is created internally using :meth:`open_dynamic_receiver()`.

        :rtype: Receiver
        """

        if receiver is None:
            receiver = self.connection.open_dynamic_receiver(timeout=timeout)

        message.reply_to = receiver.source.address

        self.send(message, timeout=timeout)

        return receiver

class _SenderOpen(_Operation):
    def __init__(self, container, connection, address):
        super(_SenderOpen, self).__init__(container)

        self.connection = connection
        self.address = address

    def init(self):
        pn_container = self.container._proton_object
        pn_connection = self.connection._proton_object

        self.proton_object = pn_container.create_sender(pn_connection, self.address)
        self.gambit_object_port.put(Sender(self.container, self.proton_object, self))

class Receiver(_Link):
    """
    A receiver is an iterable object.
    Each item returned during iteration is a message obtained by calling `receive()`.
    """

    def __init__(self, container, proton_object, open_operation):
        super(Receiver, self).__init__(container, proton_object, open_operation)

        self._delivery_queue = _queue.Queue()

    def receive(self, timeout=None):
        """
        Receive a delivery containing a message.  Blocks until a message is available.

        CONSIDER: Flow one credit if credit is zero for a more convenient no-prefetch mode.

        :rtype: Delivery
        """

        pn_delivery, pn_message = self._delivery_queue.get()
        return Delivery(self.container, pn_delivery, pn_message)

    def __iter__(self):
        return _ReceiverIterator(self)

class _ReceiverIterator(object):
    def __init__(self, receiver):
        self._receiver = receiver

    def next(self):
        return self._receiver.receive()

class _ReceiverOpen(_Operation):
    def __init__(self, container, connection, address):
        super(_ReceiverOpen, self).__init__(container)

        self.connection = connection
        self.address = address

    def init(self):
        pn_container = self.container._proton_object
        pn_connection = self.connection._proton_object
        dynamic = False

        if self.address is None:
            dynamic = True

        self.proton_object = pn_container.create_receiver(pn_connection, self.address, dynamic=dynamic)
        self.gambit_object_port.put(Receiver(self.container, self.proton_object, self))

class _Terminus(_Object):
    def _get_address(self):
        """
        The source or target address.
        """
        return self._proton_object.address

    def _set_address(self, address):
        self._proton_object.address = address

    address = property(_get_address, _set_address)

class Source(_Terminus):
    pass

class Target(_Terminus):
    pass

class _Transfer(_Object):
    def __init__(self, container, proton_object, message):
        super(_Transfer, self).__init__(container, proton_object)

        self._message = message

    @property
    def message(self):
        """
        The message associated with this transfer.
        """

        return self._message

    def settle(self):
        """
        Tell the remote peer the transfer is settled.
        """

    def settled(self):
        """
        Return true if the transfer is settled.
        """

class Tracker(_Transfer):
    def __init__(self, container, proton_object, message):
        super(Tracker, self).__init__(container, proton_object, message)

        self._message_delivered = _threading.Event()

    @property
    def state(self):
        """
        The state of the delivery as reported by the remote peer.
        """

        return self._proton_object.remote_state

    @property
    def sender(self):
        """
        The sender containing this tracker.
        """

    def await_delivery(self):
        """
        Block until the remote peer acknowledges the tracked message.
        """

        while not self._message_delivered.wait(1):
            pass

class Delivery(_Transfer):
    def accept(self):
        """
        Tell the remote peer the delivery is accepted.
        """

    def reject(self):
        """
        Tell the remote peer the delivery is rejected.
        """

    def release(self):
        """
        Tell the remote peer the delivery is released.
        """

    @property
    def receiver(self):
        """
        The receiver containing this delivery.
        """

class Message(_proton.Message):
    def _get_to(self):
        """
        The destination address of the message.
        """
        return self.address

    def _set_to(self, address):
        self.address = address

    to = property(_get_to, _set_to)

    def _get_user(self):
        """
        The user associated with this message.
        """
        return self.user_id

    def _set_user(self, user_id):
        self.user_id = user_id

    user = property(_get_user, _set_user)

class ErrorCondition(_Object):
    @property
    def name(self):
        """
        The name of the error condition.
        """

    @property
    def description(self):
        """
        A description of the error condition.
        """

    @property
    def properties(self):
        """
        Extra information about the error condition.
        
        :rtype: dict
        """

class Error(Exception):
    """
    The base Gambit API error.
    """
        
class TimeoutError(Error):
    """
    Raised if the requested timeout of a blocking operation is exceeded.
    """

class ConversionError(Error):
    """
    Raised if data cannot be converted.
    """

class _WorkerThread(_threading.Thread):
    def __init__(self, container):
        _threading.Thread.__init__(self)

        self.container = container
        self.name = "worker"
        self.daemon = True

    def start(self):
        self.container._log("Starting the worker thread")

        _threading.Thread.start(self)

    def run(self):
        try:
            self.container._proton_object.run()
        except KeyboardInterrupt:
            raise

    def stop(self):
        self.container._log("Stopping the worker thread")

        self.container._proton_object.stop()

class _Handler(_handlers.MessagingHandler):
    def __init__(self, container):
        super(_Handler, self).__init__()

        self.container = container

        # (operation class, proton endpoint) => operation
        self.pending_operations = dict()
        # proton delivery => (proton_message, on_delivery)
        self.pending_deliveries = dict()

    def on_operation_enqueued(self, event):
        op = self.container._operations.get()
        op.init()

        assert op.proton_object is not None

        self.pending_operations[(op.__class__, op.proton_object)] = op

    def on_connection_opened(self, event):
        op = self.pending_operations.pop((_ConnectionOpen, event.connection))
        op.complete()

    def on_connection_closed(self, event):
        op = self.pending_operations.pop((_EndpointClose, event.connection))
        op.complete()

    def on_link_opened(self, event):
        if event.link.is_sender:
            op = self.pending_operations.pop((_SenderOpen, event.link))
        else:
            op = self.pending_operations.pop((_ReceiverOpen, event.link))

        op.complete()

    def on_link_closed(self, event):
        op = self.pending_operations.pop((_EndpointClose, event.link))
        op.complete()

    def on_acknowledged(self, event):
        tracker, on_delivery = self.pending_deliveries.pop(event.delivery)
        tracker._message_delivered.set()

        if on_delivery is not None:
            on_delivery(tracker)

    def on_accepted(self, event):
        self.on_acknowledged(event)

    def on_rejected(self, event):
        self.on_acknowledged(event)

    def on_released(self, event):
        self.on_acknowledged(event)

    def on_sendable(self, event):
        self.send_messages(event.sender)

    def on_message_enqueued(self, event):
        self.send_messages(event.subject)

    def send_messages(self, sender):
        gb_sender = self.container._endpoints[sender]
        queue = gb_sender._message_queue
        port = gb_sender._tracker_port

        while sender.credit > 0 and not queue.empty():
            message, on_delivery = queue.get()

            delivery = sender.send(message)
            tracker = Tracker(self.container, delivery, message)

            port.put(tracker)

            self.pending_deliveries[delivery] = (tracker, on_delivery)

    def on_message(self, event):
        gb_receiver = self.container._endpoints[event.receiver]
        gb_receiver._delivery_queue.put((event.delivery, event.message))

class _ReturnPort(object):
    def __init__(self):
        self.value = None

        self.lock = _threading.Lock()
        self.empty = _threading.Condition(self.lock)
        self.full = _threading.Condition(self.lock)

    def put(self, value):
        assert value is not None

        with self.empty:
            while self.value is not None:
                self.empty.wait()

            self.value = value
            self.full.notify()

    def get(self):
        with self.full:
            while self.value is None:
                self.full.wait()

            value = self.value
            self.value = None

            self.empty.notify()

            return value
