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

class Container(object):
    def __init__(self, id=None):
        self._proton_object = _reactor.Container(_Handler(self))

        if id is not None:
            self._proton_object.container_id = id

        self._operations = _queue.Queue()
        self._worker_thread = _WorkerThread(self)

        self._event_injector = _reactor.EventInjector()
        self._proton_object.selectable(self._event_injector)

        self._senders_by_proton_object = dict()
        self._receivers_by_proton_object = dict()
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
        Use `connection.await_open()` to block until the remote peer confirms the open.

        :rtype: Connection
        """

        self._log("Connecting to {}:{}", host, port)

        op = _ConnectionOpen(self, host, port)
        op.await_start()

        self._connections.add(op.gambit_object)

        return op.gambit_object

    def _log(self, message, *args):
        with _log_mutex:
            message = message.format(*args)
            thread = _threading.current_thread()

            _sys.stdout.write("[{:.4}:{:.4}] {}\n".format(self.id, thread.name, message))
            _sys.stdout.flush()

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
        self.gambit_object = None

        self.started = _threading.Event()
        self.completed = _threading.Event()

        self.container._operations.put(self)
        self.container._event_injector.trigger(_reactor.ApplicationEvent("operation_enqueued"))

    def __repr__(self):
        return self.__class__.__name__

    def start(self):
        self.container._log("Starting {}", self)

        self.on_start()

        assert self.proton_object is not None
        assert self.gambit_object is not None

        self.started.set()

    def on_start(self):
        raise NotImplementedError()

    def await_start(self):
        self.container._log("Waiting for start of {}", self)

        while not self.started.wait(1):
            pass

    def complete(self):
        self.container._log("Completing {}", self)

        self.on_completion()
        self.completed.set()

    def on_completion(self):
        pass

    def await_completion(self):
        self.container._log("Waiting for completion of {}", self)

        while not self.completed.wait(1):
            pass

class _Endpoint(_Object):
    def __init__(self, container, proton_object, open_operation):
        super(_Endpoint, self).__init__(container, proton_object)

        self._open_operation = open_operation
        self._close_operation = None

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        self.close()

    def close(self, error=None):
        """
        Initiate close.
        Use `await_close()` to block until the remote peer confirms the close.
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
        """

        assert self._close_operation is not None
        self._close_operation.await_completion()

class _EndpointClose(_Operation):
    def __init__(self, container, endpoint):
        super(_EndpointClose, self).__init__(container)

        self.endpoint = endpoint

    def on_start(self):
        self.endpoint._proton_object.close()

        self.proton_object = self.endpoint._proton_object
        self.gambit_object = self.endpoint

class Connection(_Endpoint):
    def __init__(self, container, proton_object, open_operation):
        super(Connection, self).__init__(container, proton_object, open_operation)

        self._anonymous_sender = None

    def open_sender(self, address, **options):
        """
        Initiate sender open.
        Use `sender.await_open()` to block until the remote peer confirms the open.

        :rtype: Sender
        """

        assert address is not None

        op = _SenderOpen(self.container, self, address)
        op.await_start()

        return op.gambit_object

    def open_receiver(self, address, **options):
        """
        Initiate receiver open.
        Use `receiver.await_open()` to block until the remote peer confirms the open.

        :rtype: Receiver
        """

        assert address is not None

        op = _ReceiverOpen(self.container, self, address)
        op.await_start()

        return op.gambit_object

    def open_anonymous_sender(self, **options):
        """
        Initiate open of an unnamed sender.
        Use `sender.await_open()` to block until the remote peer confirms the open.

        :rtype: Sender
        """

        op = _SenderOpen(self.container, self, None)
        op.await_start()

        return op.gambit_object

    def open_dynamic_receiver(self, **options):
        """
        Open a sender with a dynamic source address supplied by the remote peer.
        Blocks until the remote peer confirms the open, providing a source address.

        Note: There is no need to call `receiver.await_open()` for this operation.

        :rtype: Receiver
        """

        op = _ReceiverOpen(self.container, self, None)
        op.await_completion()

        return op.gambit_object

    def send(self, message, completion_fn=None, timeout=None):
        """
        Send a message using an anonymous sender.
        The supplied message must have a non-empty `to` address.

        Blocks until credit is available and the message can be sent.
        Use `await_ack()` to block until the remote peer acknowledges the delivery.

        If set, `completion_fn(delivery)` is called after the delivery is acknowledged.
        """
        # XXX Use copydoc

        assert message.to is not None

        self._get_anonymous_sender().send(message, completion_fn)

    def await_ack(self, timeout=None):
        """
        Block until the remote peer acknowledges the most recent send.

        :rtype: Tracker
        """
        # XXX Use copydoc

        return self._get_anonymous_sender().await_ack()

    def _get_anonymous_sender(self):
        if self._anonymous_sender is None:
            self._anonymous_sender = self.open_anonymous_sender()

        return self._anonymous_sender

class _ConnectionOpen(_Operation):
    def __init__(self, container, host, port):
        super(_ConnectionOpen, self).__init__(container)

        self.host = host
        self.port = port

    def on_start(self):
        pn_container = self.container._proton_object
        connection_url = "amqp://{}:{}".format(self.host, self.port)

        self.proton_object = pn_container.connect(connection_url, allowed_mechs=b"ANONYMOUS")
        self.gambit_object = Connection(self.container, self.proton_object, self)

class Session(_Endpoint):
    pass

class Sender(_Endpoint):
    def __init__(self, container, proton_object, open_operation):
        super(Sender, self).__init__(container, proton_object, open_operation)

        self._target = Target(self.container, self._proton_object.remote_target)

        self._message_queue = _queue.Queue()
        self._tracker_queue = _queue.Queue(1)

        self._message_sent = _threading.Event()
        self._message_acked = _threading.Event()

        self.container._senders_by_proton_object[self._proton_object] = self

    @property
    def target(self):
        """
        The target terminus.
        """

        return self._target

    def send(self, message, completion_fn=None, timeout=None):
        """
        Send a message.

        Blocks until credit is available and the message can be sent.
        Use `await_ack()` to block until the remote peer acknowledges the delivery.

        If set, `completion_fn(delivery)` is called after the delivery is acknowledged.
        """

        self._message_sent.clear()
        self._message_acked.clear()

        self._message_queue.put((message, completion_fn))

        event = _reactor.ApplicationEvent("message_enqueued", subject=self._proton_object)
        self.container._event_injector.trigger(event)

        while not self._message_sent.wait(1):
            pass

    def await_ack(self, timeout=None):
        """
        Block until the remote peer acknowledges the most recent send.

        :rtype: Tracker
        """

        while not self._message_acked.wait(1):
            pass

        return self._tracker_queue.get()

class _SenderOpen(_Operation):
    def __init__(self, container, connection, address):
        super(_SenderOpen, self).__init__(container)

        self.connection = connection
        self.address = address

    def on_start(self):
        pn_container = self.container._proton_object
        pn_connection = self.connection._proton_object

        self.proton_object = pn_container.create_sender(pn_connection, self.address)
        self.gambit_object = Sender(self.container, self.proton_object, self)

class Receiver(_Endpoint):
    """
    A receiver is an iterable object.
    Each item returned during iteration is a message obtained by calling `receive()`.
    """

    def __init__(self, container, proton_object, open_operation):
        super(Receiver, self).__init__(container, proton_object, open_operation)

        self._source = Source(self.container, self._proton_object.remote_source)

        self._delivery_queue = _queue.Queue()

        self.container._receivers_by_proton_object[self._proton_object] = self

    @property
    def source(self):
        """
        The source terminus.
        """

        return self._source

    def receive(self, timeout=None):
        """
        Receive a delivery containing a message.  Blocks until a message is available.

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

    def on_start(self):
        pn_container = self.container._proton_object
        pn_connection = self.connection._proton_object
        dynamic = False

        if self.address is None:
            dynamic = True

        self.proton_object = pn_container.create_receiver(pn_connection, self.address, dynamic=dynamic)
        self.gambit_object = Receiver(self.container, self.proton_object, self)

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
        # proton delivery => (proton_message, completion_fn)
        self.pending_deliveries = dict()

    def on_operation_enqueued(self, event):
        op = self.container._operations.get()
        op.start()

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
        gb_sender = self.container._senders_by_proton_object[event.delivery.link]
        queue = gb_sender._tracker_queue
        acked = gb_sender._message_acked

        message, completion_fn = self.pending_deliveries.pop(event.delivery)
        tracker = Tracker(self.container, event.delivery, message)

        with queue.mutex:
            try:
                queue._get()
            except IndexError:
                pass

            queue._put(tracker)

        acked.set()

        if completion_fn is not None:
            completion_fn(tracker)

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
        gb_sender = self.container._senders_by_proton_object[sender]
        queue = gb_sender._message_queue
        sent = gb_sender._message_sent

        while sender.credit > 0 and not queue.empty():
            message, completion_fn = queue.get()

            delivery = sender.send(message)
            sent.set()

            self.pending_deliveries[delivery] = (message, completion_fn)

    def on_message(self, event):
        gb_receiver = self.container._receivers_by_proton_object[event.receiver]
        gb_receiver._delivery_queue.put((event.delivery, event.message))
