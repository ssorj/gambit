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

import asyncio as _asyncio
import collections as _collections
import queue as _queue
import sys as _sys
import threading as _threading
import traceback as _traceback

import proton as _proton
import proton.handlers as _handlers
import proton.reactor as _reactor

_log_mutex = _threading.Lock()

class Container:
    def __init__(self, id=None):
        self._pn_object = _reactor.Container(_Handler(self))

        if id is not None:
            self._pn_object.container_id = id

        self._operations = _queue.Queue()
        self._worker_thread = _WorkerThread(self)
        self._lock = _threading.Lock()

        self._event_injector = _reactor.EventInjector()
        self._pn_object.selectable(self._event_injector)

        # proton endpoint => gambit endpoint
        self._endpoints = dict()

        self._connections = set()

    def __enter__(self):
        _threading.current_thread().name = "user"

        with self._lock:
            self._worker_thread.start()

        return self

    def __exit__(self, exc_type, exc_value, traceback):
        self.stop()

    def stop(self, timeout=None):
        """
        Close any open connections and stop the container.  Blocks until all connections are closed.
        """

        # XXX Implement timeout

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
        return self._pn_object.container_id

    def _fire_event(self, event_name, *args):
        future = _asyncio.get_event_loop().create_future()
        event = _reactor.ApplicationEvent(event_name, subject=(future, *args))

        self._event_injector.trigger(event)

        return future

    def connect(self, conn_url, **options):
        """
        Initiate connection open.
        Use :meth:`Connection.await_open()` to block until the remote peer confirms the open.

        :rtype: Connection
        """

        self._log("Connecting to {}", conn_url)

        with self._lock:
            if not self._worker_thread.is_alive():
                self._worker_thread.start()

        return self._fire_event("gambit_connect", conn_url)

    def _log(self, message, *args):
        with _log_mutex:
            message = message.format(*args)
            thread = _threading.current_thread()

            _sys.stdout.write("[{:.4}:{:.4}] {}\n".format(self.id, thread.name, message))
            _sys.stdout.flush()

class _Object:
    def __init__(self, container, proton_object):
        self.container = container
        self._pn_object = proton_object

    def __repr__(self):
        return "{}({})".format(self.__class__.__name__, self._pn_object)

class _Endpoint(_Object):
    def __init__(self, container, proton_object, open_operation):
        super(_Endpoint, self).__init__(container, proton_object)

        self.container._endpoints[self._pn_object] = self

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        self.close()

    def close(self, error_condition=None):
        """
        Initiate close.
        Use :meth:`await_close()` to block until the remote peer confirms the close.
        """

        return self.container._fire_event("gambit_close_endpoint", self._pn_object)

class Connection(_Endpoint):
    def __init__(self, container, pn_object, open_operation):
        super(Connection, self).__init__(container, pn_object, open_operation)

        self._default_sender = None

    def open_session(self, **options):
        return self.container._fire_event("gambit_open_session", self._pn_object)

    def open_sender(self, address, **options):
        """
        Initiate sender open.
        XXX Use :meth:`Sender.await_open()` to block until the remote peer confirms the open.

        :rtype: XXX Sender
        """

        assert address is not None

        return self.container._fire_event("gambit_open_sender", self._pn_object, address)

    def open_receiver(self, address, on_message=None, **options):
        """
        Initiate receiver open.
        Use :meth:`Receiver.await_open()` to block until the remote peer confirms the open.

        If set, `on_message(delivery)` is called when a message is received.
        It is called on another thread, not the main API thread.
        Users must take care to use thread-safe code in the callback.

        :rtype: XXX Receiver
        """

        assert address is not None

        return self.container._fire_event("gambit_open_receiver", self._pn_object, address)

    def open_anonymous_sender(self, **options):
        """
        Initiate open of an unnamed sender.
        See :meth:`open_sender()`.

        :rtype: Sender
        """

        raise NotImplementedError()

    def open_dynamic_receiver(self, timeout=None, **options):
        """
        Open a sender with a dynamic source address supplied by the remote peer.
        See :meth:`open_receiver()`.

        REVIEW: Unlike the other open methods, this one blocks until the remote peer
        confirms the open and supplies the source address.

        :rtype: Receiver
        """

        raise NotImplementedError()

    @property
    def default_session(self):
        """
        The default session.
        """

        raise NotImplementedError()

class Session(_Endpoint):
    pass

class _Link(_Endpoint):
    def __init__(self, container, proton_object, open_operation):
        super(_Link, self).__init__(container, proton_object, open_operation)

        self._connection = self.container._endpoints[self._pn_object.connection]
        self._target = Target(self.container, self._pn_object.remote_target)
        self._source = Source(self.container, self._pn_object.remote_source)

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

        self._connection = self.container._endpoints[self._pn_object.connection]
        self._target = Target(self.container, self._pn_object.remote_target)

        # self._message_queue = _queue.Queue() # XXX this may come back for blocking

    def send(self, message, timeout=None, on_delivery=None):
        """
        Send a message.

        Blocks until credit is available and the message can be sent.
        Use :meth:`Tracker.await_delivery()` to block until the remote peer acknowledges
        the message.

        If set, `on_delivery(tracker)` is called after the delivery is acknowledged.
        It is called on another thread, not the main API thread.
        Users must take care to use thread-safe code in the callback.

        :rtype: Tracker
        """

        return self.container._fire_event("gambit_send", self._pn_object, message)

    def try_send(self, message, on_delivery=None):
        """
        Send a message without blocking for credit.

        If there is credit, the message is sent and a tracker is
        returned.  Use :meth:`Tracker.await_delivery()` to block until
        the remote peer acknowledges the message.

        If no credit is available, the method immediately returns
        `None`.

        If set, `on_delivery(tracker)` is called after the delivery is acknowledged.
        It is called on another thread, not the main API thread.
        Users must take care to use thread-safe code in the callback.

        :rtype: Tracker
        """

        raise NotImplementedError()

class Receiver(_Link):
    """
    A receiver is an iterable object.
    Each item returned during iteration is a delivery obtained by calling `receive()`.
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

    def try_receive(self):
        """
        Receive a delivery containing a message if one is already
        available.  Otherwise, return `None`.

        :rtype: Delivery
        """

        raise NotImplementedError()

    def __iter__(self):
        return _ReceiverIterator(self)

class _ReceiverIterator(object):
    def __init__(self, receiver):
        self._receiver = receiver

    def next(self):
        return self._receiver.receive()

class _Terminus(_Object):
    def _get_address(self):
        """
        The source or target address.
        """
        return self._pn_object.address

    def _set_address(self, address):
        self._pn_object.address = address

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

    @property
    def settled(self):
        """
        True if the transfer is settled.
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

        return self._pn_object.remote_state

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
            self.container._pn_object.run()
        except KeyboardInterrupt:
            raise

    def stop(self):
        self.container._log("Stopping the worker thread")

        self.container._pn_object.stop()

class _Handler(_handlers.MessagingHandler):
    def __init__(self, container):
        super(_Handler, self).__init__()

        self.container = container

        # XXX
        self.pending_futures = dict()

        # (operation class, proton endpoint) => operation
        self.pending_operations = dict()
        # proton delivery => (proton_message, on_delivery)
        self.pending_deliveries = dict()

    def on_gambit_connect(self, event):
        future, conn_url = event.subject
        self.container._pn_object.connect(conn_url, allowed_mechs="ANONYMOUS").__future = future

    def on_connection_opened(self, event):
        future = event.connection.__future
        conn = Connection(self.container, event.connection, None)
        future.get_loop().call_soon_threadsafe(lambda: future.set_result(conn))

    # def on_gambit_open_session(self, event):
    #     print(333)
    #     pn_conn, future = event.subject
    #     pn_conn.session().__future = future
    #     print(444)
    #
    # def on_session_opened(self, event):
    #     print(222)
    #     future = event.session.__future
    #     session = Session(self.container, event.session, None)
    #     future.get_loop().call_soon_threadsafe(lambda: future.set_result(session))

    def on_gambit_open_sender(self, event):
        future, pn_conn, address = event.subject
        self.container._pn_object.create_sender(pn_conn, address).__future = future

    def on_gambit_open_receiver(self, event):
        future, pn_conn, address = event.subject
        dynamic = address is None
        self.container._pn_object.create_receiver(pn_conn, address, dynamic=dynamic).__future = future

    def on_link_opened(self, event):
        if event.link.is_sender:
            future = event.link.__future
            sender = Sender(self.container, event.link, None)
            future.get_loop().call_soon_threadsafe(lambda: future.set_result(sender))

        if event.link.is_receiver:
            future = event.link.__future
            receiver = Receiver(self.container, event.link, None)
            future.get_loop().call_soon_threadsafe(lambda: future.set_result(receiver))

    def on_gambit_close_endpoint(self, event):
        future, pn_endpoint = event.subject
        pn_endpoint.close()
        pn_endpoint.__future = future

    def on_connection_closed(self, event):
        future = event.connection.__future
        future.get_loop().call_soon_threadsafe(lambda: future.set_result(None))

    def on_session_closed(self, event):
        future = event.session.__future
        future.get_loop().call_soon_threadsafe(lambda: future.set_result(None))

    def on_link_closed(self, event):
        future = event.link.__future
        future.get_loop().call_soon_threadsafe(lambda: future.set_result(None))

    def on_gambit_send(self, event):
        future, pn_sender, message = event.subject
        pn_sender.send(message).__future = future

    def on_acknowledged(self, event):
        future = event.delivery.__future
        tracker = Tracker(self.container, event.delivery, event.message)
        future.get_loop().call_soon_threadsafe(lambda: future.set_result(tracker))

    def on_accepted(self, event):
        self.on_acknowledged(event)

    def on_rejected(self, event):
        self.on_acknowledged(event)

    def on_released(self, event):
        self.on_acknowledged(event)

    # def on_sendable(self, event):
    #     self.send_messages(event.sender)

    # def on_message_enqueued(self, event):
    #     self.send_messages(event.subject)

    # def send_messages(self, sender):
    #     gb_sender = self.container._endpoints[sender]
    #     queue = gb_sender._message_queue
    #     port = gb_sender._tracker_port

    #     while sender.credit > 0 and not queue.empty():
    #         message, on_delivery = queue.get()

    #         delivery = sender.send(message)
    #         tracker = Tracker(self.container, delivery, message)

    #         port.put(tracker)

    #         self.pending_deliveries[delivery] = (tracker, on_delivery)

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
