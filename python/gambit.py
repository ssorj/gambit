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

class Client:
    def __init__(self, id=None):
        self._pn_container = _reactor.Container(_Handler(self))

        if id is not None:
            self._pn_container.container_id = id

        self._operations = _queue.Queue()
        self._worker_thread = _WorkerThread(self)
        self._lock = _threading.Lock()

        self._event_injector = _reactor.EventInjector()
        self._pn_container.selectable(self._event_injector)

        # proton endpoint => gambit endpoint
        self._endpoints = dict()

        self._connections = set()

    async def __aenter__(self):
        _threading.current_thread().name = "user"

        with self._lock:
            self._worker_thread.start()

        return self

    async def __aexit__(self, exc_type, exc_value, traceback):
        await self.stop()

    async def stop(self, timeout=None):
        """
        Close any open connections and stop the container.  Blocks until all connections are closed.
        """

        # XXX Implement timeout

        # Use asyncio.wait? XXX
        for conn in self._connections:
            await conn.close()

        with self._lock:
            self._worker_thread.stop()

    @property
    def id(self):
        """
        The unique identity of the container.
        """
        return self._pn_container.container_id

    def _fire_event(self, event_name, *args):
        future = _asyncio.get_event_loop().create_future()
        event = _reactor.ApplicationEvent(event_name, subject=(future, *args))

        self._event_injector.trigger(event)

        return future

    async def connect(self, conn_url, **options):
        """
        Initiate connection open.
        Use :meth:`Connection.await_open()` to block until the remote peer confirms the open.

        :rtype: Connection
        """

        self._log("Connecting to {}", conn_url)

        with self._lock:
            if not self._worker_thread.is_alive():
                self._worker_thread.start()

        return await self._fire_event("gambit_connect", conn_url)

    def _log(self, message, *args):
        with _log_mutex:
            message = message.format(*args)
            thread = _threading.current_thread()

            _sys.stdout.write("[{:.4}:{:.4}] {}\n".format(self.id, thread.name, message))
            _sys.stdout.flush()

class _Object:
    def __init__(self, client, pn_object):
        self.client = client
        self._pn_object = pn_object

    def __repr__(self):
        return "{}({})".format(self.__class__.__name__, self._pn_object)

class _Endpoint(_Object):
    def __init__(self, container, pn_object):
        super(_Endpoint, self).__init__(container, pn_object)

        self.client._endpoints[self._pn_object] = self

    async def __aenter__(self):
        return self

    async def __aexit__(self, exc_type, exc_value, traceback):
        self.close()

    async def close(self, error_condition=None):
        """
        Initiate close.
        """

        return await self.client._fire_event("gambit_close_endpoint", self._pn_object)

class Connection(_Endpoint):
    def __init__(self, container, pn_object):
        super(Connection, self).__init__(container, pn_object)

        self._default_sender = None

    async def open_session(self, **options):
        return await self.client._fire_event("gambit_open_session", self._pn_object)

    async def open_sender(self, address, **options):
        """
        Initiate sender open.
        XXX Use :meth:`Sender.await_open()` to block until the remote peer confirms the open.

        :rtype: XXX Sender
        """

        assert address is not None

        return await self.client._fire_event("gambit_open_sender", self._pn_object, address)

    async def open_receiver(self, address, on_message=None, **options):
        """
        Initiate receiver open.
        Use :meth:`Receiver.await_open()` to block until the remote peer confirms the open.

        If set, `on_message(delivery)` is called when a message is received.
        It is called on another thread, not the main API thread.
        Users must take care to use thread-safe code in the callback.

        :rtype: XXX Receiver
        """

        assert address is not None

        return await self.client._fire_event("gambit_open_receiver", self._pn_object, address)

    async def open_anonymous_sender(self, **options):
        """
        Initiate open of an unnamed sender.
        See :meth:`open_sender()`.

        :rtype: Sender
        """

        raise NotImplementedError()

    async def open_dynamic_receiver(self, timeout=None, **options):
        """
        Open a sender with a dynamic source address supplied by the remote peer.
        See :meth:`open_receiver()`.

        REVIEW: Unlike the other open methods, this one blocks until the remote peer
        confirms the open and supplies the source address.

        :rtype: Receiver
        """

        raise NotImplementedError()

    @property
    async def default_session(self):
        """
        The default session.
        """

        raise NotImplementedError()

class Session(_Endpoint):
    pass

class _Link(_Endpoint):
    def __init__(self, container, pn_object):
        super(_Link, self).__init__(container, pn_object)

        self._connection = self.client._endpoints[self._pn_object.connection]
        self._target = Target(self.client, self._pn_object.remote_target)
        self._source = Source(self.client, self._pn_object.remote_source)

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
    def __init__(self, container, pn_object):
        super(Sender, self).__init__(container, pn_object)

        self._connection = self.client._endpoints[self._pn_object.connection]
        self._target = Target(self.client, self._pn_object.remote_target)

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

        # await self.sendable()

        return self.client._fire_event("gambit_send", self._pn_object, message)

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

    def __init__(self, container, pn_object):
        super(Receiver, self).__init__(container, pn_object)

        self._delivery_queue = _queue.Queue()

    def receive(self, timeout=None):
        """
        Receive a delivery containing a message.  Blocks until a message is available.

        CONSIDER: Flow one credit if credit is zero for a more convenient no-prefetch mode.

        :rtype: Delivery
        """

        return self.client._fire_event("gambit_receive", self._pn_object)

        pn_delivery, pn_message = self._delivery_queue.get()
        return Delivery(self.client, pn_delivery, pn_message)

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
    def __init__(self, container, pn_object, message):
        super(_Transfer, self).__init__(container, pn_object)

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
    def __init__(self, container, pn_object, message):
        super(Tracker, self).__init__(container, pn_object, message)

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
    def __init__(self, client):
        _threading.Thread.__init__(self)

        self.client = client
        self.name = "worker"
        self.daemon = True

    def start(self):
        self.client._log("Starting the worker thread")


        _threading.Thread.start(self)

    def run(self):
        try:
            self.client._pn_container.run()
        except KeyboardInterrupt:
            raise

    def stop(self):
        self.client._log("Stopping the worker thread")

        self.client._pn_container.stop()

class _Handler(_handlers.MessagingHandler):
    def __init__(self, client):
        super(_Handler, self).__init__()

        self.client = client

        # proton delivery => (proton_message, on_delivery)
        self.pending_deliveries = dict()

    # Connection opening

    def on_gambit_connect(self, event):
        future, conn_url = event.subject
        self.client._pn_container.connect(conn_url, allowed_mechs="ANONYMOUS").__future = future

    def on_connection_opened(self, event):
        future = event.connection.__future
        conn = Connection(self.client, event.connection)
        future.get_loop().call_soon_threadsafe(lambda: future.set_result(conn))

    # Link opening

    def on_gambit_open_sender(self, event):
        future, pn_conn, address = event.subject
        self.client._pn_container.create_sender(pn_conn, address).__future = future

    def on_gambit_open_receiver(self, event):
        future, pn_conn, address = event.subject
        dynamic = address is None
        self.client._pn_container.create_receiver(pn_conn, address, dynamic=dynamic).__future = future

    def on_link_opened(self, event):
        if event.link.is_sender:
            future = event.link.__future
            sender = Sender(self.client, event.link)
            future.get_loop().call_soon_threadsafe(lambda: future.set_result(sender))

        if event.link.is_receiver:
            future = event.link.__future
            receiver = Receiver(self.client, event.link)
            future.get_loop().call_soon_threadsafe(lambda: future.set_result(receiver))

    # Endpoint closing

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

    # Sending

    def on_gambit_send(self, event):
        future, pn_sender, message = event.subject
        pn_sender.send(message).__future = future

    def on_acknowledged(self, event):
        future = event.delivery.__future
        tracker = Tracker(self.client, event.delivery, event.message)
        future.get_loop().call_soon_threadsafe(lambda: future.set_result(tracker))

    def on_accepted(self, event):
        self.on_acknowledged(event)

    def on_rejected(self, event):
        self.on_acknowledged(event)

    def on_released(self, event):
        self.on_acknowledged(event)

    # Receiving

    def on_gambit_receive(self, event):
        future, pn_receiver = event.subject
        pn_receiver.flow(1) # XXX no future

    # def on_sendable(self, event):
    #     self.send_messages(event.sender)

    # def on_message_enqueued(self, event):
    #     self.send_messages(event.subject)

    # def send_messages(self, sender):
    #     gb_sender = self.client._endpoints[sender]
    #     queue = gb_sender._message_queue
    #     port = gb_sender._tracker_port

    #     while sender.credit > 0 and not queue.empty():
    #         message, on_delivery = queue.get()

    #         delivery = sender.send(message)
    #         tracker = Tracker(self.client, delivery, message)

    #         port.put(tracker)

    #         self.pending_deliveries[delivery] = (tracker, on_delivery)

    def on_message(self, event):
        gb_receiver = self.client._endpoints[event.receiver]
        gb_receiver._delivery_queue.put((event.delivery, event.message))
