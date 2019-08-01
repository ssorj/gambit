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
import sys as _sys
import threading as _threading

import proton as _proton
import proton.handlers as _handlers
import proton.reactor as _reactor

_log_mutex = _threading.Lock()

class Client:
    def __init__(self, id=None):
        self._pn_container = _reactor.Container(_MessagingHandler(self))

        if id is not None:
            self._pn_container.container_id = id

        self._worker_thread = _WorkerThread(self)
        self._lock = _threading.Lock()

        self._event_injector = _reactor.EventInjector()
        self._pn_container.selectable(self._event_injector)

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

        if self._connections and _asyncio.get_event_loop().is_running():
            done, pending = await _asyncio.wait([x.close() for x in self._connections], timeout=timeout)

            if pending:
                raise TimeoutError()

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

        return await self._fire_event("gb_connect", conn_url)

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

        pn_object._gb_object = self

    def __repr__(self):
        return "{}({})".format(self.__class__.__name__, self._pn_object)

class _Endpoint(_Object):
    async def close(self, error_condition=None):
        """
        Initiate close.
        """

        if self._pn_object.state & self._pn_object.LOCAL_CLOSED:
            return

        return await self.client._fire_event("gb_close_endpoint", self._pn_object)

class Connection(_Endpoint):
    def __init__(self, client, pn_object):
        super(Connection, self).__init__(client, pn_object)

        self.client._connections.add(self)

    async def open_session(self, **options):
        return await self.client._fire_event("gb_open_session", self._pn_object)

    async def open_sender(self, address, **options):
        """
        Initiate sender open.

        :rtype: Sender
        """

        assert address is not None

        return await self.client._fire_event("gb_open_sender", self._pn_object, address)

    async def open_receiver(self, address, on_message=None, **options):
        """
        Initiate receiver open.
        Use :meth:`Receiver.await_open()` to block until the remote peer confirms the open.

        If set, `on_message(delivery)` is called when a message is received.
        It is called on another thread, not the main API thread.
        Users must take care to use thread-safe code in the callback.

        :rtype: Receiver
        """

        assert address is not None

        return await self.client._fire_event("gb_open_receiver", self._pn_object, address)

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

        return self.open_receiver(None, timeout=timeout, **options)

    @property
    async def default_session(self):
        """
        The default session.
        """

        raise NotImplementedError()

class Session(_Endpoint):
    pass

class _Link(_Endpoint):
    def __init__(self, client, pn_object):
        super(_Link, self).__init__(client, pn_object)

        self._connection = self._pn_object.connection._gb_object
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
    async def send(self, message, timeout=None, on_delivery=None):
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

        return await self.client._fire_event("gb_send", self._pn_object, message)

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

    def __init__(self, client, pn_object, event_loop):
        super(Receiver, self).__init__(client, pn_object)

        self._deliveries = _asyncio.Queue(loop=event_loop)
        self._lock = _threading.Lock()

    async def receive(self, timeout=None):
        """
        Receive a delivery containing a message.  Blocks until a message is available.

        CONSIDER: Flow one credit if credit is zero for a more convenient no-prefetch mode.

        :rtype: Delivery
        """

        await self.client._fire_event("gb_receive", self._pn_object)

        return await self._deliveries.get()

    def try_receive(self):
        """
        Receive a delivery containing a message if one is already
        available.  Otherwise, return `None`.

        :rtype: Delivery
        """

        raise NotImplementedError()

    async def __aiter__(self):
        return self

    async def __anext__(self):
        return await self.receive()

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
    def __init__(self, client, pn_object, message):
        super(_Transfer, self).__init__(client, pn_object)

        self._message = message

    @property
    def message(self):
        """
        The message associated with this transfer.
        """

        return self._message

    @property
    def state(self):
        """
        The state of the transfer as reported by the remote peer.
        """

        return self._pn_object.remote_state

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
    @property
    def sender(self):
        """
        The sender containing this tracker.
        """

class Delivery(_Transfer):
    @property
    def receiver(self):
        """
        The receiver containing this delivery.
        """

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

class _MessagingHandler(_handlers.MessagingHandler):
    def __init__(self, client):
        super(_MessagingHandler, self).__init__()

        self.client = client

        # proton delivery => (proton_message, on_delivery)
        self.pending_deliveries = dict()

    # Connection opening

    def on_gb_connect(self, event):
        future, conn_url = event.subject
        self.client._pn_container.connect(conn_url, allowed_mechs="ANONYMOUS").__future = future

    def on_connection_opened(self, event):
        _set_result(event.connection.__future, Connection(self.client, event.connection))

    # Link opening

    def on_gb_open_sender(self, event):
        future, pn_conn, address = event.subject
        self.client._pn_container.create_sender(pn_conn, address).__future = future

    def on_gb_open_receiver(self, event):
        future, pn_conn, address = event.subject
        dynamic = address is None
        self.client._pn_container.create_receiver(pn_conn, address, dynamic=dynamic).__future = future

    def on_link_opened(self, event):
        if event.link.is_sender:
            _set_result(event.link.__future, Sender(self.client, event.link))

        if event.link.is_receiver:
            future = event.link.__future
            _set_result(future, Receiver(self.client, event.link, future._loop))

    # Endpoint closing

    def on_gb_close_endpoint(self, event):
        future, pn_endpoint = event.subject
        pn_endpoint.close()
        pn_endpoint.__future = future

    def on_connection_closed(self, event):
        _set_result(event.connection.__future, None)

    def on_session_closed(self, event):
        _set_result(event.session.__future, None)

    def on_link_closed(self, event):
        _set_result(event.link.__future, None)

    # Sending

    def on_gb_send(self, event):
        future, pn_sender, message = event.subject
        pn_delivery = pn_sender.send(message)
        pn_delivery.__future = future
        pn_delivery.__message = message

    def on_acknowledged(self, event):
        future = event.delivery.__future
        tracker = Tracker(self.client, event.delivery, event.delivery.__message)
        _set_result(future, tracker)

    def on_accepted(self, event):
        self.on_acknowledged(event)

    def on_rejected(self, event):
        self.on_acknowledged(event)

    def on_released(self, event):
        self.on_acknowledged(event)

    # Receiving

    def on_gb_receive(self, event):
        future, pn_receiver = event.subject

        if pn_receiver.credit == 0:
            pn_receiver.flow(1)

        _set_result(future, None)

    def on_message(self, event):
        receiver = event.receiver._gb_object
        delivery = Delivery(self.client, event.delivery, event.message)

        with receiver._lock:
            receiver._deliveries.put_nowait(delivery)

def _set_result(future, result):
    future._loop.call_soon_threadsafe(future.set_result, result)
