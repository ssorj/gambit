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

import queue as _queue
import sys as _sys
import threading as _threading
import uuid as _uuid

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

        self._injector = _reactor.EventInjector()
        self._pn_container.selectable(self._injector)

        self._connections = set()

    def __enter__(self):
        _threading.current_thread().name = "user"

        with self._lock:
            self._worker_thread.start()

        return self

    def __exit__(self, exc_type, exc_value, traceback):
        self.stop()

    def stop(self):
        """
        Close any open connections and stop the container.  Blocks until all connections are closed.
        """

        waitables = list()

        for conn in self._connections:
            waitables.append(conn.close())

        for waitable in waitables:
            waitable.wait()

        with self._lock:
            self._worker_thread.stop()

    @property
    def id(self):
        """
        The unique identity of the container.
        """
        return self._pn_container.container_id

    def _send_event(self, event_name, *args):
        if len(args) == 1:
            args = args[0]

        event = _reactor.ApplicationEvent(event_name, subject=args)
        self._injector.trigger(event)

    def _call(self, event_name, *args):
        future = _Future(self._lock)
        self._send_event(event_name, future, *args)
        return future.result()

    def _call_async(self, event_name, *args):
        future = _Future(self._lock)
        self._send_event(event_name, future, *args)
        return future

    def connect(self, conn_url, **options):
        """
        Initiate connection open.

        :rtype: Connection
        """

        self._log("Connecting to {}", conn_url)

        with self._lock:
            if not self._worker_thread.is_alive():
                self._worker_thread.start()

        return self._call("gb_connect", conn_url)

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
    def __init__(self, client, pn_object):
        super().__init__(client, pn_object)

        self._opened = _threading.Event()
        self._closed = _threading.Event()

    def wait(self):
        """
        Wait for the remote peer to confirm the open operation.

        :rtype: Endpoint
        """
        self._opened.wait()
        return self

    def close(self, error_condition=None):
        """
        Close the endpoint.
        """

        if self._closed.is_set():
            return

        self.client._send_event("gb_close_endpoint", self._pn_object)

        return self._closed

class Connection(_Endpoint):
    def __init__(self, client, pn_object):
        super().__init__(client, pn_object)

        self._default_session = None
        self._default_sender = None

        self.client._connections.add(self)

    def open_session(self, **options):
        """
        Initiate session open.

        :rtype: Session
        """
        return self.client._call("gb_open_session", self._pn_object)

    @property
    def default_session(self):
        """
        The default session.
        """

        if self._default_session is None:
            self._default_session = self.open_session()

        return self._default_session

    def open_sender(self, address, **options):
        """
        Initiate sender open.

        :rtype: Sender
        """

        return self.default_session.open_sender(address, **options)

    def open_receiver(self, address, **options):
        """
        Initiate receiver open.

        :rtype: Receiver
        """

        return self.default_session.open_receiver(address, **options)

    def open_anonymous_sender(self, **options):
        """
        Initiate open of a sender with no target address.
        See :meth:`open_sender()`.

        :rtype: Sender
        """

        return self.default_session.open_anonymous_sender(**options)

    def open_dynamic_receiver(self, **options):
        """
        Open a sender with a dynamic source address supplied by the remote peer.

        See :meth:`open_receiver()`.

        :rtype: Future<Receiver>
        """

        return self.default_session.open_dynamic_receiver(**options)

    def send(self, message):
        """
        Send a message using the default session and default sender.
        The message 'to' field must be set.

        :rtype: Future<Tracker>
        """
        return self.default_session.send(message)

class Session(_Endpoint):
    def __init__(self, client, pn_object):
        super().__init__(client, pn_object)

        self._default_sender = None

    @property
    def connection(self):
        """
        The connection containing this sender.
        """

        return self._pn_object.connection._gb_object

    @property
    def default_sender(self):
        """
        The default sender.  The default is an anonymous sender with
        no options set.
        """

        if self._default_sender is None:
            self._default_sender = self.open_anonymous_sender()

        return self._default_sender

    def open_sender(self, address, **options):
        """
        Initiate sender open.

        :rtype: Sender
        """

        return self.client._call("gb_open_sender", self._pn_object, address, options)

    def open_receiver(self, address, **options):
        """
        Initiate receiver open.

        :rtype: Receiver
        """

        return self.client._call("gb_open_receiver", self._pn_object, address, options)

    def open_anonymous_sender(self, **options):
        """
        Initiate open of a sender with no target address.

        See :meth:`open_sender()`.

        :rtype: Sender
        """

        return self.open_sender(None, **options)

    def open_dynamic_receiver(self, **options):
        """
        Open a sender with a dynamic source address supplied by the remote peer.
        See :meth:`open_receiver()`.

        :rtype: Future<Receiver>
        """

        return self.open_receiver(None, **options).wait()

    def send(self, message):
        """
        Send a message using the default sender.
        The message 'to' field must be set.

        :rtype: Future<Tracker>
        """

        if message.to is None:
            raise Error("Message 'to' address not set")

        return self.default_sender.send(message)

class _Link(_Endpoint):
    def __init__(self, client, pn_object):
        super().__init__(client, pn_object)

        self._target = Target(self.client, self._pn_object.remote_target)
        self._source = Source(self.client, self._pn_object.remote_source)

    @property
    def connection(self):
        """
        The connection containing this link.
        """

        return self._pn_object.connection._gb_object

    @property
    def session(self):
        """
        The session containing this link.
        """

        return self._pn_object.session._gb_object

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
    def __init__(self, client, pn_object):
        super().__init__(client, pn_object)

        self._sendable = _threading.Event()

    def send(self, message):
        """
        Send a message.

        Blocks until credit is available and the message can be sent.

        :rtype: Tracker
        """

        self._sendable.wait()
        self._sendable.clear()

        return self.client._call_async("gb_send", self._pn_object, message)

    def try_send(self, message):
        """
        Send a message without blocking for credit.

        If there is credit, the message is sent and a tracker is
        returned.  If no credit is available, the method immediately
        returns `None`.

        :rtype: Tracker
        """

        if not self._sendable.is_set():
            return

        return self.client._call_async("gb_send", self._pn_object, message)

class Receiver(_Link):
    """
    A receiver is an iterable object.
    Each item returned during iteration is a delivery obtained by calling `receive()`.
    """

    def __init__(self, client, pn_object):
        super().__init__(client, pn_object)

        self._deliveries = _queue.Queue()

    def receive(self):
        """
        Receive a delivery containing a message.  Blocks until a message is available.

        :rtype: Delivery
        """

        self.client._send_event("gb_receive", self._pn_object)

        return self._deliveries.get()

    def try_receive(self):
        """
        Receive a delivery containing a message if one is already
        available.  Otherwise, return `None`.

        :rtype: Delivery
        """

        try:
            return self._deliveries.get_nowait()
        except _queue.QueueEmpty:
            return

    def __iter__(self):
        return self

    def __next__(self):
        return self.receive()

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
        super().__init__(client, pn_object)

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
        super().__init__()

        self.client = client

    # Connection opening

    def on_gb_connect(self, event):
        future, conn_url = event.subject
        pn_conn = self.client._pn_container.connect(conn_url, allowed_mechs="ANONYMOUS")
        future.set_result(Connection(self.client, pn_conn))

    def on_connection_opened(self, event):
        event.connection._gb_object._opened.set()

    # Session opening

    def on_gb_open_session(self, event):
        future, pn_conn = event.subject

        pn_session = pn_conn.session()
        pn_session.open()

        future.set_result(Session(self.client, pn_session))

    def on_session_opened(self, event):
        event.session._gb_object._opened.set()

    # Link opening

    def on_gb_open_sender(self, event):
        future, pn_session, address, options = event.subject

        pn_sender = pn_session.sender(str(_uuid.uuid4()))
        pn_sender.target.address = address
        pn_sender.open()

        future.set_result(Sender(self.client, pn_sender))

    def on_gb_open_receiver(self, event):
        future, pn_session, address, options = event.subject

        pn_receiver = pn_session.receiver(str(_uuid.uuid4()))
        pn_receiver.source.address = address
        pn_receiver.source.dynamic = address is None
        pn_receiver.open()

        future.set_result(Receiver(self.client, pn_receiver))

    def on_link_opened(self, event):
        event.link._gb_object._opened.set()

    # Endpoint closing

    def on_gb_close_endpoint(self, event):
        pn_endpoint = event.subject
        pn_endpoint.close()

    def on_connection_closed(self, event):
        event.connection._gb_object._closed.set()

    def on_session_closed(self, event):
        event.session._gb_object._closed.set()

    def on_link_closed(self, event):
        event.link._gb_object._closed.set()

    # Sending

    def on_sendable(self, event):
        event.link._gb_object._sendable.set()

    def on_gb_send(self, event):
        future, pn_sender, message = event.subject
        pn_delivery = pn_sender.send(message)
        pn_delivery.__future = future
        pn_delivery.__message = message

    def on_acknowledged(self, event):
        event.delivery.__future.set_result(Tracker(self.client, event.delivery, event.delivery.__message))

    def on_accepted(self, event):
        self.on_acknowledged(event)

    def on_rejected(self, event):
        self.on_acknowledged(event)

    def on_released(self, event):
        self.on_acknowledged(event)

    # Receiving

    def on_gb_receive(self, event):
        pn_receiver = event.subject

        if pn_receiver.credit == 0:
            pn_receiver.flow(1)

    def on_message(self, event):
        receiver = event.receiver._gb_object
        delivery = Delivery(self.client, event.delivery, event.message)

        receiver._deliveries.put(delivery)

class _Future:
    def __init__(self, lock):
        self.value = None

        self.empty = _threading.Condition(lock)
        self.full = _threading.Condition(lock)

    def set_result(self, value):
        assert value is not None

        with self.empty:
            while self.value is not None:
                self.empty.wait()

            self.value = value
            self.full.notify()

    def result(self):
        with self.full:
            while self.value is None:
                self.full.wait()

            value = self.value
            self.value = None

            self.empty.notify()

            return value

    def wait(self):
        return self.result()
