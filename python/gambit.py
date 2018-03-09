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
        self._receiver_queues = _collections.defaultdict(_queue.Queue)

        self._proton_object.selectable(self._event_injector)

        self._connections = set()

    def __enter__(self):
        _threading.current_thread().name = "user"

        self._worker_thread.start()

        return self

    def __exit__(self, exc_type, exc_value, traceback):
        for conn in self._connections:
            conn.close()

        for conn in self._connections:
            conn.wait_for_close()

        self._worker_thread.stop()

    @property
    def id(self):
        return self._proton_object.container_id

    def connect(self, host, port):
        self.log("Connecting to {}:{}", host, port)

        op = _ConnectionOpen(self, host, port)
        op.enqueue()
        op.wait_for_start()

        self._connections.add(op.gambit_object)

        return op.gambit_object

    def log(self, message, *args):
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

class _Endpoint(_Object):
    def __init__(self, container, proton_object, open_operation):
        super(_Endpoint, self).__init__(container, proton_object)

        self._open_operation = open_operation
        self._close_operation = _EndpointClose(self.container, self)

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        self.close()

    def close(self):
        self._close_operation.enqueue()

    def wait_for_open(self):
        self._open_operation.wait_for_completion()

    def wait_for_close(self):
        self._close_operation.wait_for_completion()

class Connection(_Endpoint):
    def open_sender(self, address=None):
        op = _SenderOpen(self.container, self, address)
        op.enqueue()
        op.wait_for_start()

        return op.gambit_object

    def open_receiver(self, address=None):
        op = _ReceiverOpen(self.container, self, address)
        op.enqueue()
        op.wait_for_start()

        return op.gambit_object

class Sender(_Endpoint):
    def send(self, message, completion_fn=None):
        op = _MessageSend(self.container, self, message, completion_fn)
        op.enqueue()
        op.wait_for_start()

        return op.gambit_object

class Receiver(_Endpoint):
    def __init__(self, container, proton_object, open_operation):
        super(Receiver, self).__init__(container, proton_object, open_operation)

        self.source = Source(self._proton_object.remote_source)
        self._queue = self.container._receiver_queues[self._proton_object]

    # XXX Timeout arg with magic value for no wait? or try_receive
    # XXX Advance credit by one if credit is zero?
    def receive(self):
        self.container.log("Receiving from {}", self)

        pn_delivery, pn_message = self._queue.get()

        return Delivery(pn_delivery, Message(_proton_object=pn_message))

    def next(self):
        return self.receive()

    def __iter__(self):
        return self

class Source(object):
    def __init__(self, proton_object):
        self._proton_object = proton_object

    def _get_address(self):
        return self._proton_object.address

    def _set_address(self, address):
        self._proton_object.address = address

    address = property(_get_address, _set_address)

class Tracker(_Object):
    def __init__(self, container, proton_object, send_operation):
        super(Tracker, self).__init__(container, proton_object)

        self._send_operation = send_operation

    @property
    def state(self):
        return self._proton_object.remote_state

    def wait_for_update(self):
        return self._send_operation.wait_for_completion()

class Delivery(object):
    def __init__(self, proton_object, message):
        self._proton_object = proton_object
        self.message = message

    def __repr__(self):
        return "{}({}, {})".format(self.__class__.__name__, self._proton_object, self.message)

class Message(object):
    def __init__(self, body=None, _proton_object=None):
        self._proton_object = _proton_object

        if _proton_object is None:
            self._proton_object = _proton.Message()

        if body is not None:
            self.body = body

    def __repr__(self):
        return "{}({})".format(self.__class__.__name__, self._proton_object)

    def _get_to(self):
        return self._proton_object.address

    def _set_to(self, address):
        self._proton_object.address = address

    to = property(_get_to, _set_to)

    def _get_reply_to(self):
        return self._proton_object.reply_to

    def _set_reply_to(self, address):
        self._proton_object.reply_to = address

    reply_to = property(_get_reply_to, _set_reply_to)

    def _get_body(self):
        return self._proton_object.body

    def _set_body(self, body):
        self._proton_object.body = body

    body = property(_get_body, _set_body)

class _WorkerThread(_threading.Thread):
    def __init__(self, container):
        _threading.Thread.__init__(self)

        self.container = container
        self.name = "worker"

    def start(self):
        self.container.log("Starting the worker thread")

        _threading.Thread.start(self)

    def run(self):
        try:
            self.container._proton_object.run()
        except KeyboardInterrupt:
            raise

    def stop(self):
        self.container.log("Stopping the worker thread")

        self.container._proton_object.stop()

        # XXX Why is this slow?
        self.join()

class _Handler(_handlers.MessagingHandler):
    def __init__(self, container):
        super(_Handler, self).__init__()

        self.container = container
        self.pending_operations = dict()

    def on_operation(self, event):
        op = self.container._operations.get()
        op.start()

        # Keyed by operation class and proton object
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
        op = self.pending_operations.pop((_MessageSend, event.delivery))
        op.complete()

    def on_accepted(self, event):
        self.on_acknowledged(event)

    def on_rejected(self, event):
        self.on_acknowledged(event)

    def on_released(self, event):
        self.on_acknowledged(event)

    def on_message(self, event):
        self.container.log("Received message {}", event.message)

        queue = self.container._receiver_queues[event.receiver]
        queue.put((event.delivery, event.message))

class _Operation(object):
    def __init__(self, container):
        self.container = container

        self.proton_object = None
        self.gambit_object = None

        self.started = _threading.Event()
        self.completed = _threading.Event()

    def __repr__(self):
        return self.__class__.__name__

    def enqueue(self):
        self.container.log("Enqueueing {}", self)

        self.container._operations.put(self)
        self.container._event_injector.trigger(_reactor.ApplicationEvent("operation"))

    def start(self):
        self.container.log("Starting {}", self)

        self.on_start()

        assert self.proton_object is not None
        assert self.gambit_object is not None

        self.started.set()

    def on_start(self):
        raise NotImplementedError()

    def wait_for_start(self):
        self.container.log("Waiting for start of {}", self)

        while not self.started.wait(1):
            pass

    def complete(self):
        self.container.log("Completing {}", self)

        self.on_completion()
        self.completed.set()

    def on_completion(self):
        pass

    def wait_for_completion(self):
        self.container.log("Waiting for completion of {}", self)

        while not self.completed.wait(1):
            pass

class _EndpointClose(_Operation):
    def __init__(self, container, endpoint):
        super(_EndpointClose, self).__init__(container)

        self.endpoint = endpoint

    def on_start(self):
        self.endpoint._proton_object.close()

        self.proton_object = self.endpoint._proton_object
        self.gambit_object = self.endpoint

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

        self.proton_object = pn_container.create_receiver(pn_connection, self.address,
                                                          dynamic=dynamic)
        self.gambit_object = Receiver(self.container, self.proton_object, self)

class _MessageSend(_Operation):
    def __init__(self, container, sender, message, completion_fn):
        super(_MessageSend, self).__init__(container)

        self.sender = sender
        self.message = message
        self.completion_fn = completion_fn

    def on_start(self):
        pn_sender = self.sender._proton_object
        pn_message = self.message._proton_object

        # XXX Need to block on credit

        self.proton_object = pn_sender.send(pn_message)
        self.gambit_object = Tracker(self.container, self.proton_object, self)

    def on_completion(self):
        if self.completion_fn is not None:
            self.completion_fn(self.gambit_object)
