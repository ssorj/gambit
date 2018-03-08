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

        self._worker_thread = _WorkerThread(self)

    def __enter__(self):
        _threading.current_thread().name = "user"

        self.log("Starting IO thread")

        self._worker_thread.start()

        return self

    def __exit__(self, exc_type, exc_value, traceback):
        self._worker_thread.stop()

    @property
    def id(self):
        return self._proton_object.container_id

    def connect(self, host, port):
        self.log("Connecting to {}:{}", host, port)

        op = _ConnectOperation(self, host, port)
        return op.enqueue()

    def log(self, message, *args):
        with _log_mutex:
            message = message.format(*args)
            thread = _threading.current_thread()

            _sys.stdout.write("[{:.4}:{:.4}] {}\n".format(self.id, thread.name, message))
            _sys.stdout.flush()

class _Sequence(object):
    def __init__(self):
        self.value = 0

    def next(self):
        self.value += 1
        return self.value

class _Object(object):
    def __init__(self, operation):
        self._operation = operation
        self._container = operation.container
        self._proton_object = operation.proton_object

        self._completed = _threading.Event()

    def __repr__(self):
        return "{}({})".format(self.__class__.__name__, self._proton_object)

    def _set_completed(self):
        self._completed.set()
        self._container.log("Completed {}", self)

    def wait(self, timeout=None):
        completed = self._completed.wait(timeout)

        if not completed:
            raise Exception("Timed out waiting for {}".format(self))

        return self

class _Endpoint(_Object):
    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        self._proton_object.close()

class Connection(_Endpoint):
    def open_sender(self, address=None):
        op = _OpenSenderOperation(self, address)
        return op.enqueue()

    def open_receiver(self, address=None):
        op = _OpenReceiverOperation(self, address)
        return op.enqueue()

class Sender(_Endpoint):
    def send(self, message):
        op = _SendOperation(self, message)
        return op.enqueue()

class Receiver(_Endpoint):
    def __init__(self, operation):
        super(Receiver, self).__init__(operation)

        self.source = Source(self._proton_object.remote_source)

    def receive(self, count=1):
        op = _ReceiveOperation(self, count)
        deliveries = op.enqueue()

        if count == 1:
            return deliveries[0]

        return deliveries

class Source(object):
    def __init__(self, proton_object):
        self._proton_object = proton_object

    def _get_address(self):
        return self._proton_object.address

    def _set_address(self, address):
        self._proton_object.address = address

    address = property(_get_address, _set_address)

_delivery_ids = _Sequence()

class Delivery(_Object):
    def __init__(self, operation):
        super(Delivery, self).__init__(operation)

        self.id = _delivery_ids.next()

        self.message = None

    def __repr__(self):
        return "{}({}, {})".format(self.__class__.__name__, self.id, self.message)

class Tracker(_Object):
    @property
    def state(self):
        return self._proton_object.remote_state

class Message(object):
    def __init__(self, body=None):
        self._proton_object = _proton.Message()

        if body is not None:
            self.body = body

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

        self.operations = _collections.deque()
        self.events = _reactor.EventInjector()
        self.container._proton_object.selectable(self.events)

        self.name = "worker"
        self.daemon = True

    def run(self):
        try:
            self.container._proton_object.run()
        except KeyboardInterrupt:
            raise

    def stop(self):
        self.container._proton_object.stop()

class _Handler(_handlers.MessagingHandler):
    def __init__(self, container):
        super(_Handler, self).__init__(prefetch=0)

        self.container = container

        self.pending_operations = dict()
        self.pending_deliveries = _collections.defaultdict(_collections.deque)

    def on_operation(self, event):
        op = self.container._worker_thread.operations.pop()
        op.begin()

        if type(op) is _ReceiveOperation:
            deliveries = self.pending_deliveries[op.proton_object]

            for delivery in op.gambit_object:
                deliveries.appendleft(delivery)
        else:
            self.pending_operations[op.proton_object] = op

    def on_connection_opened(self, event):
        op = self.pending_operations.pop(event.connection)
        op.gambit_object._set_completed()

    def on_link_opened(self, event):
        op = self.pending_operations.pop(event.link)
        op.gambit_object._set_completed()

    def on_accepted(self, event):
        op = self.pending_operations.pop(event.delivery)
        op.gambit_object._set_completed()

    def on_rejected(self, event):
        self.on_accepted(event)

    def on_released(self, event):
        self.on_accepted(event)

    def on_message(self, event):
        delivery = self.pending_deliveries[event.receiver].pop()
        delivery.message = event.message

        delivery._set_completed()

class _Operation(object):
    def __init__(self, container):
        self.container = container

        self.proton_object = None
        self.gambit_object = None

        self.begun = _threading.Event()

    def __repr__(self):
        return self.__class__.__name__

    def enqueue(self):
        self.container.log("Enqueueing {}", self)

        self.container._worker_thread.operations.appendleft(self)
        self.container._worker_thread.events.trigger(_reactor.ApplicationEvent("operation"))

        while not self.begun.wait(1):
            pass

        return self.gambit_object

    def begin(self):
        self.container.log("Beginning {}", self)

        self._begin()

        assert self.proton_object is not None
        assert self.gambit_object is not None

        self.begun.set()

    def wait(self, timeout=None):
        self.container.log("Waiting for completion of {}", self)
        self.completed.wait(timeout)

class _ConnectOperation(_Operation):
    def __init__(self, container, host, port):
        super(_ConnectOperation, self).__init__(container)

        self.host = host
        self.port = port

    def _begin(self):
        pn_cont = self.container._proton_object
        conn_url = "amqp://{}:{}".format(self.host, self.port)

        self.proton_object = pn_cont.connect(conn_url, allowed_mechs=b"ANONYMOUS")
        self.gambit_object = Connection(self)

class _OpenSenderOperation(_Operation):
    def __init__(self, connection, address):
        super(_OpenSenderOperation, self).__init__(connection._container)

        self.connection = connection
        self.address = address

    def _begin(self):
        pn_cont = self.container._proton_object
        pn_conn = self.connection._proton_object

        self.proton_object = pn_cont.create_sender(pn_conn, self.address)
        self.gambit_object = Sender(self)

class _OpenReceiverOperation(_Operation):
    def __init__(self, connection, address):
        super(_OpenReceiverOperation, self).__init__(connection._container)

        self.connection = connection
        self.address = address

    def _begin(self):
        pn_cont = self.container._proton_object
        pn_conn = self.connection._proton_object

        dynamic = False

        if self.address is None:
            dynamic = True

        self.proton_object = pn_cont.create_receiver(pn_conn, self.address, dynamic=dynamic)
        self.gambit_object = Receiver(self)

class _SendOperation(_Operation):
    def __init__(self, sender, message):
        super(_SendOperation, self).__init__(sender._container)

        self.sender = sender
        self.message = message

    def _begin(self):
        pn_snd = self.sender._proton_object
        pn_msg = self.message._proton_object

        # XXX Need to block on credit

        self.proton_object = pn_snd.send(pn_msg)
        self.gambit_object = Tracker(self)

class _ReceiveOperation(_Operation):
    def __init__(self, receiver, count):
        super(_ReceiveOperation, self).__init__(receiver._container)

        self.receiver = receiver
        self.count = count

    def _begin(self):
        pn_rcv = self.receiver._proton_object
        pn_rcv.flow(self.count)

        self.proton_object = pn_rcv
        self.gambit_object = list()

        for i in range(self.count):
            delivery = Delivery(self)
            self.gambit_object.append(delivery)
