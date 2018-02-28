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

import proton as _proton
import proton.handlers as _handlers
import proton.reactor as _reactor

import collections as _collections
import threading as _threading
import traceback as _traceback

class Container(object):
    def __init__(self, id=None):
        self._proton_object = _reactor.Container(_Handler(self), id=id)
        self._io_thread = _IoThread(self)

    def _notice(self, message, *args):
        message = message.format(*args)
        print("[api] {}".format(message))

    def __enter__(self):
        self._notice("Starting IO thread")

        self._io_thread.start()

        return self

    def __exit__(self, exc_type, exc_value, traceback):
        if exc_type is not None:
            _traceback.print_exception(exc_type, exc_value, traceback)

        self._io_thread.stop()

    def connect(self, conn_url):
        self._notice("Connecting to {}", conn_url)

        command = _ConnectCommand(self, conn_url)
        command._enqueue()

        return command

class _IoThread(_threading.Thread):
    def __init__(self, container):
        _threading.Thread.__init__(self)

        self.container = container

        self.commands = _collections.deque()
        self.events = _reactor.EventInjector()
        self.container._proton_object.selectable(self.events)

        self.name = "io"
        self.daemon = True

    def _notice(self, message, *args):
        message = message.format(*args)
        print("[io ] {}".format(message))

    def run(self):
        try:
            self.container._proton_object.run()
        except KeyboardInterrupt:
            raise
        except Exception as e:
            print(str(e))
            _traceback.print_exc()
            raise

    def stop(self):
        self.container._proton_object.stop()

class _Handler(_handlers.MessagingHandler):
    def __init__(self, container):
        super(_Handler, self).__init__(prefetch=0)

        self.container = container
        self.completions = dict()
        self.receive_completions = _collections.defaultdict(_collections.deque)

    def _notice(self, message, *args):
        self.container._io_thread._notice(message, *args)

    def on_command(self, event):
        command = self.container._io_thread.commands.pop()

        self._notice("Executing {}", command)

        command._begin()

        assert command._proton_object is not None

        if isinstance(command, _ReceiveCommand):
            self.receive_completions[command._proton_object].appendleft(command)
        else:
            self.completions[command._proton_object] = command

    def on_connection_remote_open(self, event):
        self.completions[event.connection]._complete(event.connection)

    def on_link_remote_open(self, event):
        if event.link.is_sender:
            self.completions[event.sender]._complete(event.sender)
        elif event.link.is_receiver:
            self.completions[event.receiver]._complete(event.receiver)

    def on_accepted(self, event):
        self.completions[event.delivery]._complete(event.delivery)

    def on_rejected(self, event):
        self.completions[event.delivery]._complete(event.delivery)

    def on_released(self, event):
        self.completions[event.delivery]._complete(event.delivery)

    def on_message(self, event):
        command = self.receive_completions[event.receiver].pop()
        command._complete(event.delivery, event.message)

class _Command(object):
    def __init__(self, container):
        self._container = container

        self._proton_object = None # Set after 'begun'
        self._result = None # Set after 'completed'

        self._begun = _threading.Event()
        self._completed = _threading.Event()

    def __repr__(self):
        return self.__class__.__name__

    def _enqueue(self):
        self._container._notice("Enqueueing {}", self)

        self._container._io_thread.commands.appendleft(self)
        self._container._io_thread.events.trigger(_reactor.ApplicationEvent("command"))

    def _begin(self):
        self._proton_object = self._create_proton_object()
        assert self._proton_object is not None

        self._begun.set()

    def _create_proton_object(self):
        raise NotImplementedError()

    def _complete(self, proton_object):
        self._result = self._wrap_result(proton_object)
        assert self._result is not None

        self._completed.set()

    def _wrap_result(self, proton_object):
        raise NotImplementedError()

    def result(self):
        self._container._notice("Waiting for result from {}", self)

        self._completed.wait(timeout=3) # XXX
        assert self._result is not None # XXX

        return self._result

class _ConnectCommand(_Command):
    def __init__(self, container, connection_url):
        super(_ConnectCommand, self).__init__(container)

        self._connection_url = connection_url

    def _create_proton_object(self):
        return self._container._proton_object.connect \
            (self._connection_url, allowed_mechs=b"ANONYMOUS")

    def _wrap_result(self, proton_object):
        return Connection(self._container, proton_object)

    def open_sender(self, address):
        command = _OpenSenderCommand(self._container, self, address)
        command._enqueue()

        return command

    def open_receiver(self, address):
        command = _OpenReceiverCommand(self._container, self, address)
        command._enqueue()

        return command

class _OpenSenderCommand(_Command):
    def __init__(self, container, connection, address):
        super(_OpenSenderCommand, self).__init__(container)

        self._connection = connection
        self._address = address

    def _create_proton_object(self):
        return self._container._proton_object.create_sender \
            (self._connection._proton_object, self._address)

    def _wrap_result(self, proton_object):
        return Sender(self._container, proton_object)

    def send(self, message):
        sender = self.result()
        return sender.send(message)

class _OpenReceiverCommand(_Command):
    def __init__(self, container, connection, address):
        super(_OpenReceiverCommand, self).__init__(container)

        self._connection = connection
        self._address = address

    def _create_proton_object(self):
        return self._container._proton_object.create_receiver \
            (self._connection._proton_object, self._address)

    def _wrap_result(self, proton_object):
        return Receiver(self._container, proton_object)

    def receive(self, count=1):
        receiver = self.result()
        return receiver.receive(count)

class _SendCommand(_Command):
    def __init__(self, container, sender, message):
        super(_SendCommand, self).__init__(container)

        self._sender = sender
        self._message = message

    def _create_proton_object(self):
        return self._sender._proton_object.send(self._message._proton_object)

    def _wrap_result(self, proton_object):
        return Tracker(self._container, proton_object)

class _ReceiveCommand(_Command):
    def __init__(self, container, receiver):
        super(_ReceiveCommand, self).__init__(container)

        self._receiver = receiver

    def _begin(self):
        self._receiver._proton_object.flow(1)
        self._proton_object = self._receiver._proton_object # XXX
        self._begun.set()

    def _complete(self, proton_object, message):
        self._result = Delivery(self._container, proton_object, message)
        self._completed.set()

class _Wrapper(object):
    def __init__(self, container, proton_object):
        self._container = container
        self._proton_object = proton_object

    def __repr__(self):
        return "{} ({})".format(self.__class__.__name__, self._proton_object)

class Endpoint(_Wrapper):
    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        self._proton_object.close()

class Connection(Endpoint):
    def open_sender(self, address):
        command = _OpenSenderCommand(self._container, self, address)
        command._enqueue()

        return command

    def open_receiver(self, address):
        command = _OpenReceiverCommand(self._container, self, address)
        command._enqueue()

        return command

class Sender(Endpoint):
    def send(self, message):
        command = _SendCommand(self._container, self, message)
        command._enqueue()

        return command

class Receiver(Endpoint):
    def receive(self, count=1):
        command = _ReceiveCommand(self._container, self)
        command._enqueue()

        return command

class Delivery(_Wrapper):
    def __init__(self, container, proton_object, message):
        super(Delivery, self).__init__(container, proton_object)

        self._message = message

    @property
    def message(self):
        return self._message

class Tracker(_Wrapper):
    @property
    def state(self):
        return self._proton_object.remote_state

class Message(object):
    def __init__(self, body=None):
        self._proton_object = _proton.Message()

        if body is not None:
            self.body = body

    def _get_to(self):
        return self._proton_object.to

    def _set_to(self, address):
        self._proton_object.to = address

    to = property(_get_to, _set_to)

    def _get_body(self):
        return self._proton_object.body

    def _set_body(self, body):
        self._proton_object.body = body

    body = property(_get_body, _set_body)
