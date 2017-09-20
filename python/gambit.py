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
        self._pno = _reactor.Container(_Handler(self), id=id)
        self._thread = _IoThread(self)

    def __enter__(self):
        print("Starting IO thread")

        self._thread.start()
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        self._pno.stop()
        self._thread.join()

    def connect(self, conn_url):
        print("Connecting to '{}'".format(conn_url))

        command = _ConnectCommand(self, conn_url)
        command._enqueue()

        return command

class _IoThread(_threading.Thread):
    def __init__(self, container):
        _threading.Thread.__init__(self)

        self.container = container

        self.commands = _collections.deque()
        self.events = _reactor.EventInjector()
        self.container._pno.selectable(self.events)

        self.daemon = True

    def run(self):
        try:
            self.container._pno.run()
        except KeyboardInterrupt:
            raise
        except:
            _traceback.print_exc()
            raise

    def stop(self):
        self.container._pno.stop()

class _Handler(_handlers.MessagingHandler):
    def __init__(self, container):
        self.container = container
        self.completions = {}

    def on_command(self, event):
        command = self.container._thread.commands.pop()

        print("Executing {}".format(command))

        key = command._open()
        self.completions[key] = command

    def on_connection_remote_open(self, event):
        self.completions[event.connection]._close(event.connection)

    def on_link_remote_open(self, event):
        if event.link.is_sender:
            self.completions[event.sender]._close(event.sender)

    def on_delivery(self, event):
        self.completions[event.delivery]._close(event.delivery)

class _Command(object):
    def __init__(self, container):
        self._container = container
        self._result = None
        self._event = _threading.Event()

    def __repr__(self):
        return self.__class__.__name__

    def _enqueue(self):
        print("Enqueueing command {}".format(self))

        self._container._thread.commands.appendleft(self)
        self._container._thread.events.trigger(_reactor.ApplicationEvent("command"))

    def _open(self):
        print("Opening {}".format(self))

        return self._do_open()

    def _do_open(self):
        raise NotImplementedError()

    def _close(self, pno):
        print("Closing {}".format(self))

        self._set(self._do_close(pno))

    def _do_close(self, pno):
        raise NotImplementedError()

    def _set(self, result):
        print("Setting result for {}".format(self))

        self._result = result
        self._event.set()

    def get(self):
        print("Waiting for result from {}".format(self))

        self._event.wait()
        return self._result

class _ConnectCommand(_Command):
    def __init__(self, container, connection_url):
        super(_ConnectCommand, self).__init__(container)

        self._connection_url = connection_url

    def _do_open(self):
        return self._container._pno.connect(self._connection_url, allowed_mechs=b"ANONYMOUS")

    def _do_close(self, pno):
        return Connection.wrap(self._container, pno)

class _OpenSenderCommand(_Command):
    def __init__(self, container, connection, address):
        super(_OpenSenderCommand, self).__init__(container)

        self._connection = connection
        self._address = address

    def _do_open(self):
        return self._container._pno.create_sender(self._connection._pno, self._address)

    def _do_close(self, pno):
        return Sender.wrap(self._container, pno)

class _SendCommand(_Command):
    def __init__(self, container, sender, message):
        super(_SendCommand, self).__init__(container)

        self._sender = sender
        self._message = message

    def _do_open(self):
        return self._sender._pno.send(self._message._pno)

    def _do_close(self, pno):
        return Delivery.wrap(self._container, pno)

class _Wrapper(object):
    def __init__(self, container, pno):
        self._container = container
        self._pno = pno

    def __repr__(self):
        return "{} ({})".format(self.__class__.__name__, self._pno)

class Endpoint(_Wrapper):
    def __init__(self, container, pno):
        super(Endpoint, self).__init__(container, pno)

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        self._pno.close()

class Connection(Endpoint):
    def __init__(self, container, pno):
        super(Connection, self).__init__(container, pno)

    @classmethod
    def wrap(cls, container, pno):
        return Connection(container, pno)

    def open_sender(self, address):
        command = _OpenSenderCommand(self._container, self, address)
        command._enqueue()

        return command

class Sender(Endpoint):
    def __init__(self, container, pno):
        super(Sender, self).__init__(container, pno)

    @classmethod
    def wrap(cls, container, pno):
        return Sender(container, pno)

    def send(self, message):
        command = _SendCommand(self._container, self, message)
        command._enqueue()

        return command

class Delivery(_Wrapper):
    def __init__(self, container, pno):
        super(Delivery, self).__init__(container, pno)

    @classmethod
    def wrap(cls, container, pno):
        return Delivery(container, pno)

    @property
    def state(self):
        return self._pno.remote_state

class Message(object):
    def __init__(self, body=None):
        self._pno = _proton.Message()

        if body is not None:
            self.body = body

    def _get_to(self):
        return self._pno.to

    def _set_to(self, address):
        self._pno.to = address

    to = property(_get_to, _set_to)

    def _get_body(self):
        return self._pno.body

    def _set_body(self, body):
        self._pno.body = body

    body = property(_get_body, _set_body)

def send():
    messages = [Message("message-{}".format(x)) for x in range(10)]

    with Container() as container:
        connection = container.connect("127.0.0.1").get()
        sender = connection.open_sender("examples").get()

        results = []

        for message in messages:
            results.append(sender.send(message))

        for result in results:
            print(result.get().state)

def main():
    send()

if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        pass
