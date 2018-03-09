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

import sys
import threading
import time

from gambit import *

def send_once(host, port):
    message = Message("hello")

    with Container("send") as cont:
        conn = cont.connect(host, port)
        sender = conn.open_sender("examples")
        sender.send(message)

        cont.log("Sent message {}", message)

def receive_once(host, port):
    with Container("receive") as cont:
        conn = cont.connect(host, port)
        receiver = conn.open_receiver("examples")
        delivery = receiver.receive()

        cont.log("Received message {}", delivery.message)

def send_thrice(host, port):
    messages = [Message("hello-{}".format(x)) for x in range(3)]
    trackers = list()

    with Container("send") as cont:
        conn = cont.connect(host, port)
        sender = conn.open_sender("examples")

        for message in messages:
            sender.send(message, lambda x: trackers.append(x))

        for tracker in trackers:
            cont.log("Sent {} ({})", tracker.message, tracker.state)

def receive_thrice(host, port):
    with Container("receive") as cont:
        conn = cont.connect(host, port)
        receiver = conn.open_receiver("examples")

        for i in range(3):
            delivery = receiver.receive()
            cont.log("Received {}", delivery.message)

def send_indefinitely(host, port):
    message = Message()

    with Container("send") as cont:
        conn = cont.connect(host, port)
        sender = conn.open_sender("examples")

        def completion_fn(tracker):
            cont.log("Sent {} ({})", tracker.message, tracker.state)

        for i in range(0xffff):
            message.body = "message-{}".format(i)
            sender.send(message, completion_fn=completion_fn)

            time.sleep(0.2)

def receive_indefinitely(host, port):
    with Container("receive") as cont:
        conn = cont.connect(host, port)
        receiver = conn.open_receiver("examples")

        for delivery in receiver:
            cont.log("Received {}", delivery.message)

def request_once(host, port):
    with Container("request") as cont:
        conn = cont.connect(host, port)
        sender = conn.open_sender("requests")
        receiver = conn.open_dynamic_receiver()

        request = Message("abc")
        request.reply_to = receiver.source.address

        sender.send(request)

        delivery = receiver.receive()

        cont.log("Sent {} and received {}", request, delivery.message)

def respond_once(host, port):
    with Container("respond") as cont:
        conn = cont.connect(host, port)
        receiver = conn.open_receiver("requests")

        delivery = receiver.receive()

        response = Message(delivery.message.body.upper())
        response.to = delivery.message.reply_to

        conn.send(response)

        cont.log("Processed {} and sent {}", delivery.message, response)

def main():
    try:
        host, port = sys.argv[1:3]
        port = int(port)
    except:
        sys.exit("Usage: demo HOST PORT")

    # Send and receive once

    send_once(host, port)
    receive_once(host, port)

    # Send and receive three times

    send_thrice(host, port)
    receive_thrice(host, port)

    # Request once and respond once

    respond_thread = threading.Thread(target=respond_once, args=(host, port))
    respond_thread.start()

    request_once(host, port)

    respond_thread.join()

    # Send and receive indefinitely

    send_thread = threading.Thread(target=send_indefinitely, args=(host, port))
    send_thread.daemon = True
    send_thread.start()

    receive_thread = threading.Thread(target=receive_indefinitely, args=(host, port))
    receive_thread.daemon = True
    receive_thread.start()

    time.sleep(1)

if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        pass
