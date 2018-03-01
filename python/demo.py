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

def send_one(host, port):
    message = Message("hello")

    with Container("send") as cont:
        conn = cont.connect(host, port)
        sender = conn.open_sender("examples")

        tracker = sender.send(message)
        tracker.wait()

        cont.log("SEND: {}", tracker.state)

def receive_one(host, port):
    with Container("receive") as cont:
        conn = cont.connect(host, port)
        receiver = conn.open_receiver("examples")

        delivery = receiver.receive()
        delivery.wait()

        cont.log("RECEIVE: {}", delivery.message)

def send_three(host, port):
    messages = [Message("hello-{}".format(x)) for x in range(3)]
    trackers = list()

    with Container("send") as cont:
        conn = cont.connect(host, port)
        sender = conn.open_sender("examples")

        for message in messages:
            tracker = sender.send(message)
            trackers.append(tracker)

        for tracker in trackers:
            cont.log("SEND: {}", tracker.wait().state)

def receive_three(host, port):
    with Container("receive") as cont:
        conn = cont.connect(host, port)
        receiver = conn.open_receiver("examples")
        deliveries = receiver.receive(3)

        for delivery in deliveries:
            cont.log("RECEIVE: {}", delivery.wait().message)

def send_indefinitely(host, port):
    with Container("send") as cont:
        conn = cont.connect(host, port)
        sender = conn.open_sender("examples")

        for i in range(0xffff):
            trackers = list()

            for j in range(100):
                message = Message("message-{}-{}".format(i, j))
                tracker = sender.send(message)
                trackers.append(tracker)

                time.sleep(0.2) # Artificially slow this down

            for tracker in trackers:
                cont.log("SEND: {}", tracker.wait().state)

def receive_indefinitely(host, port):
    with Container("receive") as cont:
        conn = cont.connect(host, port)
        receiver = conn.open_receiver("examples")

        while True:
            deliveries = receiver.receive(100)

            for delivery in deliveries:
                cont.log("RECEIVE: {}", delivery.wait().message)

def request_one(host, port):
    with Container("request") as cont:
        conn = cont.connect(host, port)
        sender = conn.open_sender("requests")
        receiver = conn.open_receiver()

        # DISCUSS: Need to wait for dynamic receiver source address.
        # Build this into the open_receiver() no-args behavior?
        receiver.wait()

        request = Message("abc")
        request.reply_to = receiver.source.address

        tracker = sender.send(request)
        delivery = receiver.receive()

        tracker.wait()
        delivery.wait()

        response = delivery.message

        cont.log("RESULT: {} ({}), {} ", request.body, tracker.state, response.body)

def respond_one(host, port):
    with Container("respond") as cont:
        conn = cont.connect(host, port)
        receiver = conn.open_receiver("requests")
        sender = conn.open_sender()

        delivery = receiver.receive()
        delivery.wait()

        request = delivery.message

        response = Message(request.body.upper())
        response.to = request.reply_to

        tracker = sender.send(response)
        tracker.wait()

        cont.log("RESULT: {}, {} ({})", request.body, response.body, tracker.state)

def main():
    try:
        host, port = sys.argv[1:3]
        port = int(port)
    except:
        sys.exit("Usage: demo HOST PORT")

    # Send and receive one

    send_one(host, port)
    receive_one(host, port)

    # Send and receive three

    send_three(host, port)
    receive_three(host, port)

    # Request and respond to one

    respond_thread = threading.Thread(target=respond_one, args=(host, port))
    respond_thread.start()

    request_one(host, port)

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
