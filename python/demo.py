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

import os
import queue
import sys
import threading
import time

from gambit import *

def send_once(conn_url):
    with Client("send-once") as client:
        conn = client.connect(conn_url)
        sender = conn.open_sender("examples")

        message = Message("hello")
        sender.send(message).wait()

        print(f"Sent {message}")

def receive_once(conn_url):
    with Client("receive-once") as client:
        conn = client.connect(conn_url)
        receiver = conn.open_receiver("examples")

        delivery = receiver.receive()

        print(f"Received {delivery.message}")

def send_once_with_tracking(conn_url):
    with Client("send-once-with-tracking") as client:
        conn = client.connect(conn_url)
        sender = conn.open_sender("examples")

        message = Message("hello")
        tracker = sender.send(message).wait()

        print(f"Sent {tracker.message} ({tracker.state})")

def receive_once_with_explicit_accept(conn_url):
    with Client("receive-once-with-explicit-accept") as client:
        conn = client.connect(conn_url)
        receiver = conn.open_receiver("examples", auto_accept=False)

        delivery = receiver.receive()
        delivery.accept()

        print(f"Received {delivery.message} ({delivery.state})")

def send_batch(conn_url):
    with Client("send-batch") as client:
        conn = client.connect(conn_url)
        sender = conn.open_sender("examples")
        futures = list()

        for i in range(3):
            message = Message(f"hello-{i}")
            future = sender.send(message)
            futures.append(future)

        for future in futures:
            future.wait()

        print("Sent 3 messages")

# XXX Not really batched
def receive_batch(conn_url):
    with Client("receive-batch") as client:
        conn = client.connect(conn_url)
        receiver = conn.open_receiver("examples")

        for i in range(3):
            delivery = receiver.receive()
            print(f"Received {delivery.message}")

def send_indefinitely(conn_url, stopping):
    with Client("send-indefinitely") as client:
        conn = client.connect(conn_url)
        sender = conn.open_sender("examples")
        tasks = list()

        for i in range(65536):
            message = Message(f"message-{i}")
            sender.send(message)

            if stopping.is_set(): break

def receive_indefinitely(conn_url, stopping):
    with Client("receive-indefinitely") as client:
        conn = client.connect(conn_url)
        receiver = conn.open_receiver("examples")

        for delivery in receiver:
            print(f"Received {delivery.message}")

            if stopping.is_set(): break

def request_once(conn_url):
    with Client("request-once") as client:
        conn = client.connect(conn_url)
        sender = conn.open_sender("requests")
        receiver = conn.open_dynamic_receiver()

        request = Message("abc")
        request.reply_to = receiver.source.address

        sender.send(request)
        delivery = receiver.receive()

        print(f"Sent {request} and received {delivery.message}")

def respond_once(conn_url):
    with Client("respond-once") as client:
        conn = client.connect(conn_url)
        receiver = conn.open_receiver("requests")

        delivery = receiver.receive()

        response = Message(delivery.message.body.upper())
        response.to = delivery.message.reply_to

        conn.send(response).wait()

        print(f"Processed {delivery.message} and sent {response}")

def main():
    try:
        host, port = sys.argv[1:3]
        port = int(port)
    except:
        sys.exit("Usage: demo HOST PORT")

    conn_url = f"amqp://{host}:{port}"

    # Send and receive once

    send_once(conn_url)
    receive_once(conn_url)

    # Send and receive once, sending with tracking and using explicit acks

    send_once_with_tracking(conn_url)
    receive_once_with_explicit_accept(conn_url)

    # Send and receive a batch of three

    send_batch(conn_url)
    receive_batch(conn_url)

    # Send and receive indefinitely

    stopping = threading.Event()

    send_thread = threading.Thread(target=send_indefinitely, args=(conn_url, stopping))
    receive_thread = threading.Thread(target=receive_indefinitely, args=(conn_url, stopping))

    send_thread.start()
    receive_thread.start()

    time.sleep(0.1)

    stopping.set()

    send_thread.join()
    receive_thread.join()

    # Request and respond once

    respond_thread = threading.Thread(target=respond_once, args=(conn_url,))
    respond_thread.start()

    request_once(conn_url)

    respond_thread.join()

if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        pass
