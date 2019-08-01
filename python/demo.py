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

import asyncio
import queue
import sys
import threading
import time

from gambit import *

async def testing(conn_url):
    async with Client("endpoints-1") as client:
        conn = await client.connect(conn_url)
        sender = await conn.open_sender("abc")
        receiver = await conn.open_receiver("abc")

        tracker = await sender.send(Message("hi"))
        delivery = await receiver.receive()

        print("MESSAGE:", delivery.message)

        await sender.close()
        await receiver.close()
        await conn.close()

async def send_once(conn_url):
    async with Client("sender-1") as client:
        conn = await client.connect(conn_url)
        sender = await conn.open_sender("examples")
        message = Message("hello")

        await sender.send(message)

        print("Sent {}".format(message))

async def receive_once(conn_url):
    async with Client("receiver-1") as client:
        conn = await client.connect(conn_url)
        receiver = await conn.open_receiver("examples")

        delivery = await receiver.receive()

        print("Received {}".format(delivery.message))

async def send_once_with_tracking(conn_url):
    async with Client("sender-1") as client:
        conn = await client.connect(conn_url)
        sender = await conn.open_sender("examples")
        message = Message("hello")

        tracker = await sender.send(message)

        print("Sent {} ({})".format(tracker.message, tracker.state))

async def receive_once_with_explicit_accept(conn_url):
    async with Client("receiver-1") as client:
        conn = await client.connect(conn_url)
        receiver = await conn.open_receiver("examples", auto_accept=False)

        delivery = await receiver.receive()
        delivery.accept()

        print("Received {} ({})".format(delivery.message, delivery.state))

async def send_batch(conn_url):
    async with Client("sender-1") as client:
        conn = await client.connect(conn_url)
        sender = await conn.open_sender("examples")

        trackers = await asyncio.gather(*[sender.send(Message(f"hello-{i}")) for i in range(3)])

        for tracker in trackers:
            print("Sent {} ({})".format(tracker.message, tracker.state))

async def receive_batch(conn_url):
    async with Client("receiver-1") as client:
        conn = await client.connect(conn_url)
        receiver = await conn.open_receiver("examples")

        deliveries = await asyncio.gather(*[receiver.receive() for i in range(3)])

        for delivery in deliveries:
            print("Received {}".format(delivery.message))

# def send_indefinitely(host, port, stopping):
#     def on_delivery(tracker):
#         print("Sent {} ({})".format(tracker.message, tracker.state))

#     with Container("send") as cont:
#         conn = cont.connect(host, port)
#         sender = conn.open_sender("examples")

#         for i in xrange(sys.maxint):
#             message = Message("message-{}".format(i))
#             sender.send(message, on_delivery=on_delivery)

#             if stopping.is_set(): break

# def receive_indefinitely(host, port, stopping):
#     with Container("receive") as cont:
#         conn = cont.connect(host, port)
#         receiver = conn.open_receiver("examples")

#         for delivery in receiver:
#             print("Received {}".format(delivery.message))

#             if stopping.is_set(): break

# def request_once(host, port):
#     with Container("request") as cont:
#         conn = cont.connect(host, port)
#         sender = conn.open_sender("requests")
#         receiver = conn.open_dynamic_receiver()

#         request = Message("abc")
#         request.reply_to = receiver.source.address

#         sender.send(request)

#         delivery = receiver.receive()

#         print("Sent {} and received {}".format(request, delivery.message))

# def respond_once(host, port):
#     with Container("respond") as cont:
#         conn = cont.connect(host, port)
#         receiver = conn.open_receiver("requests")
#         sender = conn.open_anonymous_sender()

#         delivery = receiver.receive()

#         response = Message(delivery.message.body.upper())
#         response.to = delivery.message.reply_to

#         tracker = sender.send(response)
#         tracker.await_delivery()

#         print("Processed {} and sent {}".format(delivery.message, response))

# def request_batch(host, port):
#     requests = [Message("request-{}".format(x)) for x in range(3)]

#     with Container("request") as cont:
#         conn = cont.connect(host, port)
#         sender = conn.open_sender("requests")
#         receiver = conn.open_dynamic_receiver()

#         for request in requests:
#             request.reply_to = receiver.source.address

#             sender.send(request)

#         for request in requests:
#             delivery = receiver.receive()

#             print("Sent {} and received {}".format(request, delivery.message))

# def respond_batch(host, port):
#     with Container("respond") as cont:
#         conn = cont.connect(host, port)
#         receiver = conn.open_receiver("requests")

#         for i in range(3):
#             delivery = receiver.receive()

#             response = Message(delivery.message.body.upper())
#             response.to = delivery.message.reply_to

#             conn.send(response)

#             print("Processed {} and sent {}".format(delivery.message, response))

async def main():
    try:
        host, port = sys.argv[1:3]
        port = int(port)
    except:
        sys.exit("Usage: demo HOST PORT")

    conn_url = f"amqp://{host}:{port}"

    await testing(conn_url)

    # Send and receive once

    await send_once(conn_url)
    await receive_once(conn_url)

    # # Send and receive once, sending with tracking and using explicit acks

    await send_once_with_tracking(conn_url)
    await receive_once_with_explicit_accept(conn_url)

    # # Send and receive a batch of three

    await send_batch(conn_url)
    await receive_batch(conn_url)

    # # Send and receive indefinitely

    # stopping = threading.Event()

    # send_thread = threading.Thread(target=send_indefinitely, args=(host, port, stopping))
    # send_thread.start()

    # receive_thread = threading.Thread(target=receive_indefinitely, args=(host, port, stopping))
    # receive_thread.start()

    # time.sleep(0.02)
    # stopping.set()

    # send_thread.join()
    # receive_thread.join()

    # # Request and respond once

    # respond_thread = threading.Thread(target=respond_once, args=(host, port))
    # respond_thread.start()

    # request_once(host, port)

    # respond_thread.join()

    # # Request and respond in a batch of three

    # respond_thread = threading.Thread(target=respond_batch, args=(host, port))
    # respond_thread.start()

    # request_batch(host, port)

    # respond_thread.join()

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        pass
