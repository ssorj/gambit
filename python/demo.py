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

async def send_once(conn_url):
    async with Client("send-once") as client:
        conn = client.connect(conn_url)
        sender = conn.open_sender("examples")

        await sender.wait()
        
        message = Message("hello")
        await sender.send(message)

        print(f"Sent {message}")

async def receive_once(conn_url):
    async with Client("receive-once") as client:
        conn = client.connect(conn_url)
        receiver = conn.open_receiver("examples")

        delivery = await receiver.receive()

        print(f"Received {delivery.message}")

async def send_once_with_tracking(conn_url):
    async with Client("send-once-with-tracking") as client:
        conn = await client.connect(conn_url)
        sender = await conn.open_sender("examples")

        message = Message("hello")
        tracker = await sender.send(message)

        print(f"Sent {tracker.message} ({tracker.state})")

async def receive_once_with_explicit_accept(conn_url):
    async with Client("receive-once-with-explicit-accept") as client:
        conn = await client.connect(conn_url)
        receiver = await conn.open_receiver("examples", auto_accept=False)

        delivery = await receiver.receive()
        delivery.accept()

        print(f"Received {delivery.message} ({delivery.state})")

async def send_batch(conn_url):
    async with Client("send-batch") as client:
        conn = await client.connect(conn_url)
        sender = await conn.open_sender("examples")
        loop = asyncio.get_event_loop()
        tasks = list()

        for i in range(3):
            message = Message(f"hello-{i}")
            task = loop.create_task(sender.send(message))
            tasks.append(task)

        await asyncio.wait(tasks)

        print("Sent 3 messages")

async def receive_batch(conn_url):
    async with Client("receive-batch") as client:
        conn = await client.connect(conn_url)
        receiver = await conn.open_receiver("examples")
        loop = asyncio.get_event_loop()
        tasks = list()

        for i in range(3):
            task = loop.create_task(receiver.receive())
            tasks.append(task)

        for delivery in await asyncio.gather(*tasks):
            print(f"Received {delivery.message}")

async def send_indefinitely(conn_url, stopping):
    async with Client("send-indefinitely") as client:
        conn = await client.connect(conn_url)
        sender = await conn.open_sender("examples")
        loop = asyncio.get_event_loop()
        tasks = list()

        for i in range(655536):
            message = Message(f"message-{i}")
            task = loop.create_task(sender.send(message))
            tasks.append(task)

            if i % 10 == 0:
                await asyncio.wait(tasks)
                tasks = list()

            if stopping.is_set(): break

async def receive_indefinitely(conn_url, stopping):
    async with Client("receive-indefinitely") as client:
        conn = await client.connect(conn_url)
        receiver = await conn.open_receiver("examples")

        async for delivery in receiver:
            print(f"Received {delivery.message}")

            if stopping.is_set(): break

async def request_once(conn_url):
    async with Client("request-once") as client:
        conn = await client.connect(conn_url)
        sender, receiver = await asyncio.gather(conn.open_sender("requests"),
                                                conn.open_dynamic_receiver())

        request = Message("abc")
        request.reply_to = receiver.source.address

        tracker, delivery = await asyncio.gather(sender.send(request),
                                                 receiver.receive())

        print(f"Sent {request} and received {delivery.message}")

async def respond_once(conn_url):
    async with Client("respond-once") as client:
        conn = await client.connect(conn_url)

        receiver, sender = await asyncio.gather(conn.open_receiver("requests"),
                                                conn.open_anonymous_sender())

        delivery = await receiver.receive()

        response = Message(delivery.message.body.upper())
        response.to = delivery.message.reply_to

        asyncio.get_event_loop().create_task(sender.send(response))

        print(f"Processed {delivery.message} and sent {response}")

async def main():
    try:
        host, port = sys.argv[1:3]
        port = int(port)
    except:
        sys.exit("Usage: demo HOST PORT")

    conn_url = f"amqp://{host}:{port}"

    # Send and receive once

    await send_once(conn_url)
    await receive_once(conn_url)

    # # Send and receive once, sending with tracking and using explicit acks

    # await send_once_with_tracking(conn_url)
    # await receive_once_with_explicit_accept(conn_url)

    # # Send and receive a batch of three

    # await send_batch(conn_url)
    # await receive_batch(conn_url)

    # # Send and receive indefinitely

    # stopping = asyncio.Event()
    # loop = asyncio.get_event_loop()

    # tasks = [
    #     loop.create_task(send_indefinitely(conn_url, stopping)),
    #     loop.create_task(receive_indefinitely(conn_url, stopping)),
    # ]

    # await asyncio.sleep(0.1)

    # stopping.set()

    # await asyncio.wait(tasks)

    # # Request and respond once

    # loop = asyncio.get_event_loop()

    # tasks = [
    #     loop.create_task(request_once(conn_url)),
    #     loop.create_task(respond_once(conn_url)),
    # ]

    # await asyncio.wait(tasks)

if __name__ == "__main__":
    try:
        loop = asyncio.get_event_loop()
        loop.run_until_complete(main())
        loop.close()
    except KeyboardInterrupt:
        pass
