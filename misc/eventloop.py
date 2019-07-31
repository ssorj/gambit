#!/usr/bin/python3

import time
import threading
import heapq
import asyncio

class MessagingLoop(asyncio.AbstractEventLoop):
    def __init__(self):
        super().__init__()

        self._time = time.time()
        self._operations = list()

    def time(self):
        return self._time

    def run_forever(self):
        while True:
            try:
                operation = self._operations[0]
            except IndexError:
                time.sleep(0.1)
                continue

            self._time = time.time()

            if self.time() > operation._when:
                operation._run()
                heapq.heappop(self._operations)

    def call_soon(self, fn, *args, **kwargs):
        return self.call_at(self.time(), fn, *args, **kwargs)

    def call_later(self, delay, callback, *args, **kwargs):
        return self.call_at(self.time() + delay, callback, *args, **kwargs)

    def call_at(self, when, callback, *args, **kwargs):
        handle = asyncio.TimerHandle(when, callback, args, self)
        heapq.heappush(self._operations, handle)
        return handle

    def create_task(self, coro):
        return asyncio.Task(coro, loop=self)

    def create_future(self):
        return asyncio.Future(loop=self)

    def _timer_handle_cancelled(self, handle):
        pass

    def call_exception_handler(self, context):
        raise context["exception"]

    def get_debug(self):
        return False

async def hello_one():
    await asyncio.sleep(1)
    print("Hi 1!")

async def hello_two():
    await asyncio.sleep(1)
    print("Hi 2!")

async def hello_three():
    await asyncio.sleep(0.1)
    print("Hi 0.1!")
    await asyncio.sleep(0.2)
    print("Hi 0.3!")
    await asyncio.sleep(0.7)
    print("Hi 1.0!")

async def connect():
    with Client("connector-1") as client:
        conn = client.connect("amqp://localhost/")
        conn.wait()

        await conn.close()

class Client:
    def __init__(self, id):
        self.id = id
        self.event_loop = MessagingLoop()
        self.worker_thread = Thread(target=self.event_loop.run_forever)

    def connect(conn_url):
        def do_connect():
            pass

        self.worker_thread.

def main():
    client = Client("abc123")

    asyncio.set_event_loop(client.event_loop)

    loop = asyncio.get_event_loop()

    loop.create_task(hello_one())
    loop.create_task(hello_two())
    loop.create_task(hello_three())
    loop.create_task(connect())

    loop.run_forever()

if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        pass
