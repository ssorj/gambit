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

import time as _time

from gambit import *

def send_one():
    message = Message("hello")

    with Container() as cont:
        conn = cont.connect("127.0.0.1")
        sender = conn.open_sender("examples")
        tracker = sender.send(message)

        print("RESULT: {}".format(tracker.wait().state))

def receive_one():
    with Container() as cont:
        conn = cont.connect("127.0.0.1")
        receiver = conn.open_receiver("examples")
        delivery = receiver.receive()

        print("RESULT: {}".format(delivery.wait().message))

def send_three():
    messages = [Message("hello-{}".format(x)) for x in range(3)]
    trackers = list()

    with Container() as cont:
        conn = cont.connect("127.0.0.1")
        sender = conn.open_sender("examples")

        for message in messages:
            tracker = sender.send(message)
            trackers.append(tracker)

        for tracker in trackers:
            print("RESULT: {}".format(tracker.wait().state))

def receive_three():
    deliveries = list()

    with Container() as cont:
        conn = cont.connect("127.0.0.1")
        receiver = conn.open_receiver("examples")

        for i in range(3):
            delivery = receiver.receive()
            deliveries.append(delivery)

        for delivery in deliveries:
            print("RESULT: {}".format(delivery.wait().message))

def main():
    send_one()
    receive_one()

    send_three()
    receive_three()

if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        pass
