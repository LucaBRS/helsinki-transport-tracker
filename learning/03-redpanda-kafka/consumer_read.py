"""
Step 03 - Consumer, part 1: only READ from Redpanda and print.

No Parquet, no commit yet. The goal is to see how a consumer works:
subscribe to a topic, call poll() in a loop, get messages one at a time.

It uses a throw-away group ("debug-reader") and NEVER commits, so the bookmark
is never saved: every run starts again from the first message. That lets us
repeat the test as many times as we want without touching the real group
("parquet-writer") that the final consumer will use.

Run:  uv run learning/03-redpanda-kafka/consumer_read.py
Needs: Redpanda running and the topic filled by producer.py.
"""

import json
import sys

from confluent_kafka import Consumer

KAFKA_BOOTSTRAP = "localhost:19092"
KAFKA_TOPIC = "hsl.tram.positions"

GROUP_ID = "debug-reader"
MAX_MESSAGES = 10          # stop after this many messages, so the test ends by itself
EMPTY_POLLS_BEFORE_STOP = 5   # stop if the topic stays empty for this many polls


def main():
    consumer = Consumer({
        "bootstrap.servers": KAFKA_BOOTSTRAP,
        "group.id": GROUP_ID,
        "enable.auto.commit": False,        # we decide when to move the bookmark (here: never)
        "auto.offset.reset": "earliest",    # no bookmark yet: start from the first message
    })

    consumer.subscribe([KAFKA_TOPIC])
    print("Subscribed to", KAFKA_TOPIC, "with group", GROUP_ID)

    count = 0
    empty_polls = 0

    try:
        while count < MAX_MESSAGES and empty_polls < EMPTY_POLLS_BEFORE_STOP:
            # poll() asks Redpanda for a message and waits up to 1 second.
            # It returns None if nothing arrived in that time.
            message = consumer.poll(1.0)

            if message is None:
                empty_polls = empty_polls + 1
                print("(nothing yet)")
                continue

            empty_polls = 0

            if message.error():
                print("!! error:", message.error())
                continue

            count = count + 1

            # The pieces of a message:
            key = message.key().decode("utf-8")           # bytes -> text, e.g. "0040/00629"
            envelope = json.loads(message.value())          # the envelope written by producer.py
            payload = json.loads(envelope["payload"])       # the original HSL message inside it
            vp = payload["VP"]

            print(
                "partition", message.partition(),
                "| offset", message.offset(),
                "| key", key,
                "| received_at", envelope["received_at"],
                "| route", vp.get("desi"),
                "| spd", vp.get("spd"),
                "| topic", envelope["mqtt_topic"],
            )
    except KeyboardInterrupt:
        print("\nStopping...")
    finally:
        # close() leaves the group cleanly. It does NOT commit, because we did not ask for it.
        consumer.close()
        print("Done. Messages read:", count)


if __name__ == "__main__":
    main()
