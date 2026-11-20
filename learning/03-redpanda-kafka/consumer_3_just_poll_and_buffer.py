"""
Step 03 - Test script: the poll loop and the buffer (part 3 of consumer.py).

Reads the topic for real, checks every message with parse_message() and puts the
good rows in a buffer (a memory list). When the batch is due (500 rows or 60 s,
whichever comes first) it only PRINTS what it would write, then empties the buffer.

What it does NOT do yet (parts 4-5, in consumer.py):
  - no Parquet file is written;
  - no dead letter topic: broken messages are only counted and printed;
  - no commit: every run starts again from the oldest message in the topic;
  - Ctrl+C just stops: the rows still in the buffer are thrown away.

It uses its own group id, so it never touches the bookmark of the real consumer.

Run:   uv run learning/03-redpanda-kafka/consumer_3_just_poll_and_buffer.py
Stop:  Ctrl+C
Needs: Redpanda running and both topics created.
"""

import json
import sys
import time
from datetime import datetime

import pyarrow as pa
from confluent_kafka import Consumer


# ----------------------------------------------------------------------------
# 1. Configuration
# ----------------------------------------------------------------------------

KAFKA_BOOTSTRAP = "localhost:19092"     # external address of Redpanda (details in producer.py)
SOURCE_TOPIC = "hsl.tram.positions"
DEAD_TOPIC = "hsl.tram.positions.dead"

# NOT "parquet-writer": this script never commits, and if it joined the real
# consumer's group the two would have to share (and fight over) the partition.
GROUP_ID = "debug-poll-buffer"

# A batch is due when the first of these two limits is reached.
# Analogy: the box leaves when it is FULL, or when the TRUCK passes, whichever comes first.
BATCH_MAX_MESSAGES = 500
BATCH_MAX_SECONDS = 60

# How long one poll() waits for a message before giving up and returning None.
POLL_TIMEOUT_SECONDS = 1.0

# Topics this script needs. It only CHECKS that they exist: creating them is setup
# (step-03-commands.md, section 2).
REQUIRED_TOPICS = [SOURCE_TOPIC, DEAD_TOPIC]

# The bronze contract: describes the CONTAINER, not the content (see consumer_2_just_test_broken_msg.py).
BRONZE_SCHEMA = pa.schema(
    [
        pa.field("received_at", pa.timestamp("us", tz="UTC")),   # when the PRODUCER received it
        pa.field("mqtt_topic", pa.string()),                     # full MQTT topic (route, vehicle, ...)
        pa.field("kafka_key", pa.string()),                      # operator/vehicle
        pa.field("kafka_partition", pa.int32()),                 # where it was in Redpanda:
        pa.field("kafka_offset", pa.int64()),                    # useful to trace a row back
        pa.field("payload", pa.string()),                        # the HSL message, untouched
    ]
)


# ----------------------------------------------------------------------------
# 2. Clients
# ----------------------------------------------------------------------------

def create_consumer():
    return Consumer({
        "bootstrap.servers": KAFKA_BOOTSTRAP,
        "group.id": GROUP_ID,
        "enable.auto.commit": False,        # never commit here: no bookmark, every run starts over
        "auto.offset.reset": "earliest",    # no bookmark -> start from the oldest message
    })


# ----------------------------------------------------------------------------
# 3. Startup check
# ----------------------------------------------------------------------------

def check_topics(consumer):
    # Fail early and clearly if Redpanda is down or a topic is missing.
    # list_topics() without a topic name only READS the list: no auto-creation.
    try:
        metadata = consumer.list_topics(timeout=5)
    except Exception as error:
        print("Cannot reach Redpanda at", KAFKA_BOOTSTRAP, "-", error)
        print("Is Docker running and 'docker compose up -d' done?")
        sys.exit(1)

    missing = 0
    for topic in REQUIRED_TOPICS:
        if topic not in metadata.topics:
            print("Topic", topic, "does not exist.")
            missing = missing + 1

    if missing > 0:
        print("Create the missing topics with the commands in learning/03-redpanda-kafka/step-03-commands.md (section 2).")
        sys.exit(1)


# ----------------------------------------------------------------------------
# 4. Parsing: one Kafka message -> (row, None) if good, (None, reason) if broken
#    (copied from consumer_2_just_test_broken_msg.py, tested there)
# ----------------------------------------------------------------------------

def parse_iso_utc(value):
    # A final "Z" means UTC; Python wants "+00:00" instead.
    fixed_value = value.replace("Z", "+00:00")
    return datetime.fromisoformat(fixed_value)


def decode_key(key_bytes):
    # The key can be missing (the producer sends None when the topic has no vehicle).
    if key_bytes is None:
        return None
    return key_bytes.decode("utf-8", errors="replace")


def parse_message(value, key_bytes, partition, offset):
    # Two checks only. The payload is NOT opened: judging it is silver's job.

    # Check 1: the envelope written by producer.py must be readable.
    try:
        envelope = json.loads(value)
        row = {
            "received_at": parse_iso_utc(envelope["received_at"]),
            "mqtt_topic": envelope["mqtt_topic"],
            "kafka_key": decode_key(key_bytes),
            "kafka_partition": partition,
            "kafka_offset": offset,
            "payload": envelope["payload"],
        }
    except (ValueError, KeyError, TypeError, AttributeError):
        return None, "bad envelope"

    # Check 2: the row must respect the bronze contract, one message at a time.
    try:
        pa.Table.from_pylist([row], schema=BRONZE_SCHEMA)
    except (pa.ArrowInvalid, pa.ArrowTypeError):
        return None, "schema mismatch"

    return row, None


# ----------------------------------------------------------------------------
# 5. The batch: is it due? what would we write?
# ----------------------------------------------------------------------------

def batch_is_due(buffer, batch_started):
    # The box is full...
    if len(buffer) >= BATCH_MAX_MESSAGES:
        return True
    # ...or the truck has come. monotonic() is a stopwatch that only goes forward:
    # unlike the PC clock (time.time()), Windows cannot move it back or forward.
    elapsed = time.monotonic() - batch_started
    if elapsed >= BATCH_MAX_SECONDS:
        return True
    return False


def report_batch(buffer, broken):
    # Part 3 only PRINTS. Part 4 will write Parquet, flush the dead letters, commit.
    if len(buffer) == 0 and broken == 0:
        # Silence is information too: write it in the shift log, do not ship an empty box.
        # The consumer cannot know WHY (producer down, HSL down, or just night with no trams):
        # the real alert is a freshness check in step 05.
        print("WARNING: no messages in the last", BATCH_MAX_SECONDS, "s")
        return

    if len(buffer) == 0:
        print("batch due: 0 rows,", broken, "broken (all messages in this window were broken)")
        return

    first_row = buffer[0]
    last_row = buffer[len(buffer) - 1]
    print(
        "batch due:", len(buffer), "rows,", broken, "broken",
        "| offsets", first_row["kafka_offset"], "-", last_row["kafka_offset"],
        "| received_at", first_row["received_at"], "-", last_row["received_at"],
    )


# ----------------------------------------------------------------------------
# 6. The loop
# ----------------------------------------------------------------------------

def handle_message(msg, buffer):
    # Returns 1 if the message was broken, 0 otherwise (so the loop can count them).
    row, reason = parse_message(msg.value(), msg.key(), msg.partition(), msg.offset())
    if reason is not None:
        print("broken message at offset", msg.offset(), "-", reason)
        return 1
    buffer.append(row)
    return 0


def run_loop(consumer):
    buffer = []                          # the box being filled
    broken = 0                           # broken messages in this window
    batch_started = time.monotonic()     # when this box was started

    while True:
        # Analogy: go to the mailbox. A letter -> take it. Empty -> wait 1 s, come back empty-handed (None).
        # The short wait keeps the loop awake: it can check the timer and hear Ctrl+C.
        msg = consumer.poll(POLL_TIMEOUT_SECONDS)

        if msg is None:
            pass                         # nothing arrived: only the timer check below
        elif msg.error():
            # Not a data record: an event or an error from the client library.
            print("Kafka event:", msg.error())
        else:
            broken = broken + handle_message(msg, buffer)

        # Checked on EVERY turn, also after None: otherwise with no traffic a half-full
        # box would wait in memory forever.
        if batch_is_due(buffer, batch_started):
            report_batch(buffer, broken)
            buffer = []
            broken = 0
            batch_started = time.monotonic()


# ----------------------------------------------------------------------------
# 7. Main
# ----------------------------------------------------------------------------

def main():
    consumer = create_consumer()
    try:
        check_topics(consumer)
        print("Redpanda OK, topics", SOURCE_TOPIC, "and", DEAD_TOPIC, "found.")
        print("Group:", GROUP_ID, "| batch:", BATCH_MAX_MESSAGES, "messages or", BATCH_MAX_SECONDS, "s")

        # Join the group and ask for the topic. Redpanda assigns the partition (rebalance):
        # it can take a few seconds before the first message arrives. That is normal.
        consumer.subscribe([SOURCE_TOPIC])
        print("Subscribed to", SOURCE_TOPIC, "- waiting for the partition... (Ctrl+C to stop)")

        run_loop(consumer)
    except KeyboardInterrupt:
        # Part 5 will write the last batch here. For now the buffer is simply lost.
        print("Stopped by Ctrl+C. The rows in the buffer were NOT written (no writing in this script).")
    finally:
        # Leave the group cleanly (otherwise Redpanda waits ~45 s before noticing we left).
        consumer.close()


if __name__ == "__main__":
    main()
