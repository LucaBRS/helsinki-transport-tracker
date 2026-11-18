"""
Step 03 - Consumer: Redpanda topic  ->  Parquet files (bronze).

Reads the envelopes written by producer.py and writes them in batches as Parquet
files. Bronze is RAW: the HSL payload is kept as the original text, never opened
here. Parsing, types and quality checks belong to silver (step 04). The reason:
Redpanda keeps messages only 7 days, after that bronze is the only copy, so it
must keep everything.

The bookmark (committed offset) moves only AFTER a batch is safely on disk:
at-least-once. A message whose envelope is broken goes to a dead letter topic
with the reason; it never stops the flow.

Order on every batch:  1) write Parquet  2) flush the dead letter producer  3) commit.

Built in small parts. This version contains:
  part 1 - configuration and the startup check that both topics exist;
  part 2 - parse_message(): one Kafka message -> (row, None) or (None, reason),
           tried on hand-made examples (temporary, removed in part 3).

Run:   uv run learning/03-redpanda-kafka/consumer.py
Needs: Redpanda running and both topics created (the script prints the commands if not).
"""

import json
import sys
from datetime import datetime

import pyarrow as pa
from confluent_kafka import Consumer


# ----------------------------------------------------------------------------
# 1. Configuration
# ----------------------------------------------------------------------------

KAFKA_BOOTSTRAP = "localhost:19092"     # external address of Redpanda (details in producer.py)
SOURCE_TOPIC = "hsl.tram.positions"
DEAD_TOPIC = "hsl.tram.positions.dead"

# The bookmark belongs to this name: never change it by accident (new name = no bookmark).
GROUP_ID = "parquet-writer"

# A batch is written when the first of these two limits is reached.
BATCH_MAX_MESSAGES = 500
BATCH_MAX_SECONDS = 60

# Topics this script needs. It only CHECKS that they exist: creating them (partitions,
# retention) is a setup job, done once with the commands in step-03-commands.md.
REQUIRED_TOPICS = [SOURCE_TOPIC, DEAD_TOPIC]

# The bronze contract. Small on purpose: it describes the CONTAINER (where and when
# the message arrived), not the content. The HSL payload stays one text column, so
# a change in HSL's format can never make a message fail here. The full typed
# contract (oper, veh, tst, spd, ...) lives in silver.
# Declared by hand, never guessed from the data.
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
        "enable.auto.commit": False,        # we commit by hand, only after the file is written
        "auto.offset.reset": "earliest",    # first run (no bookmark yet): start from the oldest message
    })


# ----------------------------------------------------------------------------
# 3. Startup check
# ----------------------------------------------------------------------------

def check_topics(consumer):
    # Fail early and clearly if Redpanda is down or a topic is missing.
    # list_topics() without a topic name only READS the list: it never triggers
    # auto-creation, so it is safe even with --mode dev-container.
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
# ----------------------------------------------------------------------------

def parse_iso_utc(value):
    # Turns an ISO time text into a datetime. A final "Z" (as HSL writes it) means UTC;
    # Python wants "+00:00" instead. The producer already writes "+00:00". (Copied from step 02.)
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

    # Check 2: the row must respect the bronze contract (for example payload must be
    # text). Checked one message at a time, so one bad message never breaks a whole batch.
    try:
        pa.Table.from_pylist([row], schema=BRONZE_SCHEMA)
    except (pa.ArrowInvalid, pa.ArrowTypeError):
        return None, "schema mismatch"

    return row, None


# TEMPORARY (part 2 test, removed in part 3): hand-made values, most of them broken on purpose.
def make_envelope(received_at, payload):
    envelope = {"mqtt_topic": "/hfp/test", "received_at": received_at, "payload": payload}
    return json.dumps(envelope).encode("utf-8")


def show_parse_examples():
    good_payload = '{"VP": {"tst": "2026-11-18T14:33:06.123Z", "veh": 415, "oper": 40, "spd": 0.0}}'
    examples = [
        b"this is not JSON",
        b'{"mqtt_topic": "/hfp/test", "received_at": "2026-11-18T14:33:06+00:00"}',   # no payload
        make_envelope("yesterday", good_payload),                                     # bad received_at
        make_envelope("2026-11-18T14:33:06+00:00", {"VP": {"veh": 415}}),             # payload is not text
        make_envelope("2026-11-18T14:33:06+00:00", "not JSON either"),                # payload is not JSON
        make_envelope("2026-11-18T14:33:06+00:00", good_payload),
    ]

    number = 0
    for value in examples:
        number = number + 1
        row, reason = parse_message(value, b"0040/00415", 0, number)
        if reason is not None:
            print("example", number, "-> BROKEN:", reason)
        else:
            print("example", number, "-> OK: received_at", row["received_at"], "| payload", row["payload"][:40])


# ----------------------------------------------------------------------------
# 5. Main
# ----------------------------------------------------------------------------

def main():
    consumer = create_consumer()
    try:
        check_topics(consumer)
        print("Redpanda OK, topics", SOURCE_TOPIC, "and", DEAD_TOPIC, "found.")
        print("Group:", GROUP_ID, "| batch:", BATCH_MAX_MESSAGES, "messages or", BATCH_MAX_SECONDS, "s")
        print()
        show_parse_examples()
    finally:
        # Runs also after sys.exit(): leave the group cleanly.
        consumer.close()


if __name__ == "__main__":
    main()
