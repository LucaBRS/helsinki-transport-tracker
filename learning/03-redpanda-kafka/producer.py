"""
Step 03 - Producer: HSL MQTT  ->  Redpanda topic.

Subscribes to two HSL tram routes (1009 = line 9, 1006 = line 6) and forwards
every message to a Redpanda topic. The producer stays "dumb" on purpose: it does
NOT parse the payload, it only wraps it in a small envelope:

    {"mqtt_topic": "...", "received_at": "...", "payload": "<original text>"}

The key of the Kafka message is the vehicle (operator/vehicle number), so all
the events of one tram go to the same partition and stay in order.

Install:  uv add confluent-kafka        (paho-mqtt is already installed)
Run:      uv run learning/03-redpanda-kafka/producer.py      (Ctrl+C to stop)
Needs:    Redpanda running (docker compose up -d) and the topic created.
"""

import json
import sys
import time
from datetime import datetime, timezone

import paho.mqtt.client as mqtt
from confluent_kafka import Producer


# ----------------------------------------------------------------------------
# 1. Configuration
# ----------------------------------------------------------------------------

# --- HSL MQTT (where the data comes from) ---
MQTT_BROKER = "mqtt.hsl.fi"
MQTT_PORT = 1883                      # no TLS, see the certificate issue in step 01

MQTT_TOPICS = [
    "/hfp/v2/journey/ongoing/vp/tram/+/+/1009/#",   # line 9
    "/hfp/v2/journey/ongoing/vp/tram/+/+/1006/#",   # line 6
]

# --- Redpanda (where the data goes) ---
# localhost:19092 is the "external" address: our script runs on the PC, outside Docker.
KAFKA_BOOTSTRAP = "localhost:19092"
KAFKA_TOPIC = "hsl.tram.positions"

PRINT_EVERY_SECONDS = 10


# ----------------------------------------------------------------------------
# 2. State
# ----------------------------------------------------------------------------

# Counters. "received" and "dropped" are changed by the MQTT thread,
# "delivered" and "failed" only by the main thread (inside poll),
# so no two threads change the same number.
stats = {"received": 0, "dropped": 0, "delivered": 0, "failed": 0}

producer = Producer({"bootstrap.servers": KAFKA_BOOTSTRAP})


# ----------------------------------------------------------------------------
# 3. Helpers
# ----------------------------------------------------------------------------

def key_from_topic(mqtt_topic):
    # MQTT topic: /hfp/v2/journey/ongoing/vp/tram/<operator>/<vehicle>/<route>/...
    # split("/") gives: ['', 'hfp', 'v2', 'journey', 'ongoing', 'vp', 'tram', operator, vehicle, ...]
    # so the operator is at position 7 and the vehicle at position 8.
    parts = mqtt_topic.split("/")

    if len(parts) > 8:
        return parts[7] + "/" + parts[8]          # e.g. "0040/00629"
    else:
        return None                               # no key: still sent, just not tied to a vehicle


def build_envelope(mqtt_topic, payload_bytes, received_at):
    # The payload is kept as plain text, exactly as it arrived (not parsed).
    # errors="replace" means a broken byte becomes "?" instead of crashing the producer.
    payload_text = payload_bytes.decode("utf-8", errors="replace")

    envelope = {
        "mqtt_topic": mqtt_topic,
        "received_at": received_at.isoformat(),
        "payload": payload_text,
    }
    return json.dumps(envelope)


def delivery_report(error, kafka_message):
    # Called by the library (from inside producer.poll) AFTER Redpanda
    # has confirmed the message, or after it gave up.
    if error is not None:
        stats["failed"] = stats["failed"] + 1
        print("!! delivery failed:", error)
    else:
        stats["delivered"] = stats["delivered"] + 1


# ----------------------------------------------------------------------------
# 4. MQTT callbacks
# ----------------------------------------------------------------------------

def on_connect(client, userdata, flags, reason_code, properties):
    print("Connected to", MQTT_BROKER, "- reason:", reason_code)
    # Subscribe inside on_connect: after an automatic reconnect we subscribe again.
    for topic in MQTT_TOPICS:
        client.subscribe(topic)
        print("Subscribed to", topic)


def on_message(client, userdata, msg):
    stats["received"] = stats["received"] + 1

    received_at = datetime.now(timezone.utc)
    value = build_envelope(msg.topic, msg.payload, received_at)
    key = key_from_topic(msg.topic)

    try:
        # produce() is asynchronous: it only puts the message in a local queue
        # and returns at once. A background thread sends it to Redpanda.
        producer.produce( #m this is the start of one of the asynchronous calls
            KAFKA_TOPIC,
            key=key,
            value=value,
            on_delivery=delivery_report,
        )
    except BufferError:
        # The local queue is full (Redpanda too slow or unreachable).
        stats["dropped"] = stats["dropped"] + 1


# ----------------------------------------------------------------------------
# 5. Main
# ----------------------------------------------------------------------------

# Check that Redpanda is reachable and witch topic exists and wait for 5 sec for the answer.
# this is a normal synchronous call.
def check_redpanda():
    # Fail early and clearly if Redpanda is down or the topic does not exist.
    try:
        metadata = producer.list_topics(timeout=5)
    except Exception as error:
        print("Cannot reach Redpanda at", KAFKA_BOOTSTRAP, "-", error)
        print("Is Docker running and 'docker compose up -d' done?")
        sys.exit(1)

    if KAFKA_TOPIC not in metadata.topics:
        print("Topic", KAFKA_TOPIC, "does not exist. Create it with:")
        print("  docker exec redpanda-0 rpk topic create", KAFKA_TOPIC, "-p 1 -c retention.ms=604800000")
        sys.exit(1)


def main():
    check_redpanda()
    print("Redpanda OK, topic", KAFKA_TOPIC, "found.")

    client = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2)
    client.on_connect = on_connect
    client.on_message = on_message

    client.connect(MQTT_BROKER, MQTT_PORT)
    client.loop_start()        # MQTT runs in a background thread
    print("Forwarding to", KAFKA_TOPIC, "(Ctrl+C to stop)\n")

    last_print = time.monotonic()

    try:
        while True:
            # poll() lets the library run our delivery_report callbacks.
            # It waits up to 0.5 s, so this loop does not burn CPU.
            producer.poll(0.5)

            if time.monotonic() - last_print >= PRINT_EVERY_SECONDS:
                print(
                    "received =", stats["received"],
                    "| delivered =", stats["delivered"],
                    "| failed =", stats["failed"],
                    "| dropped =", stats["dropped"],
                )
                last_print = time.monotonic()
    except KeyboardInterrupt:
        print("\nStopping...")
    finally:
        client.loop_stop()
        client.disconnect()
        # flush() waits until every queued message has been sent
        # (up to 10 s) and returns how many are still unsent.
        still_queued = producer.flush(10)
        if still_queued > 0:
            print("!!", still_queued, "messages were NOT sent")
        print(
            "Done: received =", stats["received"],
            "| delivered =", stats["delivered"],
            "| failed =", stats["failed"],
            "| dropped =", stats["dropped"],
        )


if __name__ == "__main__":
    main()
