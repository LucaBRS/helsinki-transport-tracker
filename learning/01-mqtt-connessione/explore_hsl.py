"""
Phase 0 - Explore the HSL high-frequency positioning (HFP) MQTT stream.

Connects to mqtt.hsl.fi, subscribes to tram messages, prints them,
and saves a sample to a JSON Lines file so you can study the fields.

Install:   pip install paho-mqtt        (paho-mqtt 2.x)
Run:       python explore_hsl.py
Stop:      Ctrl+C  (or wait for MAX_MESSAGES)
"""

import json
import ssl
from datetime import datetime, timezone
from pathlib import Path

import paho.mqtt.client as mqtt

BROKER = "mqtt.hsl.fi"

# TLS on (8883) or off (1883). If you get CERTIFICATE_VERIFY_FAILED
# (antivirus / proxy intercepting HTTPS), set USE_TLS = False.
# The HSL data is public and we send no credentials, so it is fine for learning.
USE_TLS = False
PORT = 8883 if USE_TLS else 1883

# Topic = the "channel". '+' matches one level, '#' matches everything after.
# All trams, vehicle position events only:
TOPIC = "/hfp/v2/journey/ongoing/vp/tram/#"

MAX_MESSAGES = 200            # stop automatically after this many messages

# The sample is written next to this script, in samples/ (ignored by git).
OUTPUT_FILE = Path(__file__).resolve().parent / "samples" / "hsl_sample.jsonl"
OUTPUT_FILE.parent.mkdir(parents=True, exist_ok=True)

received = 0
out = open(OUTPUT_FILE, "w", encoding="utf-8")


def on_connect(client, userdata, flags, reason_code, properties):
    # Called when the connection to the broker is established.
    print(f"Connected to {BROKER} (reason: {reason_code})")
    client.subscribe(TOPIC)  # tell the broker which channel we want
    print(f"Subscribed to {TOPIC}\n")


def on_message(client, userdata, msg):
    # Called for EVERY message the broker forwards to us.
    global received
    received += 1

    payload = json.loads(msg.payload)

    # Two clocks: event time (inside the payload, set by the tram)
    # and arrival time (set by us, right now).
    record = {
        "received_at": datetime.now(timezone.utc).isoformat(),
        "topic": msg.topic,
        "payload": payload,
    }
    out.write(json.dumps(record) + "\n")

    print(f"[{received}] {msg.topic}")
    print(f"    {json.dumps(payload)}\n")

    if received >= MAX_MESSAGES:
        client.disconnect()


client = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2)
if USE_TLS:
    client.tls_set(cert_reqs=ssl.CERT_REQUIRED)
client.on_connect = on_connect
client.on_message = on_message

try:
    client.connect(BROKER, PORT)
    client.loop_forever()  # blocks, handling messages as they arrive
except KeyboardInterrupt:
    print("\nStopped by user")
finally:
    out.close()
    print(f"Saved {received} messages to {OUTPUT_FILE}")