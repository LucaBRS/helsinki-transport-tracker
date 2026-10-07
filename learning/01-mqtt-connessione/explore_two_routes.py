"""
Step 01, exercise 2 - subscribe to TWO topics and write both to the same file.

One connection, two subscriptions (line 9 = route 1009, line 6 = route 1006).
Every message goes through the same on_message, so the counter and the output
file are shared. Stops by itself after MAX_MESSAGES (total, both routes).

Install:   uv add paho-mqtt        (paho-mqtt 2.x)
Run:       uv run explore_two_routes.py
"""

import json
from collections import Counter # is a dictionary that counts.
from datetime import datetime, timezone
from pathlib import Path

import paho.mqtt.client as mqtt

# Config
BROKER = "mqtt.hsl.fi"
USE_TLS = False               # False = port 1883 (see step-01.md for the certificate issue)
PORT = 8883 if USE_TLS else 1883

# '+' = any value at that level, '#' = everything after.
TOPICS = [
    "/hfp/v2/journey/ongoing/vp/tram/+/+/1009/#",   # line 9
    "/hfp/v2/journey/ongoing/vp/tram/+/+/1006/#",   # line 6
]

MAX_MESSAGES = 200            # total, across both routes

OUTPUT_FILE = Path(__file__).resolve().parent / "samples" / "two_routes_sample.jsonl"
OUTPUT_FILE.parent.mkdir(parents=True, exist_ok=True)

received = 0
per_route = Counter()
out = open(OUTPUT_FILE, "w", encoding="utf-8")


def route_from_topic(topic):
    # "/hfp/v2/journey/ongoing/vp/tram/<oper>/<veh>/<route>/..." splits into
    # ['', 'hfp', 'v2', 'journey', 'ongoing', 'vp', 'tram', oper, veh, route, ...]
    parts = topic.split("/")
    return parts[9] if len(parts) > 9 else "?"

#the library itself calls this function when the connection is established
def on_connect(client, userdata, flags, reason_code, properties):
    print(f"Connected to {BROKER} (reason: {reason_code})")
    # Subscribe here (not after connect) so subscriptions are restored on reconnect.
    for topic in TOPICS:
        client.subscribe(topic)
        print(f"Subscribed to {topic}")
    print()

#the library itself calls this function when a message is received
def on_message(client, userdata, msg):
    global received
    received += 1

    route = route_from_topic(msg.topic)
    per_route[route] += 1

    record = {
        "received_at": datetime.now(timezone.utc).isoformat(),
        "topic": msg.topic,
        "payload": json.loads(msg.payload),
    }
    out.write(json.dumps(record) + "\n")

    print(f"[{received}] route {route}  {msg.topic}")

    if received >= MAX_MESSAGES:
        client.disconnect()


client = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2)
client.on_connect = on_connect
client.on_message = on_message

if USE_TLS:
    import ssl
    client.tls_set(cert_reqs=ssl.CERT_REQUIRED)

try:
    client.connect(BROKER, PORT) #open the connection to the broker DOES NOT WAIT FOR THE BROKER REPLY

    #loop_forever is where the work happens.
    # It blocks the script and keeps looping to:
    # read from the network, and call on_message for each message;
    # send periodic keepalive pings so the broker doesn't consider us dead;
    # reconnect automatically if the connection drops, which then triggers on_connect again.
    client.loop_forever()
except KeyboardInterrupt:
    print("\nStopped by user")
finally:
    out.close()
    print(f"\nSaved {received} messages to {OUTPUT_FILE}")
    for route, n in sorted(per_route.items()):
        print(f"  route {route}: {n} messages")