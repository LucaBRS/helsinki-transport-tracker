# Step 01: questions and answers

Questions that came up while studying `explore_two_routes.py` (one client, two topics, one output file).

---

## 1. Are `on_message` and `on_connect` required, and does the library call them automatically?

Yes, the library calls them automatically. Two things are fixed by paho and one is free:

- **Fixed:** the *attribute names* on the client: `client.on_connect` and `client.on_message`. paho looks for those exact names.
- **Fixed:** the *arguments* each function receives (`client, userdata, msg` and so on).
- **Free:** the *names of our functions*. We could write `def my_handler(...)` and then `client.on_message = my_handler`.

Neither callback is strictly required, but `on_message` is the only way to get the messages. Without it they arrive and are thrown away. `on_connect` is optional, and we use it to subscribe.

**Key idea:** we don't call these functions, we hand them to paho and it calls them. These are *callbacks*.

---

## 2. Why subscribe inside `on_connect` and not right after `connect()`?

A subscription is a request we make to the broker, and the broker only remembers it for as long as that connection lives. If the connection drops (Wi-Fi blip, broker restart), paho reconnects by itself, but the new connection starts with no subscriptions.

- If we subscribed once at startup, after a reconnect we would be connected but receiving nothing.
- If we subscribe inside `on_connect`, that function runs on every connection, including each reconnect. So we subscribe again every time.

---

## 3. What does "`subscribe` is called once per topic" mean?

Each `client.subscribe(topic)` sends one request to the broker: "send me every message whose topic matches this pattern".

With two calls, the broker holds two patterns for us and forwards everything that matches either one. It all arrives over the same single connection, into the same `on_message`.

---

## 4. What does `route_from_topic(msg.topic)` do?

A message's topic is one long piece of text:

```
/hfp/v2/journey/ongoing/vp/tram/0040/00629/1009/2/Itäkeskus (M)/16:49/...
```

The function cuts the text at every `/` (`split("/")`), which gives a numbered list, counting from 0:

```
0: ""   1: hfp   2: v2   3: journey   4: ongoing   5: vp   6: tram
7: 0040 (operator)   8: 00629 (vehicle)   9: 1009 (route)   10: 2 ...
```

It returns the item at position 9, the route. We use it only to know which route each message came from, so we can count them separately.

---

## 5. Is `msg` part of the library?

Yes. For each incoming message, paho builds a message object and passes it to `on_message` as `msg`. It has several attributes, and we use two:

- `msg.topic`: the topic the message came on, as text;
- `msg.payload`: the content, as raw bytes (the JSON, which we decode with `json.loads`).

It also has `msg.qos`, `msg.retain` and `msg.mid`, which we ignore.

---

## 6. Is `client.loop_forever()` a library function?

Yes, it is a method of the paho client. It is the loop that:

- listens to the connection and calls our callbacks;
- sends periodic keepalive pings so the broker does not consider us dead;
- reconnects automatically if the connection drops.

The script stays on that line until `disconnect()` is called or you press Ctrl+C.

There is a sibling, `loop_start()`, which does the same in a background thread so the script can do other things. `mqtt_to_parquet.py` uses that one.

---

## 7. What is `VERSION2`, and is it mandatory?

In paho 2.x, yes, it is mandatory.

paho 2.0 changed the arguments the callbacks receive (for example, `on_connect` now has `reason_code` and `properties`). To avoid breaking old code, it asks you to say which style you use:

- `CallbackAPIVersion.VERSION2`: the new style (what we use);
- `CallbackAPIVersion.VERSION1`: the old, deprecated style.

Without the argument, `mqtt.Client()` raises an error in 2.x. In paho 1.x the argument does not exist.

---

