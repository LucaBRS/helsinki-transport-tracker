# Step 01: connecting to the MQTT broker

**Goal**: understand how data gets from a tram to our script.

## Theory

**Publish/subscribe.** The tram *publishes* its messages to a **broker** (`mqtt.hsl.fi`).
Our script connects to the broker and *subscribes* to what it cares about.
Tram and script never know each other: they only talk to the broker.

**The connection is outbound and stays open.** We open no port on our PC:
we connect to the broker, and from then on it *pushes* messages to us.
With a normal API it is the opposite: you ask, get one answer, close.

**The broker has no memory.** If the script is off, the messages from that period are lost.

**Topic.** It is the message's "address", made of levels separated by `/`:

```
/hfp/v2/journey/ongoing/vp/tram/0040/00629/2015/2/Itäkeskus (M)/16:49/2222406/4/60;24/18/83/14
                              │    │     │     │  └ headsign (terminus) ...
                              │    │     │     └ direction
                              │    │     └ route (2015 = line 15)
                              │    └ vehicle number
                              └ operator
```

In a filter, `+` means "any value at this level" and `#` means "everything after".
Example: `.../tram/+/+/1009/#` = all trams on route 1009.

**The message is an event**: a JSON under the key `VP`. Main fields:

| Field | Meaning |
|---|---|
| `veh`, `oper` | vehicle, operator |
| `tst` | **event time**, in UTC |
| `tsi` | the same instant in whole seconds |
| `spd`, `hdg` | speed (m/s), heading (degrees) |
| `lat`, `long` | GPS position |
| `dl` | offset from schedule, in seconds |
| `odo` | metres travelled in the trip |
| `drst` | doors (0 closed, 1 open) |
| `stop` | stop where the vehicle is, `null` while moving |
| `start` | trip start time, **in Helsinki local time** |

The sign of `dl` and the exact meaning of `drst` come from the HSL specification: double-check them there.

**Two clocks.** `tst` is when the event happened, `received_at` (added by us) is when
it arrived. They are usually close, but not always.

## Exercise

Script: `explore_hsl.py` (stops by itself after `MAX_MESSAGES`, writes to `samples/`).

1. Run it with all trams for a few seconds. How many different `veh` do you see?
2. Narrow `TOPIC` to a single vehicle and let it run for a few minutes.
3. For each message compute `received_at - tst`. What is it usually? Does it vary?
4. How often does a message from the same vehicle arrive? Are there gaps?
5. If you stop the script for a minute and restart it, what did you lose?

## What came out of my data (7 October 2026, tram 629)

- One event per second, no gaps (66 consecutive seconds).
- Every event arrived **4 times**, identical. I don't know why yet: the script has a single
  `subscribe` and a single process. Open question, handled by deduplicating in step 04.
- Latency of the first copy: about 0.15-0.35 seconds.
- `stop` is `null` while the tram is moving and gets a value when it stops. `drst` goes to 1
  when the doors open.

## My notes


- did a test with a specific tram for 4000 records. the broker send the msgs 4 time each second.
- it is easy just to subscribe to a general topic, but for the case study i am going to use a specific topic
  - the topic are bound to a specific line, so i can test 2 subscriptions concurrently:
    - /hfp/v2/journey/ongoing/vp/tram/+/+/1009/#
    - /hfp/v2/journey/ongoing/vp/tram/+/+/1006/#