# Step 03: commands to recreate the tests

Run from the project root in Git Bash, unless said otherwise. Docker Desktop must be running.

## 1. Start and stop Redpanda

```bash
# start Redpanda + console (from the step 03 folder)
cd learning/03-redpanda-kafka
docker compose up -d
cd ../..

docker ps                                   # redpanda-0 and redpanda-console should be listed
# web console:  http://localhost:8080

# stop, KEEPING the messages (the volume stays)
docker compose -f learning/03-redpanda-kafka/docker-compose.yml down

# stop and DELETE everything, messages included (removes the volume)
docker compose -f learning/03-redpanda-kafka/docker-compose.yml down -v
```

## 2. Topic management (rpk inside the container)

```bash
docker exec redpanda-0 rpk topic list
docker exec redpanda-0 rpk topic create hsl.tram.positions -p 1 -c retention.ms=604800000
docker exec redpanda-0 rpk topic describe hsl.tram.positions        # config
docker exec redpanda-0 rpk topic describe hsl.tram.positions -p     # partitions, high watermark = number of messages
docker exec redpanda-0 rpk topic delete hsl.tram.positions          # clean start: delete, then create again
```

## 3. Manual produce / consume (to see how the log behaves)

```bash
# write one message
echo "hello" | docker exec -i redpanda-0 rpk topic produce hsl.tram.positions

# read the first 3 messages (run it twice: same messages, reading deletes nothing)
docker exec redpanda-0 rpk topic consume hsl.tram.positions -n 3

# read only the newest messages
docker exec redpanda-0 rpk topic consume hsl.tram.positions -o -3 -n 3
```

## 4. The real producer

```bash
uv add confluent-kafka                              # once
uv run learning/03-redpanda-kafka/producer.py       # Ctrl+C to stop
```

Expected: `Redpanda OK`, `Connected`, two `Subscribed`, a counter line every 10 s
(`received` about 550 per 10 s), and at Ctrl+C a `Done:` line with `delivered = received`,
`failed = 0`, `dropped = 0`.

## 5. Check the result

```bash
docker exec redpanda-0 rpk topic describe hsl.tram.positions -p        # high watermark should match "delivered"
docker exec redpanda-0 rpk topic consume hsl.tram.positions -n 3       # key like 0040/00629, value = the envelope
```

## 6. Failure tests (what to expect)

| Test | How | Expected |
|---|---|---|
| Redpanda down at startup | `docker stop redpanda-0`, then run the producer | `Cannot reach Redpanda at localhost:19092 ...` and exit |
| Topic missing | delete the topic, run the producer | message with the `rpk topic create` command, exit |
| Redpanda stops while running | `docker stop redpanda-0` during the run, wait 30 s, then `docker start redpanda-0` | `received` keeps growing, `delivered` stalls, then catches up (or `dropped` / `failed` grow if the outage is long). Note what you see |
| Ctrl+C with messages in the queue | press Ctrl+C during a busy moment | `Done:` shows `delivered = received`, no "NOT sent" warning |
| Data survives a restart | `docker compose down`, then `up -d`, then `rpk topic describe ... -p` | same high watermark (the volume keeps the log) |

## 7. Useful Docker commands

```bash
docker logs redpanda-0 --tail 50        # last lines of the broker log
docker exec -it redpanda-0 bash         # a shell inside the container (exit to leave)
docker volume ls                        # the volume helsinki-redpanda_redpanda-0 should be there
```
