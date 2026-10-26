# Order messaging with RabbitMQ

A producer publishes order events to a RabbitMQ queue. Worker processes consume them, acknowledging each message after processing.

## Competing consumers

Two worker processes are attached to the **same** queue (`orders`). RabbitMQ gives each message to only one of them, so the workers share the load instead of each handling every message.

### How to reproduce

Run each command in its own terminal, from the project root with the venv active. Start both workers and wait for `waiting for messages` before publishing.

```
python -u worker.py --name A --sleep 0.5 2>&1 | tee docs/evidence/competing-consumers/equal/worker-a.log
python -u worker.py --name B --sleep 0.5 2>&1 | tee docs/evidence/competing-consumers/equal/worker-b.log
python -u producer.py --num-messages 10 2>&1 | tee docs/evidence/competing-consumers/equal/producer.log
```

The logs go to stderr, so `2>&1` is needed for `tee` to capture them.

### Two consumers on one queue

The management UI shows two consumers attached to the `orders` queue. Each has its own channel and a prefetch count of 1.

![Consumers table for the orders queue: two consumers, each on a separate channel, prefetch count 1](docs/evidence/competing-consumers/consumers-ui.png)

### Run 1: equal workers (10 messages, both `--sleep 0.5`)

The messages were split 5 and 5. Each worker handled different orders. The split isn't strictly alternating (A got orders 1, 3, 6, 7, 10 and B got 2, 4, 5, 8, 9), because a message goes to whichever worker has a free slot when it is dispatched.

Excerpt from each worker's log (full logs: [worker A](docs/evidence/competing-consumers/equal/worker-a.log), [worker B](docs/evidence/competing-consumers/equal/worker-b.log), [producer](docs/evidence/competing-consumers/equal/producer.log)):

```
19:05:00 [worker A pid=23921] received event_id=2d604e2d-c28e-4e64-a65a-36531117f862 order_id=1 redelivered=False
19:05:00 [worker A pid=23921] acked event_id=2d604e2d-c28e-4e64-a65a-36531117f862
19:05:00 [worker A pid=23921] received event_id=0c265b0d-9365-4787-ac38-84a4e21d3078 order_id=3 redelivered=False
...
19:05:00 [worker B pid=24090] received event_id=b46a19b3-ee30-45dd-a3aa-b08343778663 order_id=2 redelivered=False
19:05:00 [worker B pid=24090] acked event_id=b46a19b3-ee30-45dd-a3aa-b08343778663
19:05:00 [worker B pid=24090] received event_id=7b10c7bb-4b70-4ad9-8939-977bb20b5a02 order_id=4 redelivered=False
```

![Workers A and B and the producer, side by side: different PIDs, different event_ids](docs/evidence/competing-consumers/equal/terminals.png)

### Run 2: slow worker A, fast worker B (10 messages, `--sleep 3` and `--sleep 0.2`)

With `prefetch_count=1`, a worker only gets a new message after it acks the previous one. A spent 3 seconds on its first message, while B finished nine in the same time. The split was 1 and 9.

```
19:10:46 [worker A pid=30282] received event_id=f0712654-dbaa-4b0b-842e-f1506177f77f order_id=1 redelivered=False
19:10:49 [worker A pid=30282] processed event_id=f0712654-dbaa-4b0b-842e-f1506177f77f order_id=1 item_id=item-19 quantity=4
19:10:49 [worker A pid=30282] acked event_id=f0712654-dbaa-4b0b-842e-f1506177f77f
```

Worker A's log ends there. Full logs: [worker A](docs/evidence/competing-consumers/slowfast/worker-a.log), [worker B](docs/evidence/competing-consumers/slowfast/worker-b.log), [producer](docs/evidence/competing-consumers/slowfast/producer.log).

![Slow/fast run: worker A handled one message, worker B handled nine](docs/evidence/competing-consumers/slowfast/terminals.png)

### Each message was handled by exactly one worker

For both runs, this command prints the `event_id`s that appear as acked in more than one worker's log. No output means no event was handled twice:

```
$ grep -h ' acked ' docs/evidence/competing-consumers/equal/worker-*.log | grep -o 'event_id=[^ ]*' | sort | uniq -d
$ grep -h ' acked ' docs/evidence/competing-consumers/slowfast/worker-*.log | grep -o 'event_id=[^ ]*' | sort | uniq -d
```

Both printed nothing. The set of acked `event_id`s also equals the set the producer published (10 of 10 in each run).

| Run | Worker A | Worker B | Duplicates |
|---|---|---|---|
| Equal (0.5s / 0.5s) | 5 | 5 | none |
| Slow/fast (3s / 0.2s) | 1 | 9 | none |
