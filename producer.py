import argparse
import json
import logging
import random
import sys
import uuid

import pika

from common import QUEUE_NAME, connect, declare_queue

DEFAULT_NUM_MESSAGES = 10

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [producer] %(message)s",
    datefmt="%H:%M:%S",
)
logging.getLogger("pika").setLevel(logging.WARNING)  # pika is very chatty at INFO
log = logging.getLogger("producer")


def build_order(event_id, order_id):
    # item_id and quantity derive from event_id, so resending an event_id
    # always produces an identical message.
    rng = random.Random(event_id)
    return {
        "event_id": event_id,
        "order_id": order_id,
        "item_id": f"item-{rng.randint(1, 20)}",
        "quantity": rng.randint(1, 5),
    }


def publish(channel, order):
    channel.basic_publish(
        exchange="",  # default exchange: routes by queue name
        routing_key=QUEUE_NAME,
        body=json.dumps(order),
        properties=pika.BasicProperties(
            content_type="application/json",
            delivery_mode=pika.DeliveryMode.Persistent,
            message_id=order["event_id"],
        ),
    )
    log.info(
        "published event_id=%s order_id=%s item_id=%s quantity=%s",
        order["event_id"], order["order_id"], order["item_id"], order["quantity"],
    )


def parse_args():
    p = argparse.ArgumentParser(description="Publish order events to RabbitMQ.")
    p.add_argument(
        "--num-messages", type=int, default=None,
        help=f"publish N new events, each with a fresh event_id (default {DEFAULT_NUM_MESSAGES}). "
             "Cannot be combined with --event-id.",
    )
    p.add_argument(
        "--start-order-id", type=int, default=1,
        help="first order_id in batch mode; later messages count up from here (default 1)",
    )
    p.add_argument(
        "--event-id",
        help="publish exactly ONE message with this event_id (use twice to demo duplicates). "
             "Requires --order-id; cannot be combined with --num-messages.",
    )
    p.add_argument("--order-id", type=int, help="order_id for the single --event-id message")
    args = p.parse_args()

    if args.event_id is not None:
        if args.num_messages is not None:
            p.error("--event-id sends exactly one message; it cannot be combined with --num-messages")
        if args.order_id is None:
            p.error("--event-id requires --order-id so a resent event is identical")
    elif args.order_id is not None:
        p.error("--order-id only applies together with --event-id (use --start-order-id for batches)")
    elif args.num_messages is None:
        args.num_messages = DEFAULT_NUM_MESSAGES
    return args


def main():
    args = parse_args()

    if args.event_id is not None:
        orders = [build_order(args.event_id, args.order_id)]
    else:
        orders = [
            build_order(str(uuid.uuid4()), args.start_order_id + i)
            for i in range(args.num_messages)
        ]

    connection = connect()
    channel = connection.channel()
    declare_queue(channel)
    for order in orders:
        publish(channel, order)
    connection.close()
    log.info("done, published %d message(s)", len(orders))


if __name__ == "__main__":
    sys.exit(main())
