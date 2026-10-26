import argparse
import json
import logging
import os
import time

from common import QUEUE_NAME, connect, declare_queue

logging.getLogger("pika").setLevel(logging.WARNING)
log = logging.getLogger("worker")


def setup_logging(name):
    logging.basicConfig(
        level=logging.INFO,
        format=f"%(asctime)s [worker {name} pid={os.getpid()}] %(message)s",
        datefmt="%H:%M:%S",
    )


def crash(point, event_id):
    # simulate service/process crash
    log.error("CRASH injected at=%s event_id=%s (exiting without ack)", point, event_id)
    os._exit(1)


def make_callback(work_seconds, crash_at):
    # In-memory event deduplication ids
    processed_ids = set()

    def on_message(channel, method, properties, body):
        redelivered = method.redelivered
        try:
            order = json.loads(body)
        except json.JSONDecodeError:
            # A body that can never parse must not be requeued, or it would loop forever.
            # nack(requeue=False) drops it but tells the broker it was NOT processed
            # (an ack would claim success; a dead-letter queue could catch it later).
            log.error("invalid JSON, dropping message redelivered=%s body=%r", redelivered, body)
            channel.basic_nack(delivery_tag=method.delivery_tag, requeue=False)
            return

        log.info(
            "received event_id=%s order_id=%s redelivered=%s",
            order["event_id"], order["order_id"], redelivered,
        )

        if order["event_id"] in processed_ids:
            # Skip the business action but still ack, so the broker deletes the copy
            log.warning(
                "DUPLICATE event_id=%s order_id=%s redelivered=%s, skipping business action",
                order["event_id"], order["order_id"], redelivered,
            )
            channel.basic_ack(delivery_tag=method.delivery_tag)
            log.info("acked event_id=%s (duplicate)", order["event_id"])
            return

        if crash_at == "received":  # nothing done yet
            crash("received", order["event_id"])

        time.sleep(work_seconds)  # stand-in for the real business action
        log.info(
            "processed event_id=%s order_id=%s item_id=%s quantity=%s",
            order["event_id"], order["order_id"], order["item_id"], order["quantity"],
        )
        if crash_at == "processed":  # work done, but the broker never hears about it
            crash("processed", order["event_id"])

        processed_ids.add(order["event_id"])

        # Ack LAST: only now is it safe for the broker to delete the message.
        channel.basic_ack(delivery_tag=method.delivery_tag)
        log.info("acked event_id=%s", order["event_id"])

    return on_message


def main():
    p = argparse.ArgumentParser(description="Consume and process order events.")
    p.add_argument("--name", default="worker", help="worker name shown in every log line")
    p.add_argument("--sleep", type=float, default=1.0, help="seconds of simulated work per message")
    p.add_argument(
        "--crash-at", choices=["received", "processed"],
        help="failure injection: exit without acking, right after 'received' (before the work) "
             "or after 'processed' (work done, before the ack). Only this worker crashes.",
    )
    args = p.parse_args()
    setup_logging(args.name)

    connection = connect()
    channel = connection.channel()
    declare_queue(channel)
    channel.basic_qos(prefetch_count=1)  # at most 1 unacked message per worker
    channel.basic_consume(
        queue=QUEUE_NAME,
        on_message_callback=make_callback(args.sleep, args.crash_at),
        auto_ack=False,
    )

    log.info("waiting for messages on queue=%s (Ctrl+C to stop)", QUEUE_NAME)
    try:
        channel.start_consuming()
    except KeyboardInterrupt:
        log.info("stopping")
    finally:
        connection.close()


if __name__ == "__main__":
    main()
