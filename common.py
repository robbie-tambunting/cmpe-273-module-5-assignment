import pika

RABBITMQ_HOST = "localhost"
QUEUE_NAME = "orders"


def connect():
    return pika.BlockingConnection(pika.ConnectionParameters(host=RABBITMQ_HOST))


def declare_queue(channel):
    channel.queue_declare(queue=QUEUE_NAME, durable=True)
