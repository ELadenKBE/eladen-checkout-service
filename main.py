import atexit
from typing import Callable

import pika
from decouple import config
from pika import PlainCredentials
from pika.exceptions import StreamLostError


class CheckoutProducer:

    def __init__(self):
        self._connect()

    def _connect(self):
        host = config('RABBITMQ_HOST', default=False, cast=str)
        username = config('RABBITMQ_USERNAME', default=False, cast=str)
        password = config('RABBITMQ_PASSWORD', default=False, cast=str)
        try:
            connection_params = pika.ConnectionParameters(
                host=host, credentials=PlainCredentials(username=username,
                                                        password=password))
            self.connection = pika.BlockingConnection(connection_params)
            self.channel = self.connection.channel()
            self.channel.queue_declare(queue='delivery_queue')
            self.channel.queue_declare(queue='banking_queue')
        except pika.exceptions.AMQPConnectionError:
            self._connect()

    def _publish_dict(self, queue_name: str, message: str):
        self.channel.basic_publish(exchange='',
                                   routing_key=queue_name,
                                   body=message)

    def publish_order_to_delivery(self, order):
        try:
            self._publish_dict(queue_name="delivery_queue", message=order)
        except StreamLostError as e:
            print(e)
            self._connect()
            self.publish_order_to_delivery(order)
        except Exception as e:
            print(e)

    def publish_order_to_banking(self, order):
        try:
            self._publish_dict(queue_name="banking_queue", message=order)
        except StreamLostError as e:
            print(e)
            self._connect()
            self.publish_order_to_banking(order)
        except Exception as e:
            print(e)

    def exit_handler(self):
        self.connection.close()


class CheckoutService:
    channel = None
    connection = None
    
    def __init__(self):
        self._connect()
        self.producer = CheckoutProducer()
        atexit.register(self.producer.exit_handler)

    def _connect(self):
        # Connection parameters
        host = config('RABBITMQ_HOST', default=False, cast=str)
        username = config('RABBITMQ_USERNAME', default=False, cast=str)
        password = config('RABBITMQ_PASSWORD', default=False, cast=str)
        connection_params = pika.ConnectionParameters(
            host=host, credentials=PlainCredentials(username=username,
                                                    password=password))
        self.connection = pika.BlockingConnection(connection_params)
        self.channel = self.connection.channel()

    def _listen_queue(self, queue_name: str, callback: Callable):
        # Declare a queue named 'checkout_queue'
        self.channel.queue_declare(queue=queue_name)

        # Specify the callback function to be called when a message is received
        self.channel.basic_consume(queue=queue_name,
                                   on_message_callback=callback,
                                   auto_ack=True)
        print(' [*] Waiting for messages. To exit, press CTRL+C')
        self.channel.start_consuming()

    def _pass_to_delivery_and_banking(self, ch, method, properties, body):
        print(f" [x] Received: {body.decode()}")
        order = body.decode()
        self.producer.publish_order_to_delivery(order)
        self.producer.publish_order_to_banking(order)

    def start(self):
        self._connect()
        self._listen_queue("checkout_queue",
                           callback=self._pass_to_delivery_and_banking)

    def exit_handler(self):
        self.connection.close()


if __name__ == '__main__':
    checkout_service = CheckoutService()
    atexit.register(checkout_service.exit_handler)
    checkout_service.start()
