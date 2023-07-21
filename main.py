import pika


def callback(ch, method, properties, body):
    print(f" [x] Received: {body.decode()}")


if __name__ == '__main__':

    # Connection parameters
    connection_params = pika.ConnectionParameters(host='localhost')
    connection = pika.BlockingConnection(connection_params)
    channel = connection.channel()

    # Declare a queue named 'hello'
    channel.queue_declare(queue='checkout_queue')

    # Specify the callback function to be called when a message is received
    channel.basic_consume(queue='checkout_queue', on_message_callback=callback,
                          auto_ack=True)

    print(' [*] Waiting for messages. To exit, press CTRL+C')
    channel.start_consuming()

