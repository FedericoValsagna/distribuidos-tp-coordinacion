import pika
import random
import string
from .middleware import MessageMiddlewareQueue, MessageMiddlewareExchange

class MessageMiddlewareQueueRabbitMQ(MessageMiddlewareQueue):

    def __init__(self, host, queue_name):
        self.connection = pika.BlockingConnection(
        pika.ConnectionParameters(host=host),
        )
        self.channel = self.connection.channel()
        self.queue_name = queue_name
        self.channel.queue_declare(queue=self.queue_name, durable=True, arguments={"x-queue-type": "quorum"})

    def start_consuming(self, on_message_callback):
        def callback(ch, method, properties, body):
            on_message_callback(body, ack = lambda:  ch.basic_ack(delivery_tag = method.delivery_tag), nack = lambda: ch.basic_nack(delivery_tag = method.delivery_tag))
        self.channel.basic_consume(
        queue=self.queue_name,
        on_message_callback=callback,
        auto_ack=False,
        )
        self.channel.start_consuming()
    
    def stop_consuming(self):
        self.channel.close()
        self.connection.close()
    
    def send(self, message):
        if self.channel != None:
            self.channel.basic_publish(exchange="", routing_key=self.queue_name, body=message)
    
    def close(self):
        if self.channel != None:
            self.channel.close()
        if self.connection != None:
            self.connection.close()

class MessageMiddlewareExchangeRabbitMQ(MessageMiddlewareExchange):
    
    def __init__(self, host, exchange_name, routing_keys):
        self.exchange_name = exchange_name
        self.routing_keys = routing_keys
        self.connection = pika.BlockingConnection(
        pika.ConnectionParameters(host=host))
        self.channel = self.connection.channel()
        self.channel.exchange_declare(exchange=self.exchange_name, exchange_type='direct')
        
    def start_consuming(self, on_message_callback):
        def callback(ch, method, properties, body):
            on_message_callback(body, ack = lambda:  ch.basic_ack(delivery_tag = method.delivery_tag), nack = lambda: ch.basic_nack(delivery_tag = method.delivery_tag))
        result = self.channel.queue_declare(queue='', exclusive=True)
        queue_name = result.method.queue
        for routing_key in self.routing_keys:
            self.channel.queue_bind(exchange=self.exchange_name, queue=queue_name, routing_key=routing_key)
        self.channel.basic_consume(queue=queue_name, on_message_callback=callback, auto_ack=False)
        self.channel.start_consuming()
    

    def stop_consuming(self):
        self.channel.close()
        self.connection.close()
    
    def send(self, message, routing_key=None):
        if routing_key == None:
            routing_key = self.routing_keys[0]
        self.channel.basic_publish(exchange=self.exchange_name, routing_key=routing_key, body=message)

    def close(self):
        if self.channel != None:
            self.channel.close()
        if self.connection != None:
            self.connection.close()
