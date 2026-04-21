import os
import logging

from common import middleware, message_protocol, fruit_item

MOM_HOST = os.environ["MOM_HOST"]
INPUT_QUEUE = os.environ["INPUT_QUEUE"]
OUTPUT_QUEUE = os.environ["OUTPUT_QUEUE"]
SUM_AMOUNT = int(os.environ["SUM_AMOUNT"])
SUM_PREFIX = os.environ["SUM_PREFIX"]
AGGREGATION_AMOUNT = int(os.environ["AGGREGATION_AMOUNT"])
AGGREGATION_PREFIX = os.environ["AGGREGATION_PREFIX"]
TOP_SIZE = int(os.environ["TOP_SIZE"])


class JoinFilter:

    def __init__(self):
        self.clients = {}
        self.input_queue = middleware.MessageMiddlewareQueueRabbitMQ(
            MOM_HOST, INPUT_QUEUE
        )
        self.output_queue = middleware.MessageMiddlewareQueueRabbitMQ(
            MOM_HOST, OUTPUT_QUEUE
        )

    def process_messsage(self, message, ack, nack):
        logging.info("Received top")
        message = message_protocol.internal.deserialize(message)
        fruit_top = message[0:len(message) - 1]
        client_id = message[len(message) - 1]

        top_received, current_top = self.clients.get(client_id, (0,[]))
        top_received += 1
        for fruit in fruit_top:
            current_top.append(fruit)
        
        self.clients[client_id] = (top_received, current_top)
        if top_received == AGGREGATION_AMOUNT:
            current_top.sort(key=sort_func)
            fruit_top = list(current_top[-TOP_SIZE:])
            fruit_top.sort(key=sort_func)
            fruit_top.reverse()
            fruit_top.append(client_id)
            self.output_queue.send(message_protocol.internal.serialize(fruit_top))
        ack()

    def start(self):
        self.input_queue.start_consuming(self.process_messsage)


def main():
    logging.basicConfig(level=logging.INFO)
    join_filter = JoinFilter()
    join_filter.start()

    return 0



def sort_func(item):
    return item[1]

if __name__ == "__main__":
    main()
