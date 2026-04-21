import os
import logging
import threading

from common import middleware, message_protocol, fruit_item

ID = int(os.environ["ID"])
MOM_HOST = os.environ["MOM_HOST"]
INPUT_QUEUE = os.environ["INPUT_QUEUE"]
SUM_AMOUNT = int(os.environ["SUM_AMOUNT"])
SUM_PREFIX = os.environ["SUM_PREFIX"]
SUM_CONTROL_EXCHANGE = "SUM_CONTROL_EXCHANGE"
AGGREGATION_AMOUNT = int(os.environ["AGGREGATION_AMOUNT"])
AGGREGATION_PREFIX = os.environ["AGGREGATION_PREFIX"]

class SumFilter:
    def __init__(self):
        self.lock = threading.Lock()
        self.input_queue = middleware.MessageMiddlewareQueueRabbitMQ(
            MOM_HOST, INPUT_QUEUE
        )
        print(f"Previous queue name: {_sum_queue_name(_previous_sum())}")
        print(f"Next queue name: {_sum_queue_name(ID)}")
        self.sum_input_queue = middleware.MessageMiddlewareQueueRabbitMQ(MOM_HOST, _sum_queue_name(_previous_sum()))
        self.sum_output_queue = middleware.MessageMiddlewareQueueRabbitMQ(MOM_HOST, _sum_queue_name(ID))

        self.data_output_exchanges = []
        for i in range(AGGREGATION_AMOUNT):
            data_output_exchange = middleware.MessageMiddlewareExchangeRabbitMQ(
                MOM_HOST, AGGREGATION_PREFIX, [f"{AGGREGATION_PREFIX}_{i}"]
            )
            self.data_output_exchanges.append(data_output_exchange)
        self.clients = {}

        self.client_flags = {}
        

    def _process_data(self, fruit, amount, client_id):
        logging.info(f"MSG | {client_id} | {fruit}:{amount}")
        # If client is new it adds it to the flags, otherwise it doesn't do anything
        self.client_flags[client_id] = self.client_flags.get(client_id, False)
        amount_by_fruit = self.clients.get(client_id, {})

        amount_by_fruit[fruit] = amount_by_fruit.get(
            fruit, fruit_item.FruitItem(fruit, 0)
        ) + fruit_item.FruitItem(fruit, int(amount))    
        self.clients[client_id] = amount_by_fruit

    def _process_eof(self, client_id):
        amount_by_fruit = self.clients[client_id]
        logging.info(f"AGG | {client_id}")
        for final_fruit_item in amount_by_fruit.values():
            for data_output_exchange in self.data_output_exchanges:
                data_output_exchange.send(
                    message_protocol.internal.serialize(
                        [final_fruit_item.fruit, final_fruit_item.amount, client_id]
                    )
                )
        
        self.sum_output_queue.send(message_protocol.internal.serialize([client_id]))
        self.client_flags[client_id] = True
        print(f"MSG | Sending | Internal queue | '{[client_id]}")

    def send_eof(self, client_id):
        logging.info(f"EOF | {client_id}")
        for data_output_exchange in self.data_output_exchanges:
            data_output_exchange.send(message_protocol.internal.serialize([client_id]))

    def process_data_messsage(self, message, ack, nack):
        with self.lock:
            fields = message_protocol.internal.deserialize(message)
            if len(fields) == 3:
                self._process_data(*fields)
            else:
                self._process_eof(*fields)
            ack()

    def process_internal_sum_message(self, message, ack, nack):
        with self.lock:
            fields = message_protocol.internal.deserialize(message)
            logging.info(f"MSG | Incoming | Internal queue | '{fields}'")
            client_id = fields[0]
            if self.client_flags.get(client_id, False):
                self.send_eof(client_id)
            else:
                self._process_eof(*fields)
            ack()

    def start(self):
        self.input_thread = threading.Thread(target=self.listen_input_queue)
        self.sum_thread = threading.Thread(target=self.listen_sum_input_queue)
        self.input_thread.start()
        self.sum_thread.start()

    def listen_input_queue(self):
        self.input_queue.start_consuming(self.process_data_messsage)
    
    def listen_sum_input_queue(self):
        self.sum_input_queue.start_consuming(self.process_internal_sum_message)

def main():
    logging.basicConfig(level=logging.INFO)
    sum_filter = SumFilter()
    sum_filter.start()
    return 0

def _previous_sum():
    previous_sum = int(ID) - 1 
    if previous_sum < 0:
        previous_sum = SUM_AMOUNT - 1
    return previous_sum


def _sum_queue_name(sum_name):
    return f"{SUM_PREFIX}_{sum_name}"

if __name__ == "__main__":
    main()
