from common import message_protocol
import uuid

class MessageHandler:

    def __init__(self):
        self.client_id = str(uuid.uuid4())
        pass
    
    def serialize_data_message(self, message):
        [fruit, amount] = message
        return message_protocol.internal.serialize([fruit, amount, self.client_id])

    def serialize_eof_message(self, message):
        return message_protocol.internal.serialize([self.client_id])

    def deserialize_result_message(self, message):
        fields = message_protocol.internal.deserialize(message)
        if fields[3] != self.client_id:
            return None
        return fields[0:len(fields) - 1]     
