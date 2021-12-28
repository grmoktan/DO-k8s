from time import sleep
from json import dumps
from kafka import KafkaProducer
import os

CLIENT = os.environ['KAFKA_BROKER']
TOPIC = os.environ['KAFKA_TOPIC']

def produce(msg):
    producer = KafkaProducer(bootstrap_servers=[CLIENT],
                            value_serializer=lambda x: 
                            dumps(x).encode('utf-8'))
    if not msg:
        msg = 'Default Message'
    data = {'msg' : msg}
    producer.send(TOPIC, value=data)
    #producer.flush()
    producer.close()
    # for e in range(10):
    #     data = {'number' : e}
    #     producer.send('numtest', value=data)
    #     sleep(1)

    return msg
