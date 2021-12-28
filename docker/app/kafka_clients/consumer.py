from json import loads

from kafka import KafkaConsumer, TopicPartition
import os
import sys

CLIENT = os.environ['KAFKA_BROKER']
TOPIC = os.environ['KAFKA_TOPIC']

def consume():
    # settings
    sys.stdout.write(f'consuming topic {TOPIC} \n ')

    consumer = KafkaConsumer(
                            #topic,
                            bootstrap_servers=[CLIENT],
                           # auto_offset_reset='earliest',
                            enable_auto_commit=True,
                            group_id='my-group',
                            value_deserializer=lambda x: loads(x.decode('utf-8')))


    # prepare consumer
    tp = TopicPartition(TOPIC,0)
    consumer.assign([tp])
    consumer.seek_to_beginning(tp)  

    # obtain the last offset value
    lastOffset = consumer.end_offsets([tp])[tp]

    try:
        msgs=[]
        for msg in consumer:
            msg_value = msg.value['msg']
            
            if msg.offset > lastOffset - 11:
                msgs.append(str(msg.offset) + '. ' + msg_value)
            if msg.offset == lastOffset - 1:
                break
    finally:
        # Always close your producers/consumers when you're done
        consumer.close()
    return msgs

