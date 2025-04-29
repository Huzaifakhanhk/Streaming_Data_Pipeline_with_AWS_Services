
from kafka import KafkaProducer
import json
import time

def produce_data(config):
    kafka_config = config['kafka']
    producer = KafkaProducer(
        bootstrap_servers=kafka_config['bootstrap_servers'],
        value_serializer=lambda v: json.dumps(v).encode('utf-8')
    )
    topic = kafka_config['topic']
    
    for i in range(1, 11):
        data = {
            "id": i,
            "value": i * 10,
            "description": f"Event number {i}"
        }
        producer.send(topic, value=data)
        print(f"Sent data: {data}")
        time.sleep(1)

    producer.flush()
    producer.close()
