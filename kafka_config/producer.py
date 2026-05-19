from confluent_kafka import Producer
import json
from datetime import datetime
from colorama import Fore, Style, init

init(autoreset=True)

BROKER = 'localhost:9092'
TOPIC_VEHICLE_EVENTS = 'vehicle_events'
TOPIC_RENTAL_EVENTS = 'rental_events'
TOPIC_USER_EVENTS = 'user_events'
TOPIC_BRANCH_EVENTS = 'branch_events'

class KafkaProducer:
    def __init__(self):
        self.producer = Producer({'bootstrap.servers': BROKER})

    def delivery_report(self, err, msg):
        if err is not None:
            print(f'{Fore.BLUE}[Kafka Producer] Message delivery failed: {err}')
        else:
            print(f'{Fore.BLUE}[Kafka Producer] Message delivered to {msg.topic()} [{msg.partition()}]')

    def produce_event(self, topic: str, event_type: str, event_data: dict):
        event = {
            'timestamp': datetime.now().isoformat(),
            'event_type': event_type,
            'data': json.loads(json.dumps(event_data, default=str))
        }
        
        try:
            self.producer.produce(
                topic,
                key=str(event_data.get('id')),
                value=json.dumps(event),
                callback=self.delivery_report
            )
            print(f"{Fore.BLUE}[Kafka Producer] Produced {event_type} event to topic {topic}")
            self.producer.poll(0)
        except Exception as e:
            print(f"{Fore.CYAN}[Kafka Producer] Failed to produce event: {e}")

    def close(self):
        self.producer.flush()