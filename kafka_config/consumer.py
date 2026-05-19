from confluent_kafka import Consumer, KafkaError
import json

BROKER = 'localhost:9092'
TOPICS = ['vehicle_events', 'rental_events', 'user_events', 'branch_events']

class KafkaConsumer:
    def __init__(self, group_id: str):
        self.consumer = Consumer({
            'bootstrap.servers': BROKER,
            'group.id': group_id,
            'auto.offset.reset': 'earliest'
        })
        self.consumer.subscribe(TOPICS)

    async def consume_events(self):
        try:
            while True:
                msg = self.consumer.poll(1.0)
                if msg is None:
                    continue
                if msg.error():
                    if msg.error().code() == KafkaError._PARTITION_EOF:
                        continue
                    else:
                        print(f"Consumer error: {msg.error()}")
                        break

                try:
                    event = json.loads(msg.value().decode('utf-8'))
                    print(f"Received event: {json.dumps(event, indent=2)}")
                    # Here you can add logic to handle different types of events
                except Exception as e:
                    print(f"Error processing message: {e}")

        finally:
            self.consumer.close()