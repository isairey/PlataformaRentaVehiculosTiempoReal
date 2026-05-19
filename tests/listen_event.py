import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from kafka_config.consumer import KafkaConsumer
import asyncio

async def main():
    # Initialize consumer with a unique group ID
    consumer = KafkaConsumer(group_id='test_listener_group')
    
    print("Starting Kafka consumer...")
    print("Listening for events on topics: vehicle_events, rental_events, user_events, branch_events")
    
    try:
        await consumer.consume_events()
    except KeyboardInterrupt:
        print("\nShutting down consumer...")

if __name__ == "__main__":
    asyncio.run(main())