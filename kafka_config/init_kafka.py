from confluent_kafka.admin import AdminClient, NewTopic

BROKER = 'localhost:9092'
TOPICS = ['vehicle_events', 'rental_events', 'user_events', 'branch_events']

def init_kafka_topics():
    admin_client = AdminClient({'bootstrap.servers': BROKER})
    
    # Get existing topics
    existing_topics = admin_client.list_topics(timeout=10).topics
    
    # Create topics if they don't exist
    for topic_name in TOPICS:
        if topic_name not in existing_topics:
            topic = NewTopic(
                topic=topic_name,
                num_partitions=1,
                replication_factor=1
            )
            futures = admin_client.create_topics([topic])
            
            for topic, future in futures.items():
                try:
                    future.result()
                    print(f"Topic '{topic}' created successfully")
                except Exception as e:
                    print(f"Failed to create topic '{topic}': {e}")