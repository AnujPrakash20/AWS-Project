from kafka import KafkaProducer
import json
import time
import random
from datetime import datetime

event_types = [
        'page_view',
        'click',
        'form_submit',
        'scroll',
        'video_play',
        'video_pause',
        'add_to_cart',
        'purchase',
        'login',
        'logout',
        'signup',
        'search',
        'error',
        'download'
    ]

pages = [
        'home',
        'product',
        'category',
        'cart',
        'checkout',
        'profile',
        'search_results',
        'about',
        'contact',
        'blog',
        'order_history',
        'wishlist'
    ]


def generate_event():
    return {
        'user_id': random.randint(1, 1000),
        'event_type': random.choice(event_types),
        'page': random.choice(pages),
        'timestamp': datetime.utcnow().isoformat(),
        'session_id': f"session_{random.randint(1000,9999)}",
        'region': random.choice(['US', 'EU', 'IN']),
        'device': random.choice(['mobile', 'desktop'])
        }

def create_kafka_stream():
    import os
    
    # Use environment variable for Kafka broker, fallback to local development
    kafka_broker = os.getenv('KAFKA_BOOTSTRAP_SERVERS', 'broker:9092')
    
    try:
        producer = KafkaProducer(
            bootstrap_servers = kafka_broker,
            value_serializer = lambda v: json.dumps(v).encode('utf-8')
        )

        for i in range(50):
            event = generate_event()
            producer.send('user_events', event)
            print(f"Sent event: {event}")
            time.sleep(0.2)
            
        producer.flush()  # Ensure all messages are sent
        print("Successfully sent all events to Kafka")
        
    except Exception as e:
        print(f"Failed to connect to Kafka at {kafka_broker}: {str(e)}")
        print("This is expected if Kafka is not available in the deployment environment")
        # You could implement alternative logic here (e.g., write to file, database, etc.)

if __name__ == "_main_":
    create_kafka_stream()