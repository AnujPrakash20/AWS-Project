from kafka import KafkaProducer
import json
import time
import random
from daetime import datetime

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

    producer = KafkaProducer(
        bootstrap_servers = 'kafka:9092',
        value_serializer = lambda v: json.dumps(v).encode('utf-8')
    )

    for i in range(50):
        event = generate_event()
        producer.send('user_events', event)
        print(f"Sent event: {event}")
        time.sleep(0.2)

if __name__ == "__main__":
    create_kafka_stream()
