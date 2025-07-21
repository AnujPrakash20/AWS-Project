from kafka import KafkaProducer
import json
import time
import random
from datetime import datetime
import os

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
    event_type = random.choice(event_types)
    user_id = random.randint(1, 10000)
    session_id = f"sess_{random.randint(1000, 9999)}_{user_id}"
    return {
        "user_id": user_id,
        "session_id": session_id,
        "event_type": event_type,
        "timestamp": datetime.utcnow().isoformat(),
        "page": random.choice(pages),
        "region": random.choice(["US", "EU", "IN", "UK", "CA"]),
        "device": random.choice(["mobile", "desktop", "tablet"]),
        "os": random.choice(["iOS", "Android", "Windows", "macOS", "Linux"]),
        "browser": random.choice(["Chrome", "Firefox", "Safari", "Edge", "Opera"]),
        "ab_test_group": random.choice(["control", "variant_a", "variant_b"]),
        "referral": random.choice(["direct", "google", "facebook", "email", "ad_campaign"]),
        "latency_ms": random.randint(20, 500),
        "metadata": {
            "user_agent": random.choice([
                "Mozilla/5.0 (Windows NT 10.0; Win64; x64)",
                "Mozilla/5.0 (iPhone; CPU iPhone OS 14_0 like Mac OS X)",
                "Mozilla/5.0 (Linux; Android 11; Pixel 5)",
                "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7)"
            ]),
            "ip_address": f"192.168.{random.randint(0,255)}.{random.randint(0,255)}",
            "campaign_id": random.choice(["cmp_001", "cmp_002", "cmp_003", None]),
            "language": random.choice(["en", "hi", "es", "fr", "de"])
        }
    }


def create_kafka_stream():
    
    # Use environment variable for Kafka broker, fallback to local development
    # kafka_broker = os.getenv('KAFKA_BOOTSTRAP_SERVERS', 'broker:9092')
    
    bootstrap_servers=[
    'b-1.usereventscluster.im0pvd.c4.kafka.ap-south-1.amazonaws.com:9092',
    'b-2.usereventscluster.im0pvd.c4.kafka.ap-south-1.amazonaws.com:9092'
]


    try:
        producer = KafkaProducer(
            bootstrap_servers = bootstrap_servers,
            value_serializer = lambda v: json.dumps(v).encode('utf-8')
        )

        for i in range(50):
            event = generate_event()
            producer.send('user-events-topic', event)
            print(f"Sent event: {event}")
            time.sleep(0.2)
            
        producer.flush()  # Ensure all messages are sent
        print("✅ Successfully sent all events to Kafka")
        
    except Exception as e:
        print(f"Failed to connect to Kafka at {bootstrap_servers}: {str(e)}")
        print("This is expected if Kafka is not available in the deployment environment")
        # You could implement alternative logic here (e.g., write to file, database, etc.)

if __name__ == "__main__":
    create_kafka_stream()