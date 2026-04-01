import random
from faker import Faker
from datetime import datetime

fake = Faker()

def generate_event():
    return {
        "event_id": fake.uuid4(),
        "user_id": fake.random_int(1, 1000),
        "session_id": fake.uuid4(),
        "event_type": random.choice(["search", "click", "cart", "checkout", "purchase"]),
        "product_id": fake.random_int(100, 200),
        "category": random.choice(["electronics", "fashion"]),
        "timestamp": datetime.utcnow().isoformat(),
        "region": "India",
        "device_type": random.choice(["mobile", "web"]),
        "app_version": "1.0.0"
    }