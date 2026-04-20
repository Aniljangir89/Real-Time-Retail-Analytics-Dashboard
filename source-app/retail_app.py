import random
from faker import Faker
from datetime import datetime

fake = Faker()

def generate_event():

    event_type = "search"
    if random.random() < 0.6:   # 60% click after search
        event_type = "click"

        if random.random() < 0.5:  # 50% go to cart
            event_type = "cart"

            if random.random() < 0.4:  # 40% checkout
                event_type = "checkout"

                if random.random() < 0.5:  # 50% purchase
                    event_type = "purchase"

    event = {
        "event_id": fake.uuid4(),
        "user_id": fake.random_int(1, 1000),
        "session_id": fake.uuid4(),
        "event_type": event_type,
        "product_id": fake.random_int(100, 200),
        "category": random.choice(["electronics", "fashion"]),
        "timestamp": datetime.utcnow().isoformat(),
        "region": "India",
        "device_type": random.choice(["mobile", "web"]),
        "app_version": "1.0.0"
    }

    if event_type == "click":
        event["price"] = random.randint(100, 1000)

    elif event_type == "cart":
        event["quantity"] = random.randint(1, 5)

    elif event_type == "checkout":
        event["cart_total"] = random.randint(500, 5000)

    elif event_type == "purchase":
        event["amount"] = random.randint(500, 5000)
        event["payment_method"] = random.choice(["card", "upi"])

    return event