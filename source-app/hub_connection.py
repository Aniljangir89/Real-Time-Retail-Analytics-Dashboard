

from azure.eventhub import EventHubProducerClient, EventData
import json
import time
from retail_app import generate_event

conn_str = "Endpoint=sb://eventhub-de-training-2026.servicebus.windows.net/;SharedAccessKeyName=RootManageSharedAccessKey;SharedAccessKey=IX2Befhki1ech55ZC1TrnpQ4EbyaB9B6D+AEhIkgnvE="
eventhub_name = "retail-events"

producer = EventHubProducerClient.from_connection_string(
    conn_str=conn_str,
    eventhub_name=eventhub_name
)

while True:
    event = generate_event()
    
    try:
        batch = producer.create_batch()
        batch.add(EventData(json.dumps(event)))
        producer.send_batch(batch)

        print("✅ SENT:", event)   # 👈 THIS IS IMPORTANT
    except Exception as e:
        print("❌ ERROR:", e)

    time.sleep(2)