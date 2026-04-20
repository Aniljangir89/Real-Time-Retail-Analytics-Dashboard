from azure.eventhub import EventHubProducerClient, EventData
from azure.identity import DefaultAzureCredential
import json
import time
from retail_app import generate_event

fully_qualified_namespace = "anils-namespace.servicebus.windows.net"
eventhub_name = "retail-event"

credential = DefaultAzureCredential()

producer = EventHubProducerClient(
    fully_qualified_namespace=fully_qualified_namespace,
    eventhub_name=eventhub_name,
    credential=credential
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