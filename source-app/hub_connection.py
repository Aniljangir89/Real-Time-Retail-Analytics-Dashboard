
from azure.eventhub import EventHubProducerClient, EventData
import json
import time
from retail_app import generate_event

conn_str = "Endpoint=sb://anils-namespace.servicebus.windows.net/;SharedAccessKeyName=anils;SharedAccessKey=989fDv37Hl+QhT+eYefLIw2QW1Q/Z85UP+AEhG64fB8=;EntityPath=retail-event"

producer = EventHubProducerClient.from_connection_string(
    conn_str=conn_str,
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