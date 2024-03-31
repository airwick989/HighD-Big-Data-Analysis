import os
from google.cloud import pubsub_v1

subscription = 'projects/cloud-project-418817/subscriptions/vehicleIDs-sub'.format(
    project_id=os.getenv('GOOGLE_CLOUD_PROJECT'),
    sub='vehicleIDs-sub', 
)  
consumer = pubsub_v1.SubscriberClient()

def readMessage(message):
    print(message.data)
    message.ack()

message_stream = consumer.subscribe(subscription, readMessage)

try:
    message_stream.result()
except KeyboardInterrupt:
    message_stream.cancel()