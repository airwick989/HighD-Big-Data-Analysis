import os
import json
from google.cloud import pubsub_v1
from video_generator import HighwayAnimation
from google.oauth2 import service_account

# Path to service account key file
service_account_file = 'cred.json'

# Loads the credentials from the service account file
credentials = service_account.Credentials.from_service_account_file(
    service_account_file,
)

# Creates a Pub/Sub subscriber client with the specified credentials
consumer = pubsub_v1.SubscriberClient(credentials=credentials)

subscription_path = 'projects/cloud-project-418817/subscriptions/vehicleIDs-sub'

def readMessage(message):
    print(f"Received message: {message.data}")
    data = json.loads(message.data)  # Decodes and loads the JSON data
    
    # Extracts the table number and list of vehicle IDs
    table_number, vehicle_ids = next(iter(data.items()))

    # Creates animations for each vehicle ID
    for vehicle_id in vehicle_ids:
        print(f"Creating animation for vehicle ID: {vehicle_id} in table {table_number}")
        # Initialize the animation with the base path, table number, and vehicle ID
        highway_animation = HighwayAnimation(
            base_path='../highd-dataset-v1.0',
            recording_number=int(table_number),
            special_car_ids=[vehicle_id]
        )
        # Creates and saves the animation        
        highway_animation.create_animation()

    message.ack()  # Acknowledges that the message has been processed

def main():
    print(f"Listening for messages on {subscription_path}...")
    streaming_pull_future = consumer.subscribe(subscription_path, callback=readMessage)
    
    with consumer:
        try:
            streaming_pull_future.result()
        except KeyboardInterrupt:
            streaming_pull_future.cancel()

if __name__ == '__main__':
    main()