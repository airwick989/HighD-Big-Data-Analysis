import json
from google.cloud import bigquery, pubsub_v1
import logging

#Query Filtered Data results
def queryTable(table_number):
    client = bigquery.Client()
    table_name = f"{table_number:02d}_tracks_filtered"
    table_query = f"""
        SELECT id
        FROM `cloud-project-418817.high_risk_ids.{table_name}`;
    """
    query_job = client.query(table_query)
    results = query_job.result()
    ids = [row['id'] for row in results]
    return ids

# Convert the list of IDs to a JSON string with the table number as the key
def publish_to_pubsub(table_number, vehicle_ids):
    publisher = pubsub_v1.PublisherClient()
    topic_path = publisher.topic_path('cloud-project-418817', 'vehicleIDs') 
    data = json.dumps({str(table_number): vehicle_ids}).encode("utf-8")
    print(data)
    future = publisher.publish(topic_path, data)
    future.result()
    logging.info(f"Published vehicle IDs from table {table_number} to topic vehicleIDs")

def main():
    logging.getLogger().setLevel(logging.INFO)
    try:
        table_number = int(input("Enter the table number you wish to publish (1-60): "))
        if 1 <= table_number <= 60:
            vehicle_ids = queryTable(table_number)
            publish_to_pubsub(table_number, vehicle_ids)
        else:
            print("Please enter a number between 1 and 60.")
    except ValueError:
        print("Invalid input. Please enter a number.")
    except Exception as e:
        logging.error(f"An error occurred: {e}")

if __name__ == '__main__':
    main()