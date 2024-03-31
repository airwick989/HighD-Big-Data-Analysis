import argparse
import logging
import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.options.pipeline_options import SetupOptions
from google.cloud import bigquery, pubsub_v1

def getTableNames():
    client = bigquery.Client()
    tables_query = """
        SELECT table_name
        FROM `cloud-project-418817.highd_tracks_meta.INFORMATION_SCHEMA.TABLES`
    """
    rows = client.query(tables_query)
    tablenames = [row[0] for row in rows]
    return tablenames

def queryTables(tablenames):
    client = bigquery.Client()
    tables = {}
    for table in tablenames:
        table_query = f"""
            SELECT *
            FROM `cloud-project-418817.highd_tracks_meta.{table}`
            WHERE numLaneChanges = 1
            AND minTTC < 5 AND minTTC != -1
            AND minDHW < 10 AND minDHW != -1;
        """
        query_job = client.query(table_query)  # Execute the query
        results = query_job.result()  # Wait for the query to complete and get the results
        tables[table] = [row for row in results]  # Store the results in the dictionary
    return tables

# def publish_to_pubsub(vehicle_id, table_name, topic_name):
#     publisher = pubsub_v1.PublisherClient()
#     topic_path = publisher.topic_path('cloud-project-418817', topic_name)
#     data = f"{table_name}:{vehicle_id}".encode("utf-8")
#     future = publisher.publish(topic_path, data)
#     future.result()
#     logging.info(f"Published {vehicle_id} to Pub/Sub topic {topic_name}")

def publish_to_pubsub(table_name, vehicle_ids):
    publisher = pubsub_v1.PublisherClient()
    topic_path = publisher.topic_path('cloud-project-418817', 'vehicleIDs')
    data = f"{table_name}:{vehicle_ids}".encode("utf-8")
    future = publisher.publish(topic_path, data)
    future.result()
    logging.info(f"Published {vehicle_ids} to Pub/Sub topic vehicleIDs")

def run(argv=None):
    parser = argparse.ArgumentParser(formatter_class=argparse.ArgumentDefaultsHelpFormatter)
    known_args, pipeline_args = parser.parse_known_args(argv)
    pipeline_options = PipelineOptions(pipeline_args)
    pipeline_options.view_as(SetupOptions).save_main_session = True

    with beam.Pipeline(options=pipeline_options) as p:
        # Get table names
        table_names = getTableNames()

        # Query tables and store the results in a dictionary
        tables = queryTables(table_names)
   
        #Obtain vehicle ids and class
        vehicle_ids = {}
        for table in tables:
            cars = []
            trucks = []
            for vehicle in tables[table]:
                #Get vehicle class (at index 6)
                if vehicle[6] == 'Car':
                    cars.append(vehicle[0]) #id is at index 0
                else:
                    trucks.append(vehicle[0])
            vehicle_ids[table] = {
                'cars': cars,
                'trucks': trucks
            }
        
        # for table_name, vehicles in vehicle_ids.items():
        #     for vehicle_type, ids in vehicles.items():
        #         for vehicle_id in ids:
        #             topic_name = "carIDs" if vehicle_type == "cars" else "truckIDs"
        #             publish_to_pubsub(vehicle_id, table_name, topic_name)
        for table_name, vehicle_ids in vehicle_ids.items():
            publish_to_pubsub(table_name, vehicle_ids)



if __name__ == '__main__':
    logging.getLogger().setLevel(logging.INFO)
    run()





#Run command
'''
python3 data_filter.py \
  --runner DataflowRunner \
  --project cloud-project-418817 \
  --staging_location gs://highd-dataset-filtered/staging \
  --temp_location gs://highd-dataset-filtered/temp \
  --region northamerica-northeast2 \
  --experiment use_unsupported_python_version \
'''
