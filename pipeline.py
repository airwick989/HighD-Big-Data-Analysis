import argparse
import logging
import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.options.pipeline_options import SetupOptions
from google.cloud import bigquery, pubsub_v1
from google.cloud.bigquery import SchemaField

#fields = ["frame", "id", "x", "y", "width", "height", "xVelocity", "yVelocity", "xAcceleration", "yAcceleration", "frontSightDistance", "backSightDistance", "dhw", "thw", "ttc", "precedingXVelocity", "precedingId", "followingId", "leftPrecedingId", "leftAlongsideId", "leftFollowingId", "rightPrecedingId", "rightAlongsideId", "rightFollowingId", "laneId", "class"]
fields = ['class', 'id']

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
            WHERE numLaneChanges > 0
            AND minTTC < 5 AND minTTC != -1
            AND minDHW < 10 AND minDHW != -1;
        """
        query_job = client.query(table_query)  # Execute the query
        results = query_job.result()  # Wait for the query to complete and get the results
        tables[table] = [row for row in results]  # Store the results in the dictionary
    return tables

# def publish_to_pubsub(table_name, vehicle_ids):
#     publisher = pubsub_v1.PublisherClient()
#     topic_path = publisher.topic_path('cloud-project-418817', 'vehicleIDs')
#     data = f"{table_name}:{vehicle_ids}".encode("utf-8")
#     future = publisher.publish(topic_path, data)
#     future.result()
#     logging.info(f"Published {vehicle_ids} to Pub/Sub topic vehicleIDs")

def create_and_populate_table(dataset_id, table_id, schema, data):
    # Initialize a BigQuery client
    client = bigquery.Client()

    # Define the table reference
    table_ref = client.dataset(dataset_id).table(table_id)

    # Define the table schema
    table = bigquery.Table(table_ref, schema=schema)

    # Create the table
    client.create_table(table)

    print(f"Table {table_id} created in dataset {dataset_id}")

    # Insert rows into the table
    rows_to_insert = []
    for row in data:
        # Construct a tuple with values corresponding to the schema fields
        row_values = tuple(row[field] for field in fields)
        rows_to_insert.append(row_values)

    try:
        client.insert_rows(table, rows_to_insert)
        print(f"{len(data)} rows inserted into table {table_id}")
    except Exception:
        print(f"No rows present to be inserted into table {table_id}")

# def filterTrack(table_name, data):
#     ids = tuple(data['cars'] + data['trucks'])

#     client = bigquery.Client()
#     table_query = f"""
#         SELECT *
#         FROM `cloud-project-418817.highd_tracks.{table_name}`
#         WHERE id IN {ids};
#     """
#     query_job = client.query(table_query)  # Execute the query
#     results = query_job.result()  # Wait for the query to complete and get the results
#     table = [row for row in results]  # Store the results in the dictionary

#     tabledata = []
#     for row in table:
#         id = row[1]
#         if id in data['cars']:
#             vehicle_type = "Car"
#         else:
#             vehicle_type = "Truck"

#         tabledata.append(
#             {
#                 fields[0]: row[0],
#                 fields[1]: row[1],
#                 fields[2]: row[2],
#                 fields[3]: row[3],
#                 fields[4]: row[4],
#                 fields[5]: row[5],
#                 fields[6]: row[6],
#                 fields[7]: row[7],
#                 fields[8]: row[8],
#                 fields[9]: row[9],
#                 fields[10]: row[10],
#                 fields[11]: row[11],
#                 fields[12]: row[12],
#                 fields[13]: row[13],
#                 fields[14]: row[14],
#                 fields[15]: row[15],
#                 fields[16]: row[16],
#                 fields[17]: row[17],
#                 fields[18]: row[18],
#                 fields[19]: row[19],
#                 fields[20]: row[20],
#                 fields[21]: row[21],
#                 fields[22]: row[22],
#                 fields[23]: row[23],
#                 fields[24]: row[24],
#                 fields[25]: vehicle_type,
#             }
#         )

#     return tabledata


def tabulateIDs(table_name, data):
    tabledata = []

    for vehicle_type in data:
        if vehicle_type == "cars":
            for id in data[vehicle_type]:
                tabledata.append(
                    {
                        fields[0]: "Car",
                        fields[1]: id,
                    }
                )
        else:
            for id in data[vehicle_type]:
                tabledata.append(
                    {
                        fields[0]: "Truck",
                        fields[1]: id,
                    }
                )

    return tabledata



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
        
        # for table_name, vehicle_ids in vehicle_ids.items():
        #     publish_to_pubsub(table_name, vehicle_ids)
            
        for table_name, vehicles in vehicle_ids.items():
            dataset_id = "risky_ids"
            table_name = table_name.replace("Meta", "")
            table_id = f"{table_name}_filtered"
            # schema = [
            #     SchemaField(fields[0], "INTEGER"),
            #     SchemaField(fields[1], "INTEGER"),
            #     SchemaField(fields[2], "FLOAT"),
            #     SchemaField(fields[3], "FLOAT"),
            #     SchemaField(fields[4], "FLOAT"),
            #     SchemaField(fields[5], "FLOAT"),
            #     SchemaField(fields[6], "FLOAT"),
            #     SchemaField(fields[7], "FLOAT"),
            #     SchemaField(fields[8], "FLOAT"),
            #     SchemaField(fields[9], "FLOAT"),
            #     SchemaField(fields[10], "FLOAT"),
            #     SchemaField(fields[11], "FLOAT"),
            #     SchemaField(fields[12], "FLOAT"),
            #     SchemaField(fields[13], "FLOAT"),
            #     SchemaField(fields[14], "FLOAT"),
            #     SchemaField(fields[15], "FLOAT"),
            #     SchemaField(fields[16], "INTEGER"),
            #     SchemaField(fields[17], "INTEGER"),
            #     SchemaField(fields[18], "INTEGER"),
            #     SchemaField(fields[19], "INTEGER"),
            #     SchemaField(fields[20], "INTEGER"),
            #     SchemaField(fields[21], "INTEGER"),
            #     SchemaField(fields[22], "INTEGER"),
            #     SchemaField(fields[23], "INTEGER"),
            #     SchemaField(fields[24], "INTEGER"),
            #     SchemaField(fields[25], "STRING"),
            # ]

            schema = [
                SchemaField(fields[0], "STRING"),
                SchemaField(fields[1], "INTEGER"),
            ]

            table = tabulateIDs(table_name, vehicles)
            create_and_populate_table(dataset_id, table_id, schema, table)



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
