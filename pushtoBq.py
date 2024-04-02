import subprocess
import os
import re

# Set Variables for bucket and BQ
bucket = 'highd-dataset-unfiltered'
project_id = 'cloud-project-418817'

# Dictionary to map matched file types to their corresponding BigQuery datasets
dataset_mapping = {
    'tracks': 'highd_tracks',
    'recordingMeta': 'highd_recording_meta',
    'tracksMeta': 'highd_tracks_meta'
}

# List all csv files in bucket
cmd = f'gsutil ls gs://{bucket}/*.csv'

try:
    # List CSV files split get path
    path_to_csv = subprocess.check_output(cmd, shell=True, text=True).splitlines()

    # Combined regex to match different file types
    pattern = re.compile(r'^.*/([A-Za-z0-9_]+)_(tracks|recordingMeta|tracksMeta)\.csv$')

    for file_path in path_to_csv:
        match = pattern.match(file_path)
        if match:
            # Table name value so 01-60
            table_name = match.group(1)

            # File type tracks, recordingMeta, tacksMeta 
            file_type = match.group(2)  
            

            # Dataset Name
            bigquery_dataset = dataset_mapping.get(file_type)

            if bigquery_dataset:
                # Construct the BQ load command
                cmd_load = (
                    f'bq load --source_format=CSV --autodetect '
                    f'--field_delimiter=, '
                    f'--project_id={project_id} '
                    f'{project_id}:{bigquery_dataset}.{table_name}_{file_type} '
                    f'{file_path}'
                )
                # Load csv into BQ Dataset
                subprocess.run(cmd_load, shell=True, check=True)
                print(f"Loaded {file_path} into BigQuery table {bigquery_dataset}.{table_name}_{file_type}")
            else:
                print(f"Unsupported file type for {file_path}")

except subprocess.CalledProcessError as e:
    print(f"Failed to execute command: {e}")