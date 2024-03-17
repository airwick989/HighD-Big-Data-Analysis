from lane_change_classifier import classifyLaneChanges
import os

dir = "./highd-dataset-v1.0/data"

for file in os.listdir(dir):
    if file.split("_")[1] == "tracks.csv":
        path = f"{dir}/{file}"
        print(f"Number of lane changes in {file}:")
        print(f"{classifyLaneChanges(path)}\n")