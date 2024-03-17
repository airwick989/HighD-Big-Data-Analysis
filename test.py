import pandas as pd

data_path = "./highd-dataset-v1.0/data/01_tracks.csv"
df = pd.read_csv(data_path)
print(df)