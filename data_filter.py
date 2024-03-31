import pandas as pd
import os

DATA_DIRECTORY = "./highd-dataset-v1.0/data"
MIN_TCC = 5
MIN_DHW = 10

def filterByLaneChanges(df):
    filtered_df = df[df['numLaneChanges'] == 1]
    return filtered_df

def filterByMinTTC(df):
    df = df[df['minTTC'] != -1]
    filtered_df = df[df['minTTC'] < MIN_TCC]
    return filtered_df

def filterByMinDHW(df):
    df = df[df['minDHW'] != -1]
    filtered_df = df[df['minDHW'] < MIN_DHW]
    return filtered_df

def filterTracksMeta(path):
    tracksMeta = pd.read_csv(path)
    tracksMeta = filterByLaneChanges(tracksMeta)
    tracksMeta = filterByMinTTC(tracksMeta)
    tracksMeta = filterByMinDHW(tracksMeta)




for filename in os.listdir(DATA_DIRECTORY):
    if "tracksMeta.csv" in filename:
        refNum = filename[0:2]
        print(refNum)
        tracks_meta_path = f"{DATA_DIRECTORY}/{filename}"
        tracksMeta = filterTracksMeta(tracks_meta_path)
        
        if f"{refNum}_tracks.csv" in os.listdir(DATA_DIRECTORY):
            tracks_path = f"{DATA_DIRECTORY}/{refNum}_tracks.csv"

