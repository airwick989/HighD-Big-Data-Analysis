import pandas as pd

data_path = "./highd-dataset-v1.0/data/01_tracks.csv"
df = pd.read_csv(data_path)
groups = df.groupby('frame')

# for frame, group in groups:
#     if frame in [163, 164]:
#         print(group)    

#163 -> 164
#id 11 changes from lane 6 to 5
#moves right and down
        
#lanes 1-3 are going left, 4-6 are going right
#lane ordering is:
"""
(moving right)
6
5
4
--------------
(moving left)
3
2
1
"""      
#Lanes 1-3, laneId increase means right lane change, laneId decrease means left lane change
#Lanes 4-6, laneId increase means left lane change, laneId decrease means right lane change




for frame, group in groups:
    prev = dict(zip(group['id'].tolist(), group['laneId'].tolist()))
    break

lane_changes = {
    'left': 0,
    'right': 0
}

for frame, group in groups:
    curr = dict(zip(group['id'].tolist(), group['laneId'].tolist()))

    for vehicle in curr:
        if vehicle in prev:
            if curr[vehicle] in [1,2,3]:
                if curr[vehicle] > prev[vehicle]:
                    lane_changes['right'] += 1
                elif curr[vehicle] < prev[vehicle]:
                    lane_changes['left'] += 1
            elif curr[vehicle] in [4,5,6]:
                if curr[vehicle] > prev[vehicle]:
                    lane_changes['left'] += 1
                elif curr[vehicle] < prev[vehicle]:
                    lane_changes['right'] += 1
            prev[vehicle] = curr[vehicle]
        else:
            prev[vehicle] = curr[vehicle]

print(lane_changes)