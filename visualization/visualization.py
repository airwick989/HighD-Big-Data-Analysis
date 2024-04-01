import matplotlib.pyplot as plt
import matplotlib.animation as animation
from matplotlib.animation import FFMpegWriter
import pandas as pd
import matplotlib.image as mpimg
from matplotlib.offsetbox import OffsetImage, AnnotationBbox
import numpy as np

class HighwayAnimation:
    def __init__(self, base_path, recording_number, special_car_ids):
        self.base_path = base_path
        self.recording_number = recording_number
        self.special_car_ids = special_car_ids
        self.load_data()
        self.highway_image = None
        self.west_bound_lane_lines = None
        self.east_bound_lane_lines = None
        self.highway_length = 420
        self.fig, self.axis = plt.subplots(figsize=(64, 16))
        
        # Load the highway image and lane line data
        self.load_highway_image()
        self.load_lane_lines()

    def load_data(self):
        # Original loading of data
        tracks_file_path = f'{self.base_path}/data/{self.recording_number:02}_tracks.csv'
        recording_meta_file_path = f'{self.base_path}/data/{self.recording_number:02}_recordingMeta.csv'
        tracks_meta_path = f'{self.base_path}/data/{self.recording_number:02}_tracksMeta.csv'
        
        all_tracks = pd.read_csv(tracks_file_path)
        self.recording_meta_file = pd.read_csv(recording_meta_file_path)
        self.tracks_meta = pd.read_csv(tracks_meta_path)
        
        # Get frames for special car IDs
        special_frames = all_tracks[all_tracks['id'].isin(self.special_car_ids)]
        
        # Find neighboring car IDs for each special car ID in its frames
        neighboring_ids = set()
        for special_id in self.special_car_ids:
            # Get the unique IDs in the same frames as the special car
            same_frame_ids = all_tracks[all_tracks['frame'].isin(special_frames[special_frames['id'] == special_id]['frame'])]['id'].unique()
            # Find the immediate neighbors of the special car ID
            special_id_index = np.searchsorted(same_frame_ids, special_id)
            if special_id_index > 0:
                neighboring_ids.add(same_frame_ids[special_id_index - 1])  # Previous ID
            if special_id_index < len(same_frame_ids) - 1:
                neighboring_ids.add(same_frame_ids[special_id_index + 1])  # Next ID
        
        # Filter tracks to include only frames from the special car IDs and their neighbors
        all_relevant_ids = set(self.special_car_ids).union(neighboring_ids)
        self.tracks_file = all_tracks[all_tracks['id'].isin(all_relevant_ids)]

    def load_highway_image(self):
        highway_image_path = f'{self.base_path}/data/{self.recording_number:02}_highway.png'
        self.highway_image = mpimg.imread(highway_image_path)
        
    def load_lane_lines(self):
        self.west_bound_lane_lines = list(map(float, self.recording_meta_file['upperLaneMarkings'].iloc[0].split(';')))
        self.east_bound_lane_lines = list(map(float, self.recording_meta_file['lowerLaneMarkings'].iloc[0].split(';')))

    def update_plot(self, car_frame_number):
        axis = self.axis
        fig = self.fig
        highway_image = self.highway_image
        west_bound_lane_lines = self.west_bound_lane_lines
        east_bound_lane_lines = self.east_bound_lane_lines
        tracks_meta = self.tracks_meta
        tracks_file = self.tracks_file

        axis.clear()
        axis.set_aspect('equal')
        highway_extent = [0, self.highway_length, min(west_bound_lane_lines) - 10, max(east_bound_lane_lines) + 8]
        axis.imshow(highway_image, extent=highway_extent, aspect='auto')
        axis.set_ylim(min(west_bound_lane_lines) - 5, max(east_bound_lane_lines) + 5)

        current_frame = tracks_file[tracks_file['frame'] == car_frame_number]
        if isinstance(current_frame, pd.Series):
            current_frame = pd.DataFrame(current_frame).transpose()
        
        for index, vehicle in current_frame.iterrows():
            vehicle_id = vehicle['id']
            vehicle_width = vehicle['width']
            vehicle_height = max(vehicle['height'], 1)
            vehicle_type = tracks_meta[tracks_meta['id'] == vehicle_id]['class'].values[0]
            vehicle_center_y = vehicle['y'] - vehicle_height / 2

            if vehicle_id in self.special_car_ids:
                vehicle_color = 'red'  # Special color for certain cars
            elif vehicle_type == 'Car':
                vehicle_color = 'green'
            else:
                vehicle_color = 'blue'
            
            vehicle_edge_color = 'black'
            vehicle_alpha = 1.0  # Change transparency as needed

            vehicle_plot = plt.Rectangle((vehicle['x'], vehicle_center_y), vehicle_width, vehicle_height,
                                         color=vehicle_color, ec=vehicle_edge_color, alpha=vehicle_alpha)
            axis.add_patch(vehicle_plot)

            label_text = f"{int(vehicle['xVelocity'])}km/h | ID:{int(vehicle_id)}"
            text_x_position = vehicle['x']
            text_y_position = vehicle['y'] + vehicle_height / 2 + 1
            bbox_props = dict(boxstyle="round,pad=0.3", ec="black", lw=1, fc="white")
            axis.text(text_x_position, text_y_position, label_text, fontsize=32, ha='center', bbox=bbox_props)

            # Assume the overlay images are present in the specified paths
            # You will need to add your logic for which image to use and when to flip it
            # ...

    def create_animation(self):
        ani = animation.FuncAnimation(self.fig, self.update_plot, frames=sorted(self.tracks_file['frame'].unique()),
                                      interval=100)
        writer = FFMpegWriter(fps=24, metadata=dict(artist='Me'), bitrate=1800, codec='libx264')
        ani.save(f'group{self.recording_number}_animation.mp4', writer=writer, dpi=100)

# Example usage:
highway_animation = HighwayAnimation(base_path='../highd-dataset-v1.0', recording_number=1, special_car_ids=[977,732,972,701,484,11,79])
highway_animation.create_animation()
plt.show()