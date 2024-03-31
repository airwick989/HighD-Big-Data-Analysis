import matplotlib.pyplot as plt
import matplotlib.animation as animation
from matplotlib.animation import FFMpegWriter
import pandas as pd
from PIL import Image
import matplotlib.image as mpimg
from matplotlib.offsetbox import OffsetImage, AnnotationBbox
import numpy as np

# Load the tracks and recording meta data
tracks_file = pd.read_csv('../highd-dataset-v1.0/data/01_tracks.csv')
recording_meta_file = pd.read_csv('../highd-dataset-v1.0/data/01_recordingMeta.csv')
tracks_meta = pd.read_csv('../highd-dataset-v1.0/data/01_tracksMeta.csv')

# Load the highway image
highway_image_path = '../highd-dataset-v1.0/data/01_highway.png'
highway_image = mpimg.imread(highway_image_path)

# Retrieve the height (y) location of each lane line
# Right to left
west_bound_lane_lines = list(map(float, recording_meta_file['upperLaneMarkings'].iloc[0].split(';')))
# Left to right
east_bound_lane_lines = list(map(float, recording_meta_file['lowerLaneMarkings'].iloc[0].split(';')))

# Define highway length (may need to be adjusted to match your dataset)
highway_length = 420

# Print the lanes in drawings
def plot_lanes(axis, west_bound_lane_lines, east_bound_lane_lines, highway_length):
    # Lane lines for each lane
    for lane_line in west_bound_lane_lines:
        axis.plot([0, highway_length], [lane_line] * 2, color='white', linestyle='-', linewidth=1)
    for lane_line in east_bound_lane_lines:
        axis.plot([0, highway_length], [lane_line] * 2, color='white', linestyle='-', linewidth=1)

# Update function for the animation
def update_plot(car_frame_number, tracks_file, axis, fig, highway_image, west_bound_lane_lines, east_bound_lane_lines, tracks_meta):
    axis.clear()
    axis.set_aspect('equal')
    
    # Set the highway image as the background with proper aspect ratio
    highway_extent = [0, highway_length, min(west_bound_lane_lines) - 8, max(east_bound_lane_lines) + 8]
    axis.imshow(highway_image, extent=highway_extent, aspect='auto')
    
    # Optionally, set a tighter y-limits to zoom in on the highway
    axis.set_ylim(min(west_bound_lane_lines) - 5, max(east_bound_lane_lines) + 5)
    
    current_frame = tracks_file[tracks_file['frame'] == car_frame_number]
    
    if isinstance(current_frame, pd.Series):
        current_frame = pd.DataFrame(current_frame).transpose()
        
    for index, vehicle in current_frame.iterrows():
        vehicle_id = vehicle['id']
        vehicle_width = vehicle['width']
        # Ensure that the height of the vehicles is visible by setting a minimum height
        vehicle_height = max(vehicle['height'], 1)  # Adjust the minimum height as needed
        vehicle_type = tracks_meta[tracks_meta['id'] == vehicle_id]['class'].values[0]
        # Adjust the y-coordinate to center the vehicle in its lane
        vehicle_center_y = vehicle['y'] - vehicle_height / 2
        
        # Vehicle color, border color, and transparency can be adjusted as needed

        if vehicle_type == 'Car':
            vehicle_color = 'red'
        else:
            vehicle_color = 'blue'
          # Change as needed
        vehicle_edge_color = 'black'  # Border color
        vehicle_alpha = 0.7  # Transparency
        
        # Create the rectangle representing the vehicle
        vehicle_plot = plt.Rectangle((vehicle['x'], vehicle_center_y), vehicle_width, vehicle_height,
                                     color=vehicle_color, ec=vehicle_edge_color,
                                     alpha=vehicle_alpha)
        axis.add_patch(vehicle_plot)

        # Define the label text including vehicle ID and xVelocity (in km/h)
        label_text = f"{int(vehicle['xVelocity'])}km/h | ID:{int(vehicle['id'])}"
        
        # Adjust the text position to be within the vehicle rectangle
        text_x_position = vehicle['x']
        text_y_position = vehicle['y'] + vehicle_height / 2 + 1
        
        # Use bbox to set the background color of the text for better visibility
        bbox_props = dict(boxstyle="round,pad=0.3", ec="black", lw=1, fc="azure")
        
        # Add the label text for each vehicle
        axis.text(text_x_position, text_y_position, label_text, color='black', fontsize=24, ha='center', va='center', bbox=bbox_props)
        
        # Load the overlay image
        garbage_image = mpimg.imread('garbage.png')
        lightning_image = mpimg.imread('lightning.png')
        reversed_lightning_image = np.flip(lightning_image, axis=1)
        reversed_garbage_image = np.flip(garbage_image, axis=1)
        
        # Create an annotation box for overlay image
        
        if vehicle_type == 'Car':
            imagebox = OffsetImage(lightning_image, zoom=0.25)
            if vehicle['xVelocity'] < 0:
                imagebox = OffsetImage(reversed_lightning_image, zoom=0.25)
            
        else:
            imagebox = OffsetImage(reversed_garbage_image, zoom=1)
            if vehicle['xVelocity'] < 0:
                imagebox = OffsetImage(garbage_image, zoom=1)

        ab = AnnotationBbox(imagebox, (vehicle['x'], vehicle_center_y), frameon=False)
        axis.add_artist(ab)


# Create the figure and axis for the animation
fig, axis = plt.subplots(figsize=(46, 10))
axis.set_aspect('equal')  # Set the aspect ratio to be equal

# Create the animation
ani = animation.FuncAnimation(fig, update_plot, frames=sorted(tracks_file['frame'].unique()),
                              fargs=(tracks_file, axis, fig, highway_image, west_bound_lane_lines, east_bound_lane_lines, tracks_meta),
                              interval=100)

# Set up the writer for saving the file
writer = FFMpegWriter(fps=75, metadata=dict(artist='Me'), bitrate=1800, codec='libx264')

# Save the animation
ani.save('group9_animation.mp4', writer=writer, dpi=100)  # Specify the DPI if needed

# Show the result
plt.show()