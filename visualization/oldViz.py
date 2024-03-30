import matplotlib.pyplot as plt
import matplotlib.animation as animation
from matplotlib.animation import FFMpegWriter
import pandas as pd
from PIL import Image
import matplotlib.image as mpimg

# Load the tracks and recording meta data
tracks_file = pd.read_csv('./data/01_tracks.csv')
recording_meta_file = pd.read_csv('./data/01_recordingMeta.csv')

# Load the highway image
highway_image_path = './data/01_highway.jpeg'
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
def update_plot(car_frame_number, tracks_file, axis, fig, highway_image, west_bound_lane_lines, east_bound_lane_lines):
    axis.clear()
    axis.set_aspect('equal')
    
    # Set the highway image as the background
    axis.imshow(highway_image, extent=[0, highway_length, min(west_bound_lane_lines), max(east_bound_lane_lines)])
    
    plot_lanes(axis, west_bound_lane_lines, east_bound_lane_lines, highway_length)
    
    current_frame = tracks_file[tracks_file['frame'] == car_frame_number]
    
    if isinstance(current_frame, pd.Series):
        current_frame = pd.DataFrame(current_frame).transpose()
        
    for index, vehicle in current_frame.iterrows():
        width = vehicle['width']
        height = vehicle['height']
        
        vehicle_color = 'cyan'  # Change as needed
        vehicle_edge_color = 'black'  # Border color
        vehicle_alpha = 0.7  # Transparency
        
        vehicle_plot = plt.Rectangle((vehicle['x'], vehicle['y']), width, height,
                                     color=vehicle_color, ec=vehicle_edge_color,
                                     alpha=vehicle_alpha)
        axis.add_patch(vehicle_plot)

# Create the figure and axis for the animation
fig, axis = plt.subplots(figsize=(30, 16))
axis.set_aspect('equal')  # Set the aspect ratio to be equal

# Create the animation
ani = animation.FuncAnimation(fig, update_plot, frames=sorted(tracks_file['frame'].unique()),
                              fargs=(tracks_file, axis, fig, highway_image, west_bound_lane_lines, east_bound_lane_lines),
                              interval=100)

# Set up the writer for saving the file
writer = FFMpegWriter(fps=10, metadata=dict(artist='Me'), bitrate=1800)

# Save the animation
ani.save('group6_animation2.mp4', writer=writer, dpi=100)  # Specify the DPI if needed

# Show the result
plt.show()









