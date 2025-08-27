import os
import numpy as np
import matplotlib.pyplot as plt
import matplotlib.animation as animation
from osgeo import gdal

# Path to the multiband GeoTIFF file
file_path = 'Python/holocene_climate/data/CHELSA_TraCE21k_UK_V1.0_bio12_20250826T154526.tif'

# Open the dataset
dataset = gdal.Open(file_path)

# Get raster dimensions and number of bands
cols = dataset.RasterXSize
rows = dataset.RasterYSize
bands = dataset.RasterCount

# Get global min and max values across all bands for consistent color scaling
global_min = float('inf')
global_max = float('-inf')
for i in range(1, bands + 1):
    band = dataset.GetRasterBand(i)
    data = band.ReadAsArray()
    global_min = min(global_min, np.nanmin(data))
    global_max = max(global_max, np.nanmax(data))

# Create a figure for animation
fig, ax = plt.subplots(figsize=(8, 6))

# Function to update each frame
def update(frame):
    ax.clear()
    band = dataset.GetRasterBand(frame + 1)
    data = band.ReadAsArray()
    timestamp = band.GetDescription()
    im = ax.imshow(data, cmap='viridis', vmin=global_min, vmax=global_max)
    ax.set_title(f'Precipitation - {timestamp}')
    ax.axis('off')
    return [im]

# Create animation
ani = animation.FuncAnimation(fig, update, frames=bands, blit=False)

# Save animation as GIF and MP4
ani.save('precipitation_timeseries.gif', writer='pillow', fps=5)
ani.save('precipitation_timeseries.mp4', writer='ffmpeg', fps=5)

# Close the dataset
dataset = None
