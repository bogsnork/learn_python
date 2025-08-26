import os
import datetime
import rasterio
from rasterio.vrt import WarpedVRT
from rasterio.windows import from_bounds
from rasterio.enums import Resampling
from rasterio.shutil import copy as rio_copy
from rasterio.io import MemoryFile
import numpy as np
import csv

# Set PROJ_LIB (seems to be a recurring issue on some systems)
try:
    import pyproj
    os.environ["PROJ_LIB"] = pyproj.datadir.get_data_dir()
except ImportError:
    pass

# Variables that have a 'month' component in their filename
MONTHLY_VARS = {"pr", "tasmax", "tasmin"}

def filter_urls_from_csv(csv_rows, variable=None, month_range=None, timeID_range=None):
    """
    Filter CSV rows by variable, month, and timeID ranges.
    Returns a list of tuples: (url, var, month, timeID)
    """
    filtered = []
    for row in csv_rows:
        var = row['variable']
        url = row['url']
        # For monthly variables, filter by month and timeID
        if var in MONTHLY_VARS:
            month = int(row['month'])
            timeID = int(row['timeID'])
            if variable and var != variable:
                continue
            if month_range and not (month_range[0] <= month <= month_range[1]):
                continue
            if timeID_range and not (timeID_range[0] <= timeID <= timeID_range[1]):
                continue
            filtered.append((url, var, month, timeID))
        else:
            # For non-monthly variables, only filter by variable and timeID
            timeID = int(row['timeID'])
            if variable and var != variable:
                continue
            if timeID_range and not (timeID_range[0] <= timeID <= timeID_range[1]):
                continue
            filtered.append((url, var, None, timeID))
    return filtered

def extract_and_stack_geotiffs(urls_info, bbox, output_path):
    bands = []
    band_names = []
    out_meta = None

    for url, var, month, timeID in urls_info:
        print(f"Processing {url}")
        with rasterio.open(url) as src:
            vrt_options = {
                "crs": "EPSG:4326",  # WGS84
                "resampling": Resampling.nearest
            }
            with WarpedVRT(src, **vrt_options) as vrt:
                window = from_bounds(*bbox, vrt.transform)
                data = vrt.read(1, window=window)
                out_transform = vrt.window_transform(window)
                if out_meta is None:
                    out_meta = vrt.meta.copy()
                    out_meta.update({
                        "height": data.shape[0],
                        "width": data.shape[1],
                        "transform": out_transform,
                        "crs": "EPSG:4326"
                    })
                bands.append(data)
                # Band name logic
                if var in MONTHLY_VARS:
                    band_names.append(f"{var}_m{month}_c{timeID}")
                else:
                    band_names.append(f"{var}_c{timeID}")

    if not bands:
        print("No bands to write.")
        return

    profile = out_meta.copy()
    profile.update({
        "count": len(bands),
        "driver": "GTiff",
        "compress": "deflate",
        "tiled": True,
        "blockxsize": 512,
        "blockysize": 512
    })

    with MemoryFile() as memfile:
        with memfile.open(**profile) as dst:
            for idx, band in enumerate(bands, start=1):
                dst.write(band, idx)
                dst.set_band_description(idx, band_names[idx-1])
        rio_copy(memfile.name, output_path, driver='COG', compress='deflate', blocksize=512, overview_resampling='nearest')
  
    print(f"Saved {output_path}")

if __name__ == "__main__":
    # ----------------- USER-EDITABLE PARAMETERS -----------------
    # Path to the CSV file containing URLs and parameters
    URLS_CSV = "Python/holocene_climate/data/urls_to_query_bio12.csv"

    # Set the variable you want to extract (e.g. "pr", "tasmax", "bio18", "dem", etc.)
    VARIABLE = "bio12"  # or None for all variables

    # Set the month range if applicable (for monthly variables only)
    # Example: (1, 12) for all months, (6, 8) for June to August, or None for all
    MONTH_RANGE = (1, 2) if VARIABLE in MONTHLY_VARS else None  # or None for all

    # Set the timeID range (for all variables)
    # Example: (-200, 20) for all, or (19, 20) for a small test
    TIMEID_RANGE = None#(19, 20)  # or None for all
    # ------------------------------------------------------------

    # Read URLs and parameters from CSV
    with open(URLS_CSV, newline='') as csvfile:
        reader = csv.DictReader(csvfile)
        csv_rows = [row for row in reader]

    # Print the first 10 lines of the CSV
    print("First 10 rows from CSV:")
    for row in csv_rows[:10]:
        print(row)

    # Filter URLs
    urls_info = filter_urls_from_csv(
        csv_rows,
        variable=VARIABLE,
        month_range=MONTH_RANGE,
        timeID_range=TIMEID_RANGE
    )
    print(f"Processing {len(urls_info)} GeoTIFF URLs after filtering.")

    # Bounding box for Britain and Ireland (WGS84)
    MIN_LON, MIN_LAT, MAX_LON, MAX_LAT = -11.0, 49.5, 2.1, 61.0
    BBOX = (MIN_LON, MIN_LAT, MAX_LON, MAX_LAT)

    now = datetime.datetime.now().strftime("%Y%m%dT%H%M%S")
    output_path = f"Python/holocene_climate/data/CHELSA_TraCE21k_UK_V1.0_{VARIABLE}_{now}.tif"
    os.makedirs(os.path.dirname(output_path), exist_ok=True)
    extract_and_stack_geotiffs(urls_info, BBOX, output_path)