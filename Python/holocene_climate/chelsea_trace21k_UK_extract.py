import os
import datetime
import rasterio
from rasterio.vrt import WarpedVRT
from rasterio.windows import from_bounds
from rasterio.enums import Resampling
from rasterio.shutil import copy as rio_copy
from rasterio.io import MemoryFile
import numpy as np
import requests
from bs4 import BeautifulSoup

# Set PROJ_LIB if not already set
if "PROJ_LIB" not in os.environ:
    try:
        import pyproj
        os.environ["PROJ_LIB"] = pyproj.datadir.get_data_dir()
    except ImportError:
        pass

def get_https_urls_from_envicloud(variable="pr", limit=5):
    """
    Scrape the EnviCloud directory listing for CHELSA TraCE21k precipitation GeoTIFFs.
    Returns a list of HTTPS URLs.
    """
    base_url = f"https://os.zhdk.cloud.switch.ch/chelsav1/chelsa_trace/{variable}/"
    response = requests.get(base_url)
    soup = BeautifulSoup(response.text, features="xml")
    urls = []
    for link in soup.find_all("a"):
        href = link.get("href")
        if href and href.endswith(".tif"):
            urls.append(base_url + href)
            if limit and len(urls) >= limit:
                break
    return urls

def extract_and_stack_geotiffs(urls, bbox, output_path):
    bands = []
    band_names = []
    out_meta = None

    for url in urls:
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
                # Extract mXX_cYY from filename
                parts = os.path.basename(url).split("_")
                m = parts[3]
                c = parts[4]
                band_names.append(f"c{c}_m{m}")

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
    # Set the variable for the data type (e.g., "pr" for precipitation)
    VARIABLE = "pr"
    # Set the maximum number of files to process
    FILE_PROCESS_LIMIT = 10  # Change this value as needed

    # Scrape HTTPS URLs from EnviCloud (get all, then limit for processing)
    all_urls = get_https_urls_from_envicloud(variable=VARIABLE)
    print(f"Found {len(all_urls)} GeoTIFF URLs on EnviCloud.")

    # Limit the number of files to process
    urls = all_urls[:FILE_PROCESS_LIMIT]
    print(f"Processing {len(urls)} GeoTIFF URLs.")

    # Bounding box for Britain and Ireland (WGS84)
    MIN_LON, MIN_LAT, MAX_LON, MAX_LAT = -11.0, 49.5, 2.1, 61.0
    BBOX = (MIN_LON, MIN_LAT, MAX_LON, MAX_LAT)

    now = datetime.datetime.now().strftime("%Y%m%dT%H%M%S")
    output_path = f"Python/holocene_climate/data/CHELSA_TraCE21k_UK_V1.0_{VARIABLE}_{now}.tif"
    os.makedirs(os.path.dirname(output_path), exist_ok=True)
    extract_and_stack_geotiffs(urls, BBOX, output_path)