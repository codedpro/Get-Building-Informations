from selenium import webdriver
from selenium.webdriver.edge.service import Service
from selenium.webdriver.edge.options import Options
import math
import os
import time

def lat_lon_to_tile(lat, lon, zoom):
    """
    Convert latitude and longitude to tile x and y coordinates at a given zoom level.
    """
    n = 2.0 ** zoom
    x_tile = int((lon + 180.0) / 360.0 * n)
    y_tile = int((1.0 - math.log(math.tan(math.radians(lat)) + 1 / math.cos(math.radians(lat))) / math.pi) / 2.0 * n)
    return x_tile, y_tile

def generate_tile_urls(lat_min, lon_min, lat_max, lon_max, zoom):
    """
    Generate tile URLs for the bounding box and zoom level using the correct {zoom}/{y}/{x} order.
    """
    x_min, y_min = lat_lon_to_tile(lat_min, lon_min, zoom)
    x_max, y_max = lat_lon_to_tile(lat_max, lon_max, zoom)

    y_min, y_max = min(y_min, y_max), max(y_min, y_max)

    print(f"Tile Range: x: {x_min}-{x_max}, y: {y_min}-{y_max}")

    base_url = "https://gnaf2.post.ir/tile/layers/group"
    urls = []
    for x in range(x_min, x_max + 1):
        for y in range(y_min, y_max + 1):
            urls.append(f"{base_url}/{zoom}/{y}/{x}.pbf")  # Correct order
    return urls

def setup_edge_driver(download_dir, edgedriver_path):
    """
    Set up Selenium Edge WebDriver with a custom download directory.
    """
    options = Options()
    options.add_experimental_option("prefs", {
        "download.default_directory": download_dir,
        "download.prompt_for_download": False,
        "directory_upgrade": True
    })
    options.add_argument("--start-maximized")
    options.add_argument("--inprivate")  # Incognito mode
    options.add_argument("--disable-cloud-managed-settings")
    options.add_argument("--ignore-certificate-errors")
    
    service = Service(executable_path=edgedriver_path)
    return webdriver.Edge(service=service, options=options)

def download_pbf_files(driver, urls):
    """
    Navigate to each URL and download the PBF file using the Edge WebDriver.
    """
    for url in urls:
        try:
            print(f"Downloading: {url}")
            driver.get(url)
            time.sleep(2)  # Wait for the file to download
        except Exception as e:
            print(f"Failed to download {url}: {e}")

if __name__ == "__main__":
    # Define the bounding box for Tehran, Iran (example)
    lat_min = 38.0
    lon_min = 46.1
    lat_max = 38.2
    lon_max = 46.5
    zoom = 14  # Adjust zoom level as needed

    # Set up the download directory
    download_dir = os.path.abspath("PBF-East Azerbaijan")
    os.makedirs(download_dir, exist_ok=True)

    # Path to Edge WebDriver
    edgedriver_path = r"F:\Programming\Alireza\Gnaf\edgedriver_win64\msedgedriver.exe"  # Update this path

    # Generate PBF URLs
    pbf_urls = generate_tile_urls(lat_min, lon_min, lat_max, lon_max, zoom)
    print(f"Generated {len(pbf_urls)} URLs.")

    if not pbf_urls:
        print("No URLs generated. Check the bounding box and zoom level.")
    else:
        # Set up Edge WebDriver and download the files
        driver = setup_edge_driver(download_dir, edgedriver_path)
        download_pbf_files(driver, pbf_urls)
        driver.quit()
        print("Download process completed.")
