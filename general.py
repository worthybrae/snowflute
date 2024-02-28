import os
import requests
from zipfile import ZipFile
from io import BytesIO
import pandas as pd
import numpy as np
from tqdm import tqdm
import geohash
import re


def format_geohash_filter(geohash_list):
    formatted_geohashes = " or ".join([f"geohash like '{g[:3].lower}%'" for g in geohash_list])
    return f"({formatted_geohashes})"
    
def format_id_filter(id_list):
    lower_prefixes = " or ".join([f"id like '{id[:3].lower()}%'" for id in id_list])
    lower_equality = " or ".join([f"id = '{id.lower()}'" for id in id_list])
    upper_prefixes = " or ".join([f"id like '{id[:3].upper()}%'" for id in id_list])
    upper_equality = " or ".join([f"id = '{id.upper()}'" for id in id_list])
    prefix_filter = f"({lower_prefixes} or {upper_prefixes})"
    equality_filter = f"({lower_equality} or {upper_equality})"
    return prefix_filter, equality_filter

def format_ip_filter(ip_list):
    prefix_filter = " or ".join([f"ip_address like '{ip[:3]}%'" for ip in ip_list])
    equality_filter = " or ".join([f"ip_address = '{ip}'" for ip in ip_list])
    return f"({prefix_filter})", f"({equality_filter})"

def load_geohashes_from_csv(iso3_code):
    df = pd.read_csv('data/geohashes.csv')
    matched_row = df[df['Alpha-3'] == iso3_code]
    if matched_row.empty or not matched_row['Geohash (3)'].iloc[0]:
        return None
    return [x.strip() for x in matched_row['Geohash (3)'].iloc[0].split(',')]

def get_geohashes(iso3, lower_left_lat, lower_left_lng, upper_right_lat, upper_right_lng, precision=3):
    if iso3:
        geohashes = load_geohashes_from_csv(iso3)
    else:
        geohashes = None

    if not geohashes:
        # The error values are derived from the precision of the geohash.
        lat_err = lng_err = 0.0001 * 2 ** (5 - precision)

        lat_steps = np.arange(lower_left_lat, upper_right_lat, lat_err)
        lng_steps = np.arange(lower_left_lng, upper_right_lng, lng_err)

        # Use a set comprehension for unique geohashes
        geohashes = {
            geohash.encode(lat, lng, precision)
            for lat in tqdm(lat_steps, desc="Calculating Geohashes")
            for lng in lng_steps
        }

    return list(set([g[:3] for g in geohashes]))

def download_boundaries():
    url = "https://www.naturalearthdata.com/http//www.naturalearthdata.com/download/110m/cultural/ne_110m_admin_0_countries.zip"
    headers = {
        "User-Agent": "Mozilla/5.0 (X11; Ubuntu; Linux x86_64; rv:89.0) Gecko/20100101 Firefox/89.0"
    }

    # Ensure the directory exists
    if not os.path.exists('data/country_boundaries'):
        os.makedirs('data/country_boundaries')

    # Download the zip file
    try:
        response = requests.get(url, headers=headers)
        response.raise_for_status()  # Raise an error for bad responses
    except:
        print('error making request to get country boundaries file')
        return False

    try:
        with ZipFile(BytesIO(response.content)) as z:
            z.extractall('data/country_boundaries')
    except:
        print('error unzipping the compressed boundary file')
        return False
    return True

def haversine(lon1, lat1, lon2, lat2):
    """Calculate the great circle distance between two points on the earth."""
    from math import radians, cos, sin, asin, sqrt

    # Convert decimal degrees to radians
    lon1, lat1, lon2, lat2 = map(radians, [lon1, lat1, lon2, lat2])

    # Haversine formula
    dlon = lon2 - lon1
    dlat = lat2 - lat1
    a = sin(dlat/2)**2 + cos(lat1) * cos(lat2) * sin(dlon/2)**2
    c = 2 * asin(sqrt(a))
    r = 6371  # Radius of Earth in kilometers
    return c * r

def create_lat_lon_grid(minx, miny, maxx, maxy, precision):
    # Calculate step sizes based on precision
    sample_geohash = geohash.encode((miny - 1 + maxy + 1) / 2, (minx - 1 + maxx + 1) / 2, precision)
    decoded_geohash = geohash.bbox(sample_geohash)
    lat_size = haversine(minx, decoded_geohash['s'], minx, decoded_geohash['n'])
    lon_size = haversine(decoded_geohash['w'], miny, decoded_geohash['e'], miny)

    lat_step = lat_size / 200  # Approx conversion from km to lat
    lon_step = lon_size / (200 * np.cos(np.radians(miny)))  # Approx conversion from km to lon

    latitudes = np.arange(miny, maxy, lat_step)
    longitudes = np.arange(minx, maxx, lon_step)

    return [(lat, lon) for lat in latitudes for lon in longitudes]

def sanitize_string(string):
    # Remove invalid file name characters
    s = string.replace('-', '_')
    return re.sub(r'[\\/*?:"<>|]', "", s)

def convert_list_to_pandas(result_set, columns=False):
    if columns:
        return 
    else:
        return pd.DataFrame(result_set, columns=False)