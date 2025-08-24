import logging
import sqlite3
from typing import Tuple, List

import Config
import os
import math
import re
import requests
import SQL
from openpyxl import Workbook
import re
import SQL
import math


def setup_logger() -> logging.Logger:
    logger = logging.getLogger(__name__)
    s_handler = logging.StreamHandler()
    logger.addHandler(s_handler)
    logger.setLevel(logging.INFO)
    return logger


def connect_db() -> sqlite3.Connection:
    db = sqlite3.connect(Config.CONFIG_DB_FILE, check_same_thread=False)
    SQL.execute_sql_query_no_results(db, SQL.create_tracking_table)
    SQL.execute_sql_query_no_results(db, SQL.create_listing_tracking_table)
    SQL.execute_sql_query_no_results(db, SQL.create_boundaries_tracking_table)
    SQL.execute_sql_query_no_results(db, SQL.create_listing_index)
    return db


def validate_response_or_exception(response: requests.Response, code: int, logger: logging.Logger):
    if response.status_code != code:
        logger.error(f'Problem ulr {response.url} \n {response.status_code}')
        raise Exception(f'Problem ulr {response.url} \n {response.status_code} \n {response.text}')


def load_data_points(db: sqlite3.Connection, file_path)  -> tuple[int, list[tuple[float, float, float, float]]]:
    remaining = []
    total = 0
    with open(file_path, 'r') as file:
        for idx, line in enumerate(file.readlines()):
            total += 1
            line = line.strip().replace('\n', '').replace('\t', '')
            p1, p2 = line.split('|')
            xmin, ymin = p1.split(',')
            xmax, ymax = p2.split(',')
            remaining.append((float(xmin), float(ymin), float(xmax), float(ymax),))
    return total, remaining


def get_zoom_level(lat_min, lng_min, lat_max, lng_max, map_width_px, map_height_px):
    """
    Calculate the zoom level for Google Maps based on boundary coordinates and map size

    Args:
        lat_min: minimum latitude
        lng_min: minimum longitude
        lat_max: maximum latitude
        lng_max: maximum longitude
        map_width_px: map width in pixels
        map_height_px: map height in pixels

    Returns:
        zoom_level: int
    """

    # Constants for Google Maps
    GLOBE_WIDTH = 256  # a constant in Google's map projection
    ZOOM_MAX = 21

    def degrees_to_radians(deg):
        return deg * (math.pi / 180)

    # Calculate angular distance in radians
    angle = lng_max - lng_min
    if angle < 0:
        angle += 360

    # Calculate zoom level based on width
    zoom_width = math.floor(math.log(map_width_px * 360 / angle / GLOBE_WIDTH) / math.log(2))

    # Calculate zoom level based on height
    lat_rad_min = degrees_to_radians(lat_min)
    lat_rad_max = degrees_to_radians(lat_max)
    world_height = GLOBE_WIDTH * (1 << zoom_width)
    lat_fraction = (math.log(math.tan(lat_rad_max / 2 + math.pi / 4) /
                             math.tan(lat_rad_min / 2 + math.pi / 4)))
    zoom_height = math.floor(math.log(map_height_px * 2 / lat_fraction / GLOBE_WIDTH) / math.log(2))

    # Use the smaller zoom level to ensure all points are visible
    zoom_level = min(zoom_width, zoom_height, ZOOM_MAX)

    return max(1, int(zoom_level))
