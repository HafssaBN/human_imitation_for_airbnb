# --- START OF FILE HostConfig.py ---
import os

# --- DYNAMIC PATH CONFIGURATION ---
# This makes the paths work correctly regardless of where the script is run from.

# 1. Get the absolute path to the directory containing this file.
#    Assuming this file is in a folder like 'airbnb_host/config/'
CONFIG_DIR = os.path.dirname(os.path.abspath(__file__))

# 2. Get the path to the main package directory ('airbnb_host') by going up one level.
BASE_DIR = os.path.dirname(CONFIG_DIR)

# 3. Build the full, absolute paths to the database and output files.
#    os.path.join is the correct way to build paths that work on any operating system.

# Fichier de base de données SQLite
# This tells the script to look for the database in a subfolder named 'database'.
CONFIG_DB_FILE = os.path.join(BASE_DIR, 'database', 'Airbnb.db')

# Fichier de sortie au format CSV
# This tells the script to save the output in a subfolder named 'output'.
CONFIG_OUTPUT_FILE = os.path.join(BASE_DIR, 'output', 'Data_out.csv')
# --- END OF DYNAMIC PATH CONFIGURATION ---


# Nombre maximal de tentatives en cas d'erreur avant de quitter le programme
CONFIG_MAX_RETRIES = 15

# Délai entre les pages (s)
CONFIG_PAGE_DELAY_MIN = 1
CONFIG_PAGE_DELAY_MAX = 2

BOUNDARIES_PER_SCRAPING = 20

MAX_LISTINGS_PER_RUN = 3
DETAIL_SCRAPE_LIMIT = 3


# How long before we rescrape the same stuff
UPDATE_WINDOW_DAYS_BOUNDARY = 30
UPDATE_WINDOW_DAYS_LISTING  = 30


# --- Proxy Configuration ---
proxy_username = 'pnuuwebv'
proxy_password = 'njqgpghsah6h'
proxy_host = '64.137.96.74'
proxy_port = 6641


CONFIG_PROXY = None
'''
CONFIG_PROXY = {
    "server": f"http://{proxy_host}:{proxy_port}",
    "username": proxy_username,
    "password": proxy_password
}
'''