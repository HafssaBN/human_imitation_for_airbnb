# Fichier de base de données SQLite
CONFIG_DB_FILE = 'Airbnb.db'

# Fichier de sortie au format XLSX
CONFIG_OUTPUT_FILE = 'Data_out.csv'

# Nombre maximal de tentatives en cas d'erreur avant de quitter le programme
CONFIG_MAX_RETRIES = 15

# Délai entre les pages (s)
CONFIG_PAGE_DELAY_MIN = 1  # le temps d'attente avant de passer à la page suivante (secondes)
CONFIG_PAGE_DELAY_MAX = 2  # le temps d'attente avant de passer à la page suivante (secondes)

BOUNDARIES_PER_SCRAPING = 20

MAX_LISTINGS_PER_RUN = 3
DETAIL_SCRAPE_LIMIT = 3


# How long before we rescrape the same stuff
UPDATE_WINDOW_DAYS_BOUNDARY = 30   # days before re-scraping a boundary
UPDATE_WINDOW_DAYS_LISTING  = 30   # days before re-scraping a listing


# --- Proxy Configuration ---
# The credentials from your manager
proxy_username = 'pnuuwebv'
proxy_password = 'njqgpghsah6h'
proxy_host = '64.137.96.74'
proxy_port = 6641



# The correct way to structure the proxy settings for Playwright
CONFIG_PROXY = {
    "server": f"http://{proxy_host}:{proxy_port}",  # The server is just the host and port
    "username": proxy_username,                    # The username is a separate key
    "password": proxy_password                     # The password is a separate key
}