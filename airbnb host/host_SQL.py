# host_SQL.py
import sqlite3
import datetime
from typing import Iterable, Dict, Any

import Config

# -----------------------------
# Base DDL
# -----------------------------

create_boundaries_tracking_table = """
CREATE TABLE IF NOT EXISTS boundaries_tracking (
  id INTEGER PRIMARY KEY,
  xmin REAL, ymin REAL, xmax REAL, ymax REAL,
  timestamp INTEGER, total INTEGER
);
"""

create_listing_tracking_table = """
CREATE TABLE IF NOT EXISTS listing_tracking (
  id TEXT PRIMARY KEY,
  ListingObjType TEXT,
  roomTypeCategory TEXT,
  title TEXT,
  name TEXT,
  picture TEXT,
  checkin TEXT,
  checkout TEXT,
  price TEXT,
  discounted_price TEXT,
  original_price TEXT,
  link TEXT,
  scraping_time INTEGER,
  needs_detail_scraping INTEGER DEFAULT 0,
  has_detailed_data INTEGER DEFAULT 0,
  reviewsCount INTEGER,
  averageRating REAL,
  host TEXT,
  airbnbLuxe TEXT,
  location TEXT,
  maxGuestCapacity INTEGER,
  isGuestFavorite TEXT,
  lat REAL,
  lng REAL,
  isSuperhost TEXT,
  isVerified TEXT,
  ratingCount TEXT,
  userId TEXT,
  years INTEGER,
  months INTEGER,
  hostrAtingAverage REAL
);
"""

create_listing_index = """CREATE INDEX IF NOT EXISTS idx_listing ON listing_tracking(id);"""

create_tracking_table = """
CREATE TABLE IF NOT EXISTS tracking (
  tracking INTEGER
);
"""

# ---- Host core table (expanded)
create_host_tracking_table = """
CREATE TABLE IF NOT EXISTS host_tracking (
  userId TEXT PRIMARY KEY,
  name TEXT,
  isSuperhost INTEGER,
  isVerified INTEGER,
  ratingAverage REAL,
  ratingCount INTEGER,
  years INTEGER,
  months INTEGER,
  total_listings INTEGER,
  profile_url TEXT,
  scraping_time INTEGER,
  -- NEW
  profile_photo_url TEXT,
  about_text TEXT,
  bio_text TEXT
);
"""

# ---- Child tables for variable-length data

create_host_listings_table = """
CREATE TABLE IF NOT EXISTS host_listings (
  userId TEXT,
  listingId TEXT,
  listingUrl TEXT,
  PRIMARY KEY (userId, listingId)
);
"""

create_host_guidebooks_table = """
CREATE TABLE IF NOT EXISTS host_guidebooks (
  userId TEXT,
  title TEXT,
  url TEXT,
  PRIMARY KEY (userId, url)
);
"""

create_host_travels_table = """
CREATE TABLE IF NOT EXISTS host_travels (
  userId TEXT,
  place TEXT,
  country TEXT,
  trips INTEGER,
  when_label TEXT,
  PRIMARY KEY (userId, place, when_label)
);
"""

create_host_reviews_table = """
CREATE TABLE IF NOT EXISTS host_reviews (
  userId TEXT,
  reviewId TEXT,
  sourceListingId TEXT,
  reviewer_name TEXT,
  reviewer_location TEXT,
  rating REAL,
  date_text TEXT,
  text TEXT,
  PRIMARY KEY (userId, reviewId)
);
"""

# -----------------------------
# Utilities
# -----------------------------

def execute_sql_query_no_results(db: sqlite3.Connection, query: str):
    cur = db.cursor()
    cur.execute(query)
    db.commit()

def _add_column_if_missing(db: sqlite3.Connection, table: str, column: str, ddl_type: str):
    cur = db.cursor()
    cur.execute(f"PRAGMA table_info({table})")
    cols = [r[1] for r in cur.fetchall()]
    if column not in cols:
        cur.execute(f"ALTER TABLE {table} ADD COLUMN {column} {ddl_type}")
        db.commit()

def init_all_tables(db: sqlite3.Connection):
    # base
    execute_sql_query_no_results(db, create_boundaries_tracking_table)
    execute_sql_query_no_results(db, create_listing_tracking_table)
    execute_sql_query_no_results(db, create_listing_index)
    execute_sql_query_no_results(db, create_tracking_table)

    # host
    execute_sql_query_no_results(db, create_host_tracking_table)
    execute_sql_query_no_results(db, create_host_listings_table)
    execute_sql_query_no_results(db, create_host_guidebooks_table)
    execute_sql_query_no_results(db, create_host_travels_table)
    execute_sql_query_no_results(db, create_host_reviews_table)

    # migrations (idempotent)
    _add_column_if_missing(db, "host_tracking", "profile_photo_url", "TEXT")
    _add_column_if_missing(db, "host_tracking", "about_text", "TEXT")
    _add_column_if_missing(db, "host_tracking", "bio_text", "TEXT")

# -----------------------------
# Listing helpers
# -----------------------------

def insert_new_listing(db: sqlite3.Connection, data: dict):
    now_ts = int(datetime.datetime.now().timestamp())
    data['scraping_time'] = now_ts
    data['has_detailed_data'] = 1
    data['needs_detail_scraping'] = 0

    query = """
      INSERT OR REPLACE INTO listing_tracking (
        id, ListingObjType, roomTypeCategory, title, name, picture, checkin, checkout,
        price, discounted_price, original_price, link, scraping_time,
        needs_detail_scraping, has_detailed_data,
        reviewsCount, averageRating, host, airbnbLuxe, location, maxGuestCapacity, isGuestFavorite,
        lat, lng, isSuperhost, isVerified, ratingCount, userId, years, months, hostrAtingAverage
      ) VALUES (
        :id, :ListingObjType, :roomTypeCategory, :title, :name, :picture, :checkin, :checkout,
        :price, :discounted_price, :original_price, :link, :scraping_time,
        :needs_detail_scraping, :has_detailed_data,
        :reviewsCount, :averageRating, :host, :airbnbLuxe, :location, :maxGuestCapacity, :isGuestFavorite,
        :lat, :lng, :isSuperhost, :isVerified, :ratingCount, :userId, :years, :months, :hostrAtingAverage
      )
    """
    db.cursor().execute(query, data)
    db.commit()

def insert_basic_listing(db: sqlite3.Connection, data: dict):
    now_ts = int(datetime.datetime.now().timestamp())
    data['scraping_time'] = now_ts
    data['has_detailed_data'] = 0
    data['needs_detail_scraping'] = 1

    defaults = {
        'reviewsCount': 0, 'averageRating': 0.0, 'host': None, 'airbnbLuxe': False,
        'location': None, 'maxGuestCapacity': 0, 'isGuestFavorite': False, 'lat': None, 'lng': None,
        'isSuperhost': False, 'isVerified': False, 'ratingCount': 0, 'userId': None,
        'years': 0, 'months': 0, 'hostrAtingAverage': 0.0
    }
    for k, v in defaults.items():
        data.setdefault(k, v)

    query = """
      INSERT OR REPLACE INTO listing_tracking (
        id, ListingObjType, roomTypeCategory, title, name, picture, checkin, checkout,
        price, discounted_price, original_price, link, scraping_time,
        needs_detail_scraping, has_detailed_data,
        reviewsCount, averageRating, host, airbnbLuxe, location, maxGuestCapacity, isGuestFavorite,
        lat, lng, isSuperhost, isVerified, ratingCount, userId, years, months, hostrAtingAverage
      ) VALUES (
        :id, :ListingObjType, :roomTypeCategory, :title, :name, :picture, :checkin, :checkout,
        :price, :discounted_price, :original_price, :link, :scraping_time,
        :needs_detail_scraping, :has_detailed_data,
        :reviewsCount, :averageRating, :host, :airbnbLuxe, :location, :maxGuestCapacity, :isGuestFavorite,
        :lat, :lng, :isSuperhost, :isVerified, :ratingCount, :userId, :years, :months, :hostrAtingAverage
      )
    """
    db.cursor().execute(query, data)
    db.commit()

def update_listing_with_details(db: sqlite3.Connection, listing_id: str, detail_data: dict):
    detail_data['has_detailed_data'] = 1
    detail_data['needs_detail_scraping'] = 0
    detail_data['id'] = listing_id

    query = """
      UPDATE listing_tracking SET
        reviewsCount=:reviewsCount, averageRating=:averageRating, host=:host, airbnbLuxe=:airbnbLuxe,
        location=:location, maxGuestCapacity=:maxGuestCapacity, isGuestFavorite=:isGuestFavorite,
        lat=:lat, lng=:lng, isSuperhost=:isSuperhost, isVerified=:isVerified, ratingCount=:ratingCount,
        userId=:userId, years=:years, months=:months, hostrAtingAverage=:hostrAtingAverage,
        has_detailed_data=:has_detailed_data, needs_detail_scraping=:needs_detail_scraping
      WHERE id=:id
    """
    db.cursor().execute(query, detail_data)
    db.commit()

def check_if_listing_exists(db: sqlite3.Connection, listing_id: str) -> bool:
    now = datetime.datetime.now()
    start_time = int((now - datetime.timedelta(days=Config.UPDATE_WINDOW_DAYS_LISTING)).timestamp())
    cur = db.cursor()
    cur.execute(
        "SELECT 1 FROM listing_tracking WHERE id=? AND scraping_time>=? LIMIT 1",
        (listing_id, start_time),
    )
    return cur.fetchone() is not None

# -----------------------------
# Host profile + child writers
# -----------------------------

def upsert_host_profile(db: sqlite3.Connection, data: dict):
    """
    Keys you can pass:
      userId, name, isSuperhost, isVerified, ratingAverage, ratingCount,
      years, months, total_listings, profile_url, scraping_time,
      profile_photo_url, about_text, bio_text
    """
    init_all_tables(db)

    cur = db.cursor()
    cur.execute("SELECT * FROM host_tracking WHERE userId=?", (data.get("userId"),))
    row = cur.fetchone()

    def keep(old, new):
        return new if new not in (None, "") else old

    to_save = {
        "userId": data.get("userId"),
        "name": keep(row[1] if row else None, data.get("name")),
        "isSuperhost": keep(row[2] if row else None, data.get("isSuperhost")),
        "isVerified": keep(row[3] if row else None, data.get("isVerified")),
        "ratingAverage": keep(row[4] if row else None, data.get("ratingAverage")),
        "ratingCount": keep(row[5] if row else None, data.get("ratingCount")),
        "years": keep(row[6] if row else None, data.get("years")),
        "months": keep(row[7] if row else None, data.get("months")),
        "total_listings": data.get("total_listings") if data.get("total_listings") is not None else (row[8] if row else 0),
        "profile_url": keep(row[9] if row else None, data.get("profile_url")),
        "scraping_time": data.get("scraping_time") or int(datetime.datetime.now().timestamp()),
        "profile_photo_url": keep(row[11] if row else None, data.get("profile_photo_url")),
        "about_text": keep(row[12] if row else None, data.get("about_text")),
        "bio_text": keep(row[13] if row else None, data.get("bio_text")),
    }

    cur.execute("""
      INSERT INTO host_tracking (
        userId, name, isSuperhost, isVerified, ratingAverage, ratingCount,
        years, months, total_listings, profile_url, scraping_time,
        profile_photo_url, about_text, bio_text
      ) VALUES (
        :userId, :name, :isSuperhost, :isVerified, :ratingAverage, :ratingCount,
        :years, :months, :total_listings, :profile_url, :scraping_time,
        :profile_photo_url, :about_text, :bio_text
      )
      ON CONFLICT(userId) DO UPDATE SET
        name=excluded.name,
        isSuperhost=excluded.isSuperhost,
        isVerified=excluded.isVerified,
        ratingAverage=excluded.ratingAverage,
        ratingCount=excluded.ratingCount,
        years=excluded.years,
        months=excluded.months,
        total_listings=excluded.total_listings,
        profile_url=excluded.profile_url,
        scraping_time=excluded.scraping_time,
        profile_photo_url=excluded.profile_photo_url,
        about_text=excluded.about_text,
        bio_text=excluded.bio_text
    """, to_save)
    db.commit()

def replace_host_listings(db: sqlite3.Connection, user_id: str, items: Iterable[Dict[str, Any]]):
    init_all_tables(db)
    cur = db.cursor()
    cur.execute("DELETE FROM host_listings WHERE userId=?", (user_id,))
    cur.executemany(
        "INSERT OR REPLACE INTO host_listings (userId, listingId, listingUrl) VALUES (?, ?, ?)",
        [(user_id, str(i.get("listingId")), i.get("listingUrl")) for i in items if i.get("listingId")],
    )
    db.commit()

def replace_host_guidebooks(db: sqlite3.Connection, user_id: str, items: Iterable[Dict[str, Any]]):
    init_all_tables(db)
    cur = db.cursor()
    cur.execute("DELETE FROM host_guidebooks WHERE userId=?", (user_id,))
    cur.executemany(
        "INSERT OR REPLACE INTO host_guidebooks (userId, title, url) VALUES (?, ?, ?)",
        [(user_id, i.get("title"), i.get("url")) for i in items if i.get("url")],
    )
    db.commit()

def replace_host_travels(db: sqlite3.Connection, user_id: str, items: Iterable[Dict[str, Any]]):
    init_all_tables(db)
    cur = db.cursor()
    cur.execute("DELETE FROM host_travels WHERE userId=?", (user_id,))
    cur.executemany(
        "INSERT OR REPLACE INTO host_travels (userId, place, country, trips, when_label) VALUES (?, ?, ?, ?, ?)",
        [(user_id, i.get("place"), i.get("country"), int(i.get("trips") or 0), i.get("when")) for i in items],
    )
    db.commit()

def upsert_host_reviews(db: sqlite3.Connection, user_id: str, items: Iterable[Dict[str, Any]]):
    init_all_tables(db)
    cur = db.cursor()
    cur.executemany(
        """INSERT OR REPLACE INTO host_reviews
           (userId, reviewId, sourceListingId, reviewer_name, reviewer_location, rating, date_text, text)
           VALUES (?, ?, ?, ?, ?, ?, ?, ?)""",
        [(
            user_id,
            i.get("reviewId"),
            i.get("sourceListingId"),
            i.get("reviewer_name"),
            i.get("reviewer_location"),
            float(i.get("rating")) if i.get("rating") is not None else None,
            i.get("date_text"),
            i.get("text"),
        ) for i in items if i.get("reviewId")]
    )
    db.commit()

# -----------------------------
# Stats
# -----------------------------

def get_scraping_stats(db: sqlite3.Connection):
    cur = db.cursor()
    stats = {}
    cur.execute("SELECT COUNT(*) FROM listing_tracking")
    stats['total_listings'] = cur.fetchone()[0]
    cur.execute("SELECT COUNT(*) FROM listing_tracking WHERE has_detailed_data = 0")
    stats['basic_only'] = cur.fetchone()[0]
    cur.execute("SELECT COUNT(*) FROM listing_tracking WHERE has_detailed_data = 1")
    stats['with_details'] = cur.fetchone()[0]
    cur.execute("SELECT COUNT(*) FROM listing_tracking WHERE needs_detail_scraping = 1 AND has_detailed_data = 0")
    stats['pending_details'] = cur.fetchone()[0]
    cur.execute("SELECT COUNT(*) FROM boundaries_tracking")
    stats['boundaries_processed'] = cur.fetchone()[0]
    recent_time = int((datetime.datetime.now() - datetime.timedelta(days=1)).timestamp())
    cur.execute("SELECT COUNT(*) FROM listing_tracking WHERE scraping_time >= ?", (recent_time,))
    stats['recent_listings'] = cur.fetchone()[0]
    return stats
