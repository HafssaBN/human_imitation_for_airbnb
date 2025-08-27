'''
Airbnb.db database is a library with different sections:
host_tracking table:= The School's Main Student Directory, This is the main front desk. It has one record card for each host with their basic information (name, bio, photo URL..).
**Purpose: This is the master file for every student in the entire school.
Each student has one and only one record card in this directory.
This record card contains all their details: full name, date of birth, address, grades, photo, etc.
It is the single source of truth for all information about a student.


host_listings table: This is the "Listings" shelf. It just contains lists of which books (listings) each person (host) has checked out.

**Purpose: This is a simple checklist whose only job is to answer the question: "Which students are in this specific teacher's class?"
It is just a simple list connecting a teacher_id to a student_id.
It does not contain the student's grades, address, or photo. Why? Because that would be inefficient and messy. If a student's address changes, you don't want to update 5 different class rosters; you want to update the master Student Directory once.



host_guidebooks table: This is the "Guidebooks" shelf.
host_travels table: This is the "Travels" shelf.
host_reviews table: This is the "Reviews" shelf.

'''

import sqlite3
import datetime
from typing import List, Dict, Any, Optional,Iterable
import json 
from .config import HostConfig 

# -----------------------------
# Base DDL
# -----------------------------


# ---- listings (parent) -----------------------------------------
create_listing_tracking_table = """
CREATE TABLE IF NOT EXISTS listing_tracking (
  ListingId TEXT PRIMARY KEY,
  ListingUrl TEXT UNIQUE,
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
  Urlhost TEXT,
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
  userUrl TEXT,
  years INTEGER,
  months INTEGER,
  hostrAtingAverage REAL

);
"""

# Helpful index for lookups by ID
create_listing_index = """CREATE INDEX IF NOT EXISTS idx_listing ON listing_tracking(ListingId);"""
  

# ---- co-hosts per listing ----------------------------

create_co_hosts_table = """
CREATE TABLE IF NOT EXISTS co_hosts (
  ListingId       TEXT,
  co_host_id      TEXT,
  co_host_name    TEXT,
  co_host_url     TEXT,
  co_host_picture TEXT,
  PRIMARY KEY (ListingId, co_host_id),
  FOREIGN KEY (ListingId) REFERENCES listing_tracking(ListingId) ON DELETE CASCADE
);

"""


# ---- hosts (parent) --------------------------------------------
create_host_tracking_table = """
CREATE TABLE IF NOT EXISTS host_tracking (
  userId TEXT PRIMARY KEY,
  userUrl TEXT UNIQUE,
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
  profile_photo_url TEXT,
  about_text TEXT,
  bio_text TEXT
);
"""




# ---- Child tables for variable-length data
# ---- child: which listings belong to a host ------------------
# ON DELETE CASCADE is added meaning that : "If the parent is deleted, automatically delete all of its children."
create_host_listings_table = """
CREATE TABLE IF NOT EXISTS host_listings (
  userId TEXT,
  name TEXT,
  listingId TEXT,
  listingUrl TEXT,
  PRIMARY KEY (userId, listingId),
  FOREIGN KEY (userId)    REFERENCES host_tracking(userId)     ON DELETE CASCADE,
  FOREIGN KEY (listingId) REFERENCES listing_tracking(ListingId) ON DELETE CASCADE
);
"""
# ---- child: guidebooks -----------------------------------------

"""
Because the primary key is only userId, the database will only allow one row per userId.
➡️ That means each host can have at most one guidebook.

But in reality, a host can publish multiple guidebooks (e.g., "Best Cafés in Paris", "Hiking in the Alps"). 
To support that, your PK should include something unique per guidebook, not just the user. For example: url / title


"""




create_host_guidebooks_table = """
CREATE TABLE IF NOT EXISTS host_guidebooks (
  id     INTEGER PRIMARY KEY AUTOINCREMENT,
  userId TEXT,
  title  TEXT,
  url    TEXT,
  FOREIGN KEY (userId) REFERENCES host_tracking(userId) ON DELETE CASCADE
);

"""
# ---- child: travels --------------------------------------------
create_host_travels_table =""" 
CREATE TABLE IF NOT EXISTS host_travels (
  userId     TEXT,
  name       TEXT,
  place      TEXT,
  country    TEXT,
  trips      INTEGER,
  when_label TEXT,
  PRIMARY KEY (userId, place, when_label),
  FOREIGN KEY (userId) REFERENCES host_tracking(userId) ON DELETE CASCADE
);

"""

create_host_reviews_table = """
CREATE TABLE IF NOT EXISTS host_reviews (
  userId TEXT,
  name TEXT,
  reviewId TEXT,
  sourceListingId TEXT,
  reviewer_name TEXT,
  reviewer_location TEXT,
  rating REAL,
  date_text TEXT,
  text TEXT,
  PRIMARY KEY (userId, reviewId),
  FOREIGN KEY (userId) REFERENCES host_tracking (userId) ON DELETE CASCADE
);
"""

create_listing_reviews_table = """
CREATE TABLE IF NOT EXISTS listing_reviews (
  ListingId         TEXT,
  reviewId          TEXT,
  userId            TEXT,
  reviewer_name     TEXT,
  reviewer_location TEXT,
  rating            REAL,
  date_text         TEXT,
  text              TEXT,
  sourceListingId   TEXT,
  PRIMARY KEY (ListingId, reviewId),
  FOREIGN KEY (ListingId) REFERENCES listing_tracking(ListingId) ON DELETE CASCADE
);

"""
# ---- listing images ----------------------------------



def create_listing_pictures_table_dynamic():
    """
    Create a table that can dynamically add picture columns as needed.
    We'll start with a reasonable number and expand as needed.
    """
    return """
    CREATE TABLE IF NOT EXISTS listing_pictures (
        ListingId TEXT PRIMARY KEY,
        total_pictures INTEGER DEFAULT 0,
        picture_1 TEXT,
        picture_2 TEXT,
        picture_3 TEXT,
        picture_4 TEXT,
        picture_5 TEXT,
        picture_6 TEXT,
        picture_7 TEXT,
        picture_8 TEXT,
        picture_9 TEXT,
        picture_10 TEXT,
        picture_11 TEXT,
        picture_12 TEXT,
        picture_13 TEXT,
        picture_14 TEXT,
        picture_15 TEXT,
        picture_16 TEXT,
        picture_17 TEXT,
        picture_18 TEXT,
        picture_19 TEXT,
        picture_20 TEXT,
        FOREIGN KEY (ListingId) REFERENCES listing_tracking(ListingId) ON DELETE CASCADE
    );
    """

def get_max_picture_columns(db: sqlite3.Connection) -> int:
    """Get the current maximum number of picture columns in the table."""
    cur = db.cursor()
    cur.execute("PRAGMA table_info(listing_pictures)")
    cur.execute("PRAGMA foreign_keys = ON;")
    columns = cur.fetchall()
    
    picture_columns = [col[1] for col in columns if col[1].startswith('picture_')]
    if not picture_columns:
        return 0
    
    # Extract numbers and find max
    numbers = []
    for col in picture_columns:
        try:
            num = int(col.split('_')[1])
            numbers.append(num)
        except (IndexError, ValueError):
            continue
    
    return max(numbers) if numbers else 0




def add_picture_columns_if_needed(db: sqlite3.Connection, required_columns: int):
    """Dynamically add picture columns if we need more than currently exist."""
    current_max = get_max_picture_columns(db)
    
    if required_columns <= current_max:
        return  # We already have enough columns
    
    cur = db.cursor()
    for i in range(current_max + 1, required_columns + 1):
        column_name = f"picture_{i}"
        try:
            cur.execute(f"ALTER TABLE listing_pictures ADD COLUMN {column_name} TEXT")
            print(f"Added column: {column_name}")
        except sqlite3.OperationalError as e:
            if "duplicate column name" not in str(e).lower():
                raise
    
    db.commit()



create_listing_images_unique_index = """
CREATE UNIQUE INDEX IF NOT EXISTS ux_listing_images
  ON listing_images_table(ListingId, Picture);
"""

def upsert_listing_pictures_horizontal(db: sqlite3.Connection, listing_id: str, picture_urls: List[str]):
    """
    Insert/update pictures for a listing in horizontal format (picture_1, picture_2, etc.)
    """
    if not picture_urls:
        return
    
    # Remove duplicates while preserving order
    unique_pictures = list(dict.fromkeys([url for url in picture_urls if url]))
    
    if not unique_pictures:
        return
    
    # Ensure we have enough columns
    add_picture_columns_if_needed(db, len(unique_pictures))
    
    cur = db.cursor()
    
    # Build the dynamic SQL
    columns = ["ListingId", "total_pictures"]
    values = [listing_id, len(unique_pictures)]
    placeholders = ["?", "?"]
    
    for i, picture_url in enumerate(unique_pictures, 1):
        column_name = f"picture_{i}"
        columns.append(column_name)
        values.append(picture_url)
        placeholders.append("?")
    
    # Create the INSERT OR REPLACE query
    columns_str = ", ".join(columns)
    placeholders_str = ", ".join(placeholders)
    
    # For UPDATE part, we need to set all picture columns to NULL first, then update with new values
    max_columns = get_max_picture_columns(db)
    update_sets = ["total_pictures = excluded.total_pictures"]
    
    # Reset all picture columns to NULL
    for i in range(1, max_columns + 1):
        update_sets.append(f"picture_{i} = NULL")
    
    # Then set the ones we have data for
    for i in range(1, len(unique_pictures) + 1):
        update_sets.append(f"picture_{i} = excluded.picture_{i}")
    
    update_sets_str = ", ".join(update_sets)
    
    query = f"""
    INSERT INTO listing_pictures ({columns_str})
    VALUES ({placeholders_str})
    ON CONFLICT(ListingId) DO UPDATE SET {update_sets_str}
    """
    
    cur.execute(query, values)
    db.commit()
    
    print(f"Updated {listing_id} with {len(unique_pictures)} pictures")


def get_listing_pictures(db: sqlite3.Connection, listing_id: str) -> Dict[str, Any]:
    """Get all pictures for a listing in a structured format."""
    cur = db.cursor()
    cur.execute("SELECT * FROM listing_pictures WHERE ListingId = ?", (listing_id,))
    row = cur.fetchone()
    
    if not row:
        return {"ListingId": listing_id, "total_pictures": 0, "pictures": []}
    
    # Get column names
    cur.execute("PRAGMA table_info(listing_pictures)")
    columns = [col[1] for col in cur.fetchall()]
    
    # Create dict from row
    row_dict = dict(zip(columns, row))
    
    # Extract picture URLs in order
    pictures = []
    i = 1
    while f"picture_{i}" in row_dict:
        url = row_dict[f"picture_{i}"]
        if url:  # Only add non-null URLs
            pictures.append(url)
        i += 1
    
    return {
        "ListingId": listing_id,
        "total_pictures": row_dict.get("total_pictures", 0),
        "pictures": pictures,
        "raw_data": row_dict  # Include raw data for debugging
    }




def upsert_listing_images(db: sqlite3.Connection, listing_id: str, picture_urls):
    """Insert all images for a listing (ignores duplicates)."""
    if not picture_urls:
        return
    rows = [(listing_id, u) for u in picture_urls if u]
    cur = db.cursor()
    cur.executemany(
        "INSERT OR IGNORE INTO listing_images_table (ListingId, Picture) VALUES (?, ?)",
        rows
    )
    db.commit()



def replace_listing_images_horizontal(db: sqlite3.Connection, listing_id: str, photos: List[str]):
    """
    Replace the old vertical storage with new horizontal storage.
    This replaces the old replace_listing_images function.
    """
    init_pictures_table(db)
    upsert_listing_pictures_horizontal(db, listing_id, photos)


def replace_listing_images(db: sqlite3.Connection, listing_id: str, photos: Iterable[str]):
    init_all_tables(db)
    cur = db.cursor()
    cur.execute("DELETE FROM listing_images_table WHERE ListingId=?", (listing_id,))
    cur.executemany(
        "INSERT OR IGNORE INTO listing_images_table (ListingId, Picture) VALUES (?, ?)",
        [(listing_id, p) for p in photos if p]
    )
    db.commit()
"""
Why replace_listing_images exists

Because just creating the table is not enough — you need to populate it each time you scrape.

Imagine scraping Listing #123 today (10 photos). Tomorrow the host deletes 2 photos and adds 3 new ones.

If you only ever INSERT, you will end up with stale + duplicated photos.

With replace_listing_images:

DELETE FROM listing_images_table WHERE ListingId=? → clears the old set for this listing.

INSERT the fresh set → now DB reflects the latest photos.

That’s why it’s called replace — it refreshes the child table for that listing




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
from typing import Any, Optional

def _to_float_or_none(v: Any) -> Optional[float]:
    """Convert value to float if possible, otherwise return None."""
    if v is None:
        return None
    try:
        return float(v)
    except (ValueError, TypeError):
        return None
    





def init_pictures_table(db: sqlite3.Connection):
    """Initialize the pictures table with the new schema."""
    execute_sql_query_no_results(db, create_listing_pictures_table_dynamic())

# Modified version of the existing function to integrate with the new schema









def init_all_tables(db: sqlite3.Connection):
    # base

   
    execute_sql_query_no_results(db, create_listing_tracking_table)
    execute_sql_query_no_results(db, create_listing_index)
    execute_sql_query_no_results(db, create_listing_reviews_table)
    execute_sql_query_no_results(db, create_co_hosts_table)
    execute_sql_query_no_results(db, create_listing_images_unique_index)
    execute_sql_query_no_results(db, create_listing_pictures_table_dynamic())


    # host + children
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
        ListingId,ListingUrl, ListingObjType, roomTypeCategory, title, name, picture, checkin, checkout,
        price, discounted_price, original_price, link, scraping_time,
        needs_detail_scraping, has_detailed_data,
        reviewsCount, averageRating, host, airbnbLuxe, location, maxGuestCapacity, isGuestFavorite,
        lat, lng, isSuperhost, isVerified, ratingCount, userId, years, months, hostrAtingAverage
      ) VALUES (
        :ListingId, :ListingUrl, :ListingObjType, :roomTypeCategory, :title, :name, :picture, :checkin, :checkout,
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
        'reviewsCount': 0, 'averageRating': 0.0, 'host': None, 'Urlhost': None, 'airbnbLuxe': None,
        'location': None, 'maxGuestCapacity': 0, 'isGuestFavorite': None, 'lat': None, 'lng': None,
        'isSuperhost': None, 'isVerified': None, 'ratingCount': 0, 'userId': None, 'userUrl': None,
        'years': 0, 'months': 0, 'hostrAtingAverage': 0.0, 'ListingObjType': 'REGULAR',
        'roomTypeCategory': None, 'title': None, 'name': None, 'picture': None, 'checkin': None,
        'checkout': None, 'price': None, 'discounted_price': None, 'original_price': None,
    }
    for k, v in defaults.items():
        data.setdefault(k, v)

    query = """
      INSERT OR REPLACE INTO listing_tracking (
        ListingId, ListingUrl, ListingObjType, roomTypeCategory, title, name, picture,
        checkin, checkout, price, discounted_price, original_price, link, scraping_time,
        needs_detail_scraping, has_detailed_data, reviewsCount, averageRating, host, Urlhost,
        airbnbLuxe, location, maxGuestCapacity, isGuestFavorite, lat, lng, isSuperhost, isVerified,
        ratingCount, userId, userUrl, years, months, hostrAtingAverage
      ) VALUES (
        :ListingId, :ListingUrl, :ListingObjType, :roomTypeCategory, :title, :name, :picture,
        :checkin, :checkout, :price, :discounted_price, :original_price, :link, :scraping_time,
        :needs_detail_scraping, :has_detailed_data, :reviewsCount, :averageRating, :host, :Urlhost,
        :airbnbLuxe, :location, :maxGuestCapacity, :isGuestFavorite, :lat, :lng, :isSuperhost, :isVerified,
        :ratingCount, :userId, :userUrl, :years, :months, :hostrAtingAverage
      )
    """
    db.cursor().execute(query, data)
    db.commit()

def update_listing_with_details(db: sqlite3.Connection, listing_id: str, detail_data: dict):
    """
    Insert all photos for a listing into listing_images_table.
    Uses a UNIQUE index on (ListingId, Picture) to avoid duplicates.
    """
    detail_data['has_detailed_data'] = 1
    detail_data['needs_detail_scraping'] = 0
    detail_data['ListingId'] = listing_id


    # Defaults for optional fields we now persist
    for k in ("title","roomTypeCategory","picture","checkin","checkout",
              "price","discounted_price","original_price", "Urlhost", "userUrl"):
        detail_data.setdefault(k, None)



    query = """
      UPDATE listing_tracking SET
        title=:title,
        roomTypeCategory=:roomTypeCategory,
        picture=:picture,
        checkin=:checkin,
        checkout=:checkout,
        price=:price,
        discounted_price=:discounted_price,
        original_price=:original_price,

        reviewsCount=:reviewsCount,
        averageRating=:averageRating,
        host=:host,
        Urlhost=:Urlhost,
        airbnbLuxe=:airbnbLuxe,
        location=:location,
        maxGuestCapacity=:maxGuestCapacity,
        isGuestFavorite=:isGuestFavorite,
        lat=:lat,
        lng=:lng,
        isSuperhost=:isSuperhost,
        isVerified=:isVerified,
        ratingCount=:ratingCount,
        userId=:userId,
        userUrl=:userUrl,
        years=:years,
        months=:months,
        hostrAtingAverage=:hostrAtingAverage,

        has_detailed_data=:has_detailed_data,
        needs_detail_scraping=:needs_detail_scraping
      WHERE ListingId=:ListingId
    """
    
    
    db.cursor().execute(query, detail_data)
    db.commit()

def check_if_listing_exists(db: sqlite3.Connection, listing_id: str) -> bool:
    now = datetime.datetime.now()
    start_time = int((now - datetime.timedelta(days=HostConfig.UPDATE_WINDOW_DAYS_LISTING)).timestamp())
    cur = db.cursor()
    cur.execute(
        "SELECT 1 FROM listing_tracking WHERE ListingId=? AND scraping_time>=? LIMIT 1",
        (listing_id, start_time),
    )
    return cur.fetchone() is not None

# -----------------------------
# Host profile + child writers
# -----------------------------

def upsert_host_profile(db: sqlite3.Connection, data: Dict[str, Any]):
    init_all_tables(db)
    cur = db.cursor()
    cur.execute("SELECT * FROM host_tracking WHERE userId=?", (data.get("userId"),))
    row = cur.fetchone()

    def keep(old, new):
        return new if new not in (None, "") else old

    to_save = {
        "userId": data.get("userId"),
        "name": keep(row[2] if row else None, data.get("name")),
        "isSuperhost": keep(row[3] if row else None, data.get("isSuperhost")),
        "isVerified": keep(row[4] if row else None, data.get("isVerified")),
        "ratingAverage": keep(row[5] if row else None, data.get("ratingAverage")),
        "ratingCount": keep(row[6] if row else None, data.get("ratingCount")),
        "years": keep(row[7] if row else None, data.get("years")),
        "months": keep(row[8] if row else None, data.get("months")),
        "total_listings": data.get("total_listings") if data.get("total_listings") is not None else (row[9] if row else 0),
        "profile_url": keep(row[10] if row else None, data.get("profile_url")),
        "scraping_time": data.get("scraping_time") or int(datetime.datetime.now().timestamp()),
        "profile_photo_url": keep(row[12] if row else None, data.get("profile_photo_url")),
        "about_text": keep(row[13] if row else None, data.get("about_text")),
        "bio_text": keep(row[14] if row else None, data.get("bio_text")),
        "userUrl": keep(row[1] if row else None, data.get("userUrl")),
    }

    cur.execute("""
      INSERT INTO host_tracking (
        userId, userUrl, name, isSuperhost, isVerified, ratingAverage, ratingCount,
        years, months, total_listings, profile_url, scraping_time,
        profile_photo_url, about_text, bio_text
      ) VALUES (
        :userId, :userUrl, :name, :isSuperhost, :isVerified, :ratingAverage, :ratingCount,
        :years, :months, :total_listings, :profile_url, :scraping_time,
        :profile_photo_url, :about_text, :bio_text
      )
      ON CONFLICT(userId) DO UPDATE SET
        userUrl=excluded.userUrl,
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


def replace_host_listings(db, user_id: str, items: Iterable[Dict[str, Any]]):
    init_all_tables(db)
    cur = db.cursor()

    # Get host name once
    cur.execute("SELECT name FROM host_tracking WHERE userId=?", (user_id,))
    row = cur.fetchone()
    host_name = row[0] if row else None

    cur.execute("DELETE FROM host_listings WHERE userId=?", (user_id,))
    cur.executemany(
        "INSERT OR REPLACE INTO host_listings (userId, name, listingId, listingUrl) VALUES (?, ?, ?, ?)",
        [(user_id, host_name, str(i.get("listingId")), i.get("listingUrl")) for i in items if i.get("listingId")],
    )
    db.commit()



def set_host_name_for_listings(db: sqlite3.Connection, user_id: str, host_name: str):
    """After we learn host name from PDP, update host_listings.name for that user."""
    cur = db.cursor()
    cur.execute("UPDATE host_listings SET name=? WHERE userId=?", (host_name, user_id))
    db.commit()


def upsert_cohosts(db: sqlite3.Connection, listing_id: str, cohosts: Iterable[Dict[str, Any]]):
    """Insert/replace co-host rows for a listing."""
    if not cohosts:
        return
    cur = db.cursor()
    payload = []
    for ch in cohosts:
        payload.append((
            str(listing_id),
            ch.get("co_host_id"),
            ch.get("co_host_name"),
            ch.get("co_host_url"),
            ch.get("co_host_picture"),
        ))
    cur.executemany(
        """INSERT OR REPLACE INTO co_hosts
           (ListingId, co_host_id, co_host_name, co_host_url, co_host_picture)
           VALUES (?, ?, ?, ?, ?)""",
        payload,
    )
    db.commit()

def update_host_listing_name(db: sqlite3.Connection, user_id: str, listing_id: str, host_name: str):
    """Set the name for a specific host/listing row."""
    if not host_name:
        return
    cur = db.cursor()
    cur.execute(
        "UPDATE host_listings SET name=? WHERE userId=? AND listingId=?",
        (host_name, user_id, str(listing_id)),
    )
    db.commit()

def backfill_host_listing_names_from_tracking(db: sqlite3.Connection, user_id: str):
    """
    Fill any missing names in host_listings from listing_tracking.host.
    Useful after hydration when listing_tracking is populated.
    """
    cur = db.cursor()
    cur.execute("""
        UPDATE host_listings
        SET name = (
            SELECT host FROM listing_tracking lt
            WHERE lt.ListingId = host_listings.listingId
        )
        WHERE userId = ? AND (name IS NULL OR name = '');
    """, (user_id,))
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

def replace_host_travels(db, user_id: str, items: Iterable[Dict[str, Any]]):
    init_all_tables(db)
    cur = db.cursor()

    # fetch host name once
    cur.execute("SELECT name FROM host_tracking WHERE userId=?", (user_id,))
    row = cur.fetchone()
    host_name = row[0] if row else None

    # clear existing rows for this host
    cur.execute("DELETE FROM host_travels WHERE userId=?", (user_id,))

    # insert fresh rows (note the column is when_label)
    payload = []
    for i in items:
        payload.append((
            user_id,
            host_name,
            i.get("place"),
            i.get("country"),
            int(i.get("trips") or 0),
            i.get("when"),            # source key is "when"
        ))

    cur.executemany(
        "INSERT OR REPLACE INTO host_travels (userId, name, place, country, trips, when_label) VALUES (?, ?, ?, ?, ?, ?)",
        payload,
    )
    db.commit()



def backfill_host_child_names(db: sqlite3.Connection, user_id: str):
    """Copy the current name from host_tracking into child tables that store a name column."""
    cur = db.cursor()
    # host_travels
    cur.execute("""
        UPDATE host_travels
           SET name = (SELECT name FROM host_tracking ht WHERE ht.userId = host_travels.userId)
         WHERE userId = ? AND (name IS NULL OR name = '');
    """, (user_id,))
    # host_reviews (if you keep a name column there)
    cur.execute("""
        UPDATE host_reviews
           SET name = (SELECT name FROM host_tracking ht WHERE ht.userId = host_reviews.userId)
         WHERE userId = ? AND (name IS NULL OR name = '');
    """, (user_id,))
    # host_listings already handled by your backfill, so we leave it
    db.commit()

def set_host_name_for_reviews(db, user_id: str, host_name: str):
    cur = db.cursor()
    cur.execute("UPDATE host_reviews SET name=? WHERE userId=?", (host_name, user_id))
    db.commit()
def upsert_host_reviews(db: sqlite3.Connection, user_id: str, items: Iterable[Dict[str, Any]]):
    cur = db.cursor()

    # fetch once
    cur.execute("SELECT name FROM host_tracking WHERE userId=?", (user_id,))
    row = cur.fetchone()
    host_name = row[0] if row else None

    payload = []
    for i in items:
        review_id = i.get("reviewId")
        if not review_id:
            continue
        payload.append((
            user_id,
            host_name,                     # <— new
            review_id,
            i.get("sourceListingId"),
            i.get("reviewer_name"),
            i.get("reviewer_location"),
            _to_float_or_none(i.get("rating")),
            i.get("date_text"),
            i.get("text"),
        ))
    if payload:
        cur.executemany(
            """
            INSERT OR REPLACE INTO host_reviews
              (userId, name, reviewId, sourceListingId, reviewer_name, reviewer_location, rating, date_text, text)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            payload,
        )
        db.commit()


# -----------------------------
# Stats
# -----------------------------

def get_scraping_stats(db: sqlite3.Connection):
    cur = db.cursor()
    stats: Dict[str, Any] = {}
    cur.execute("SELECT COUNT(*) FROM listing_tracking")
    stats['total_listings'] = cur.fetchone()[0]
    cur.execute("SELECT COUNT(*) FROM listing_tracking WHERE has_detailed_data = 0")
    stats['basic_only'] = cur.fetchone()[0]
    cur.execute("SELECT COUNT(*) FROM listing_tracking WHERE has_detailed_data = 1")
    stats['with_details'] = cur.fetchone()[0]
    cur.execute("SELECT COUNT(*) FROM listing_tracking WHERE needs_detail_scraping = 1 AND has_detailed_data = 0")
    stats['pending_details'] = cur.fetchone()[0]

    recent_time = int((datetime.datetime.now() - datetime.timedelta(days=1)).timestamp())
    cur.execute("SELECT COUNT(*) FROM listing_tracking WHERE scraping_time >= ?", (recent_time,))
    stats['recent_listings'] = cur.fetchone()[0]
    return stats
