import sqlite3
import datetime

import Config

create_boundaries_tracking_table = """
    CREATE TABLE IF NOT EXISTS "boundaries_tracking" (
        "id"	INTEGER NOT NULL UNIQUE,
        "xmin"	REAL,
        "ymin"	REAL,
        "xmax"	REAL,
        "ymax"	REAL,
        "timestamp" INTEGER,
        "total" INTEGER,
        PRIMARY KEY("id")
    );
"""

create_listing_tracking_table = """
    CREATE TABLE IF NOT EXISTS "listing_tracking" (
        "id"	TEXT NOT NULL,
        "ListingObjType" TEXT,
        "roomTypeCategory" TEXT,
        "title" TEXT,
        "name" TEXT,
        "picture" TEXT,
        "checkin" TEXT,
        "checkout" TEXT,
        "price" TEXT,
        "discounted_price" TEXT,
        "original_price" TEXT,
        "link"	TEXT,
        "scraping_time"	INTEGER,
        "needs_detail_scraping" INTEGER DEFAULT 0,
        "has_detailed_data" INTEGER DEFAULT 0,
        reviewsCount INTEGER,
        averageRating REAL,
        host TEXT,
        airbnbLuxe TEXT,
        location TEXT,
        "maxGuestCapacity" INTEGER,
        "isGuestFavorite" TEXT,
        "lat" REAL,
        "lng" REAL,
        "isSuperhost" TEXT,
        "isVerified" TEXT,
        "ratingCount" TEXT,
        userId TEXT,
        years INTEGER,
        months INTEGER,
        hostrAtingAverage REAL,
        PRIMARY KEY("id")
    );
"""

create_listing_index = """
    CREATE INDEX IF NOT EXISTS idx_listing ON listing_tracking(id);
"""

create_tracking_table = """
    CREATE TABLE IF NOT EXISTS tracking (
        tracking INTEGER
    );
"""

def execute_sql_query_no_results(db: sqlite3.Connection, query: str):
    cur = db.cursor()
    cur.execute(query)
    db.commit()

def insert_new_listing(db: sqlite3.Connection, data: dict):
    """Insert listing with full detailed data (requires PDP API call)"""
    now = datetime.datetime.now()
    now_timestamp = int(now.timestamp())
    data['scraping_time'] = now_timestamp
    data['has_detailed_data'] = 1
    data['needs_detail_scraping'] = 0
    
    query = """
        INSERT OR REPLACE INTO listing_tracking (
            "id", "ListingObjType", "roomTypeCategory", 
            "title", "name", "picture", "checkin", "checkout", "price", "discounted_price", "original_price", "link", "scraping_time",
            "needs_detail_scraping", "has_detailed_data",
            reviewsCount, averageRating, host, "airbnbLuxe", "location", "maxGuestCapacity", "isGuestFavorite",
            "lat", "lng", "isSuperhost", "isVerified", "ratingCount", "userId", "years", "months", "hostrAtingAverage"
        ) VALUES (
            :id, :ListingObjType, :roomTypeCategory, :title, :name, :picture, :checkin, 
            :checkout, :price, :discounted_price, :original_price, :link, :scraping_time,
            :needs_detail_scraping, :has_detailed_data,
            :reviewsCount, :averageRating, :host, :airbnbLuxe, :location, :maxGuestCapacity, :isGuestFavorite,
            :lat, :lng, :isSuperhost, :isVerified, :ratingCount, :userId, :years, :months, :hostrAtingAverage
        )
    """
    cur = db.cursor()
    cur.execute(query, data)
    db.commit()

def insert_basic_listing(db: sqlite3.Connection, data: dict):
    """Insert listing with just search result data, no detailed PDP API call needed"""
    now = datetime.datetime.now()
    now_timestamp = int(now.timestamp())
    data['scraping_time'] = now_timestamp
    data['has_detailed_data'] = 0
    data['needs_detail_scraping'] = 1
    
    # Set default values for fields that require PDP API
    defaults = {
        'reviewsCount': 0,
        'averageRating': 0.0,
        'host': None,
        'airbnbLuxe': False,
        'location': None,
        'maxGuestCapacity': 0,
        'isGuestFavorite': False,
        'lat': None,
        'lng': None,
        'isSuperhost': False,
        'isVerified': False,
        'ratingCount': 0,
        'userId': None,
        'years': 0,
        'months': 0,
        'hostrAtingAverage': 0.0
    }
    
    # Apply defaults for missing fields
    for key, default_value in defaults.items():
        if key not in data:
            data[key] = default_value
    
    query = """
        INSERT OR REPLACE INTO listing_tracking (
            "id", "ListingObjType", "roomTypeCategory", 
            "title", "name", "picture", "checkin", "checkout", "price", "discounted_price", "original_price", "link", "scraping_time",
            "needs_detail_scraping", "has_detailed_data",
            reviewsCount, averageRating, host, "airbnbLuxe", "location", "maxGuestCapacity", "isGuestFavorite",
            "lat", "lng", "isSuperhost", "isVerified", "ratingCount", "userId", "years", "months", "hostrAtingAverage"
        ) VALUES (
            :id, :ListingObjType, :roomTypeCategory, :title, :name, :picture, :checkin, 
            :checkout, :price, :discounted_price, :original_price, :link, :scraping_time,
            :needs_detail_scraping, :has_detailed_data,
            :reviewsCount, :averageRating, :host, :airbnbLuxe, :location, :maxGuestCapacity, :isGuestFavorite,
            :lat, :lng, :isSuperhost, :isVerified, :ratingCount, :userId, :years, :months, :hostrAtingAverage
        )
    """
    cur = db.cursor()
    cur.execute(query, data)
    db.commit()

def update_listing_with_details(db: sqlite3.Connection, listing_id: str, detail_data: dict):
    """Update existing basic listing with detailed data from PDP API"""
    detail_data['has_detailed_data'] = 1
    detail_data['needs_detail_scraping'] = 0
    
    query = """
        UPDATE listing_tracking SET
            reviewsCount = :reviewsCount,
            averageRating = :averageRating,
            host = :host,
            airbnbLuxe = :airbnbLuxe,
            location = :location,
            maxGuestCapacity = :maxGuestCapacity,
            isGuestFavorite = :isGuestFavorite,
            lat = :lat,
            lng = :lng,
            isSuperhost = :isSuperhost,
            isVerified = :isVerified,
            ratingCount = :ratingCount,
            userId = :userId,
            years = :years,
            months = :months,
            hostrAtingAverage = :hostrAtingAverage,
            has_detailed_data = :has_detailed_data,
            needs_detail_scraping = :needs_detail_scraping
        WHERE id = :id
    """
    detail_data['id'] = listing_id
    cur = db.cursor()
    cur.execute(query, detail_data)
    db.commit()

def mark_listing_for_detailed_scraping(db: sqlite3.Connection, listing_id: str):
    """Mark a listing that needs detailed scraping later"""
    query = """
        UPDATE listing_tracking 
        SET needs_detail_scraping = 1 
        WHERE id = ?
    """
    cur = db.cursor()
    cur.execute(query, (listing_id,))
    db.commit()

def get_listings_needing_details(db: sqlite3.Connection, limit: int = 100):
    """Get listings that need detailed scraping"""
    query = """
        SELECT id, link, title FROM listing_tracking 
        WHERE needs_detail_scraping = 1 AND has_detailed_data = 0
        ORDER BY scraping_time DESC
        LIMIT ?
    """
    cur = db.cursor()
    cur.execute(query, (limit,))
    return cur.fetchall()

def get_basic_listings_count(db: sqlite3.Connection):
    """Get count of listings with only basic data"""
    query = """
        SELECT COUNT(*) FROM listing_tracking 
        WHERE has_detailed_data = 0
    """
    cur = db.cursor()
    cur.execute(query)
    return cur.fetchone()[0]

def get_detailed_listings_count(db: sqlite3.Connection):
    """Get count of listings with detailed data"""
    query = """
        SELECT COUNT(*) FROM listing_tracking 
        WHERE has_detailed_data = 1
    """
    cur = db.cursor()
    cur.execute(query)
    return cur.fetchone()[0]

def insert_new_boundaries_tracking(db: sqlite3.Connection, data: dict):
    now = datetime.datetime.now()
    now_timestamp = int(now.timestamp())
    data['timestamp'] = now_timestamp
    cur = db.cursor()
    cur.execute("""
        SELECT * FROM boundaries_tracking where id = ?;
    """, (data['id'],))
    result = cur.fetchone()
    if result is None:
        query = """
            INSERT INTO boundaries_tracking (
                id, xmin, xmax, ymin, ymax, timestamp, total
            ) VALUES (
                :id, :xmin, :xmax, :ymin, :ymax, :timestamp, :total
            );
        """
    else:
        query = """
            UPDATE boundaries_tracking
            SET total = :total, timestamp = :timestamp
            WHERE id = :id;
        """
    cur.execute(query, data)
    db.commit()

def check_if_listing_exists(db: sqlite3.Connection, listing_id: str):
    """Check if listing exists (regardless of detail level)"""
    now = datetime.datetime.now()
    delta = datetime.timedelta(days=Config.UPDATE_WINDOW_DAYS_LISTING)
    start_time = int((now - delta).timestamp())
    query = """
        SELECT * 
          FROM listing_tracking 
         WHERE id = ? AND scraping_time >= ?;
    """
    cur = db.cursor()
    cur.execute(query, (listing_id, start_time,))
    rows = cur.fetchall()
    return len(rows) > 0

def check_if_detailed_listing_exists(db: sqlite3.Connection, listing_id: str):
    """Check if listing exists with detailed data"""
    now = datetime.datetime.now()
    delta = datetime.timedelta(days=Config.UPDATE_WINDOW_DAYS_LISTING)
    start_time = int((now - delta).timestamp())
    query = """
        SELECT * 
          FROM listing_tracking 
         WHERE id = ? AND scraping_time >= ? AND has_detailed_data = 1;
    """
    cur = db.cursor()
    cur.execute(query, (listing_id, start_time,))
    rows = cur.fetchall()
    return len(rows) > 0

def check_if_boundaries_exists(db: sqlite3.Connection, _id: int):
    now = datetime.datetime.now()
    delta = datetime.timedelta(days=Config.UPDATE_WINDOW_DAYS_BOUNDARY)
    start_time = int((now - delta).timestamp())
    query = """
        SELECT *
          FROM boundaries_tracking
          WHERE id = ? AND timestamp >= ?;
    """
    cur = db.cursor()
    cur.execute(query, (_id, start_time,))
    rows = cur.fetchall()
    return len(rows) > 0

def export_all_listings(db: sqlite3.Connection):
    min_time = datetime.datetime.now() - datetime.timedelta(days=1)
    min_time_timestamp = int(min_time.timestamp())
    query = """
        SELECT *
        FROM (
            SELECT *,
                   ROW_NUMBER() OVER (PARTITION BY id ORDER BY scraping_time DESC) AS rn
            FROM listing_tracking
            WHERE scraping_time >= ?
        )
        WHERE rn = 1;
    """
    cur = db.cursor()
    cur.execute(query, (min_time_timestamp,))
    return cur.fetchall()

def export_listings_by_type(db: sqlite3.Connection, detailed_only: bool = False):
    """Export listings with option to filter by detail level"""
    min_time = datetime.datetime.now() - datetime.timedelta(days=1)
    min_time_timestamp = int(min_time.timestamp())
    
    detail_filter = "AND has_detailed_data = 1" if detailed_only else ""
    
    query = f"""
        SELECT *
        FROM (
            SELECT *,
                   ROW_NUMBER() OVER (PARTITION BY id ORDER BY scraping_time DESC) AS rn
            FROM listing_tracking
            WHERE scraping_time >= ? {detail_filter}
        )
        WHERE rn = 1;
    """
    cur = db.cursor()
    cur.execute(query, (min_time_timestamp,))
    return cur.fetchall()

def get_tracking(db: sqlite3.Connection):
    cur = db.cursor()
    cur.execute("""
        SELECT * FROM tracking limit 1;
    """)
    result = cur.fetchone()
    if result is None:
        cur.execute("""
        INSERT INTO tracking (tracking) VALUES (0);
        """)
        db.commit()
        return 0
    else:
        return result[0]

def update_tracking(db: sqlite3.Connection, tracking: int):
    cur = db.cursor()
    cur.execute("""
    UPDATE tracking SET tracking = ?;
    """, (tracking,))
    db.commit()

def get_scraping_stats(db: sqlite3.Connection):
    """Get comprehensive scraping statistics"""
    cur = db.cursor()
    
    stats = {}
    
    # Total listings
    cur.execute("SELECT COUNT(*) FROM listing_tracking")
    stats['total_listings'] = cur.fetchone()[0]
    
    # Basic vs detailed
    cur.execute("SELECT COUNT(*) FROM listing_tracking WHERE has_detailed_data = 0")
    stats['basic_only'] = cur.fetchone()[0]
    
    cur.execute("SELECT COUNT(*) FROM listing_tracking WHERE has_detailed_data = 1")
    stats['with_details'] = cur.fetchone()[0]
    
    # Pending detail scraping
    cur.execute("SELECT COUNT(*) FROM listing_tracking WHERE needs_detail_scraping = 1 AND has_detailed_data = 0")
    stats['pending_details'] = cur.fetchone()[0]
    
    # Boundaries processed
    cur.execute("SELECT COUNT(*) FROM boundaries_tracking")
    stats['boundaries_processed'] = cur.fetchone()[0]
    
    # Recent activity (last 24 hours)
    recent_time = int((datetime.datetime.now() - datetime.timedelta(days=1)).timestamp())
    cur.execute("SELECT COUNT(*) FROM listing_tracking WHERE scraping_time >= ?", (recent_time,))
    stats['recent_listings'] = cur.fetchone()[0]
    
    return stats