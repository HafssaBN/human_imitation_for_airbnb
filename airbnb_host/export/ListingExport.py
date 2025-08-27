import csv
import sqlite3
from ..config import HostConfig
import os 
def main():
    """
    Exports detailed data for all scraped listings into a CSV file.
    """
    conn = sqlite3.connect(HostConfig.CONFIG_DB_FILE)
    cur = conn.cursor()

    # Query the listing_tracking table for all detailed listings
    cur.execute("""
        SELECT
            id,
            title,
            link,
            price,
            discounted_price,
            roomTypeCategory,
            averageRating,
            reviewsCount,
            maxGuestCapacity,
            location,
            host,
            userId,
            isSuperhost,
            lat,
            lng
        FROM listing_tracking
        WHERE has_detailed_data = 1
        ORDER BY scraping_time DESC
    """)
    
    listings = cur.fetchall()
    
    if not listings:
        print("❌ No detailed listings found in the database to export.")
        return

    print(f"📊 Found {len(listings)} detailed listings to export.")

    # Define the output file name
    output_file = os.path.join(HostConfig.BASE_DIR, 'output', 'listings_details.csv')

    with open(output_file, "w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)

        # Write the header row
        header = [
            "listing_id", "title", "url", "price", "discounted_price", 
            "room_type", "rating_average", "reviews_count", "max_guests",
            "location", "host_name", "host_id", "host_is_superhost", 
            "latitude", "longitude"
        ]
        writer.writerow(header)

        # Write the data rows
        writer.writerows(listings)

    conn.close()
    print(f"✅ Exported {len(listings)} listings -> {output_file}")

if __name__ == "__main__":
    main()