
import csv
import sqlite3
import os
from ..config import HostConfig as Config

def main():
    """
    Exports detailed data for all scraped reviews into a CSV file.
    """
    conn = sqlite3.connect(Config.CONFIG_DB_FILE)
    cur = conn.cursor()

    # Query the host_reviews table
    cur.execute("""
        SELECT
            userId,
            reviewer_name,
            date_text,
            rating,
            text
        FROM host_reviews
        ORDER BY userId, rowid DESC
    """)
    
    reviews = cur.fetchall()
    
    if not reviews:
        print("❌ No reviews found in the database to export.")
        return

    print(f"📊 Found {len(reviews)} reviews to export.")

    # Define the output file path
    output_file = os.path.join(Config.BASE_DIR, 'output', 'reviews_details.csv')

    with open(output_file, "w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)

        # Write the header row
        header = ["host_id", "reviewer_name", "date", "rating", "review_text"]
        writer.writerow(header)

        # Write the data rows
        writer.writerows(reviews)

    conn.close()
    print(f"✅ Exported {len(reviews)} reviews -> {output_file}")

if __name__ == "__main__":
    main()