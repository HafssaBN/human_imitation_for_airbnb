
import csv
import sqlite3
import os
from ..config import HostConfig as Config

def main():
    """
    Exports all scraped host guidebooks into a CSV file.
    """
    conn = sqlite3.connect(Config.CONFIG_DB_FILE)
    cur = conn.cursor()

    # Query the host_guidebooks table
    cur.execute("""
        SELECT
            userId,
            title,
            url
        FROM host_guidebooks
        ORDER BY userId
    """)
    
    guidebooks = cur.fetchall()
    
    if not guidebooks:
        print("❌ No guidebooks found in the database to export.")
        return

    print(f"📊 Found {len(guidebooks)} guidebooks to export.")

    # Define the output file path
    output_file = os.path.join(Config.BASE_DIR, 'output', 'guidebooks_details.csv')

    with open(output_file, "w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)

        # Write the header row
        header = ["host_id", "guidebook_title", "guidebook_url"]
        writer.writerow(header)

        # Write the data rows
        writer.writerows(guidebooks)

    conn.close()
    print(f"✅ Exported {len(guidebooks)} guidebooks -> {output_file}")

if __name__ == "__main__":
    main()