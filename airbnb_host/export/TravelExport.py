
import csv
import sqlite3
import os
from ..config import HostConfig as Config

def main():
    """
    Exports all scraped host travel history into a CSV file.
    """
    conn = sqlite3.connect(Config.CONFIG_DB_FILE)
    cur = conn.cursor()

    # Query the host_travels table
    cur.execute("""
        SELECT
            userId,
            place,
            country,
            trips,
            when_label
        FROM host_travels
        ORDER BY userId
    """)
    
    travels = cur.fetchall()
    
    if not travels:
        print("❌ No travel history found in the database to export.")
        return

    print(f"📊 Found {len(travels)} travel records to export.")

    # Define the output file path
    output_file = os.path.join(Config.BASE_DIR, 'output', 'travels_details.csv')

    with open(output_file, "w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)

        # Write the header row
        header = ["host_id", "place_visited", "country", "number_of_trips", "date_of_visit"]
        writer.writerow(header)

        # Write the data rows
        writer.writerows(travels)

    conn.close()
    print(f"✅ Exported {len(travels)} travel records -> {output_file}")

if __name__ == "__main__":
    main()