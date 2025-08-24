# HostExport.py
import csv
import sqlite3
import Config

def main():
    conn = sqlite3.connect(Config.CONFIG_DB_FILE)
    cur = conn.cursor()
    cur.execute("""
        SELECT userId, name, isSuperhost, isVerified, ratingAverage, ratingCount,
               years, months, total_listings, profile_url, scraping_time
        FROM host_tracking
        ORDER BY scraping_time DESC
    """)
    rows = cur.fetchall()
    out = getattr(Config, "CONFIG_HOST_OUTPUT_FILE", "hosts.csv")
    with open(out, "w", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        w.writerow([
            "id_utilisateur","nom","isSuperhost","isVerified","avg_hote_evaluation",
            "nbr_evaluation","annees","mois","total_listings","host_url","scrape_time"
        ])
        w.writerows(rows)
    conn.close()
    print(f"✅ exported {len(rows)} hosts -> {out}")

if __name__ == "__main__":
    main()
