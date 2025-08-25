# HostExport.py
import csv
import sqlite3
import Config

def _fetch_host_rows(conn):
    cur = conn.cursor()
    cur.execute("""
        SELECT
          h.userId, h.name, h.isSuperhost, h.isVerified, h.ratingAverage, h.ratingCount,
          h.years, h.months,
          COALESCE((SELECT COUNT(*) FROM host_listings l WHERE l.userId=h.userId), 0) AS actual_count,
          h.profile_url, h.scraping_time,
          h.profile_photo_url, h.about_text, h.bio_text
        FROM host_tracking h
        ORDER BY h.scraping_time DESC
    """)
    return cur.fetchall()

def _fetch_listings_for_host(conn, user_id):
    cur = conn.cursor()
    cur.execute("""
        SELECT listingId, listingUrl
        FROM host_listings
        WHERE userId = ?
        ORDER BY CAST(listingId AS TEXT)
    """, (user_id,))
    return cur.fetchall()

def _fetch_guidebooks_for_host(conn, user_id):
    cur = conn.cursor()
    cur.execute("""
        SELECT title, url
        FROM host_guidebooks
        WHERE userId = ?
        ORDER BY rowid
    """, (user_id,))
    return cur.fetchall()

def _fetch_travels_for_host(conn, user_id):
    cur = conn.cursor()
    cur.execute("""
        SELECT place, country, trips, when_label
        FROM host_travels
        WHERE userId = ?
        ORDER BY rowid
    """, (user_id,))
    return cur.fetchall()

def _fetch_reviews_count(conn, user_id):
    cur = conn.cursor()
    cur.execute("SELECT COUNT(*) FROM host_reviews WHERE userId = ?", (user_id,))
    return cur.fetchone()[0]

def main():
    conn = sqlite3.connect(Config.CONFIG_DB_FILE)
    hosts = _fetch_host_rows(conn)

    # figure max listing columns needed
    max_listings = 0
    for h in hosts:
        uid = h[0]
        max_listings = max(max_listings, len(_fetch_listings_for_host(conn, uid)))

    print(f"📊 Found {len(hosts)} hosts to export")
    print(f"📊 Max listings found: {max_listings}, will export {max_listings} columns")

    out = getattr(Config, "CONFIG_HOST_OUTPUT_FILE", "hosts.csv")
    with open(out, "w", newline="", encoding="utf-8") as f:
        w = csv.writer(f)

        # header
        base_cols = [
            "id_utilisateur","nom","isSuperhost","isVerified","avg_hote_evaluation",
            "nbr_evaluation","annees","mois",
            "actual_listings_count",
            "host_url","scrape_time",
            "profile_photo_url","about_text","bio_text",
            "guidebooks_count","guidebooks_details",
            "visited_places_count","visited_places_details",
            "reviews_count"
        ]
        listing_cols = []
        for i in range(1, max_listings + 1):
            listing_cols += [f"listing_id_{i}", f"listing_url_{i}"]

        w.writerow(base_cols + listing_cols)

        # rows
        for row in hosts:
            user_id = row[0]
            guidebooks = _fetch_guidebooks_for_host(conn, user_id)
            travels = _fetch_travels_for_host(conn, user_id)
            listings = _fetch_listings_for_host(conn, user_id)
            reviews_count = _fetch_reviews_count(conn, user_id)

            guide_str = " | ".join([f"{t}: {u}" for (t, u) in guidebooks])
            travel_str = " | ".join([f"{p}, {c} — {wl or ''}".strip() for (p, c, _trips, wl) in travels])

            base = list(row) + [
                len(guidebooks), guide_str,
                len(travels), travel_str,
                reviews_count
            ]

            # flatten listing pairs and pad
            flat = []
            for (lid, url) in listings:
                flat += [lid, url]
            while len(flat) < 2 * max_listings:
                flat += ["", ""]

            w.writerow(base + flat)

    conn.close()
    print(f"✅ Exported {len(hosts)} hosts -> {out}")
    if hosts:
        print(f"📈 Average listings per host: {sum(_fetch_reviews_count(sqlite3.connect(Config.CONFIG_DB_FILE), h[0]) * 0 + len(_fetch_listings_for_host(sqlite3.connect(Config.CONFIG_DB_FILE), h[0])) for h in hosts)/len(hosts):.1f}")
        print(f"📈 Host with most listings: {max_listings}")
        print(f"📈 Hosts with profile data: {len([h for h in hosts if h[11] or h[12] or h[13]])}/{len(hosts)}")

if __name__ == "__main__":
    main()
