# export_csv.py
import csv
import Utils, SQL

DEFAULT_COLUMNS = [
    "id","title","price","discounted_price","original_price","link","location",
    "averageRating","reviewsCount","isSuperhost","isVerified","lat","lng",
    "has_detailed_data","needs_detail_scraping","scraping_time"
]

def fetch_rows(db, detailed_only=False):
    rows = SQL.export_listings_by_type(db, detailed_only=detailed_only)
    return rows

def main(path="listings.csv", detailed_only=False, columns=None):
    db = Utils.connect_db()
    try:
        cols = columns or DEFAULT_COLUMNS
        rows = fetch_rows(db, detailed_only)
        if not rows:
            print("No rows to export.")
            return
        # rows are sqlite Row objects – ensure all columns exist
        with open(path, "w", newline="", encoding="utf-8") as f:
            w = csv.DictWriter(f, fieldnames=cols)
            w.writeheader()
            for r in rows:
                out = {c: r[c] if c in r.keys() else None for c in cols}
                w.writerow(out)
        print(f"✓ Exported {len(rows)} rows to {path}")
    finally:
        db.close()

if __name__ == "__main__":
    import argparse
    p = argparse.ArgumentParser()
    p.add_argument("--out", default="listings.csv", help="CSV file path")
    p.add_argument("--detailed-only", action="store_true", help="Export only rows with detailed data")
    args = p.parse_args()
    main(args.out, args.detailed_only)
