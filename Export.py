import csv
import sqlite3
import Config
import SQL
import re
from openpyxl.cell.cell import ILLEGAL_CHARACTERS_RE

def main():
    db = sqlite3.connect(Config.CONFIG_DB_FILE)
    
    # Updated columns list with host_url added
    columns = [
        'id','type','type_location','titre','nom','image','checkin','checkout',
        'prix','prix_promo','prix_original','lien','scrape_time','nbr_avis',
        'avg_evaluation','hote','airbnbLuxe','location','max_personnes',
        'isGuestFavorite','latitude','longitude','isSuperhost','isVerified',
        'nbr_evaluation','id_utilisateur','annees','mois','avg_hote_evaluation',
        'host_url'  # New column added
    ]
    
    with open(Config.CONFIG_OUTPUT_FILE, 'w', newline='', encoding='utf-8') as f:
        w = csv.writer(f)
        w.writerow(columns)
        
        for row in SQL.export_all_listings(db):
            clean = [re.sub(ILLEGAL_CHARACTERS_RE, '', x) if isinstance(x, str) else x
                    for x in row]
            # optional sanity check
            if len(clean) != len(columns):
                print(f"WARNING: row has {len(clean)} values, header has {len(columns)}")
            w.writerow(clean)
    
    db.close()
    print(f"✅ Export completed with host URLs added to {Config.CONFIG_OUTPUT_FILE}")

if __name__ == '__main__':
    main()