import csv
import sqlite3
import Config
import SQL
import re
from openpyxl.cell.cell import ILLEGAL_CHARACTERS_RE

def main():
    db = sqlite3.connect(Config.CONFIG_DB_FILE)
    columns = [
        'id','type','type_location','titre','nom','image','checkin','checkout',
        'prix','prix_promo','prix_original','lien','scrape_time','nbr_avis',
        'avg_evaluation','hote','airbnbLuxe','location','max_personnes',
        'isGuestFavorite','latitude','longitude','isSuperhost','isVerified',
        'nbr_evaluation','id_utilisateur','annees','mois','avg_hote_evaluation'
    ]
    with open(Config.CONFIG_OUTPUT_FILE, 'w', newline='', encoding='utf-8') as f:
        w = csv.writer(f)
        w.writerow(columns[:-1])  # keep same slice you had in XLSX exporter
        for row in SQL.export_all_listings(db):
            clean = [re.sub(ILLEGAL_CHARACTERS_RE, '', x) if isinstance(x, str) else x
                     for x in row[:-1]]
            w.writerow(clean)
    db.close()

if __name__ == '__main__':
    main()
