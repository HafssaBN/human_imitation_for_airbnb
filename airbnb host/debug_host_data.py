# debug_host_data.py
import sqlite3
import json
from typing import Dict, List, Any
import Config

def analyze_host_data(user_id: str = None):
    """Analyze scraped host data to understand discrepancies"""
    
    conn = sqlite3.connect(Config.CONFIG_DB_FILE)
    cur = conn.cursor()
    
    print("🔍 HOST DATA ANALYSIS")
    print("=" * 50)
    
    # Get all hosts or specific host
    if user_id:
        cur.execute("SELECT * FROM host_tracking WHERE userId=?", (user_id,))
        hosts = cur.fetchall()
        print(f"📊 Analyzing host: {user_id}")
    else:
        cur.execute("SELECT * FROM host_tracking ORDER BY scraping_time DESC")
        hosts = cur.fetchall()
        print(f"📊 Found {len(hosts)} total hosts in database")
    
    if not hosts:
        print("❌ No host data found!")
        return
    
    # Get column names for host_tracking
    cur.execute("PRAGMA table_info(host_tracking)")
    host_columns = [col[1] for col in cur.fetchall()]
    
    for host_row in hosts:
        host_dict = dict(zip(host_columns, host_row))
        user_id = host_dict['userId']
        
        print(f"\n👤 HOST: {host_dict.get('name', 'Unknown')} (ID: {user_id})")
        print("-" * 30)
        
        # Show profile data
        print("📝 PROFILE DATA:")
        print(f"   Name: {host_dict.get('name', 'N/A')}")
        print(f"   Superhost: {'Yes' if host_dict.get('isSuperhost') else 'No'}")
        print(f"   Verified: {'Yes' if host_dict.get('isVerified') else 'No'}")
        print(f"   Rating: {host_dict.get('ratingAverage', 'N/A')} ({host_dict.get('ratingCount', 0)} reviews)")
        print(f"   Experience: {host_dict.get('years', 0)} years, {host_dict.get('months', 0)} months")
        print(f"   Profile Photo: {'Yes' if host_dict.get('profile_photo_url') else 'No'}")
        print(f"   Bio Text: {'Yes' if host_dict.get('about_text') else 'No'} ({len(host_dict.get('about_text') or '')} chars)")
        
        # Show listings data
        cur.execute("SELECT COUNT(*) FROM host_listings WHERE userId=?", (user_id,))
        actual_listings_count = cur.fetchone()[0]
        
        cur.execute("SELECT listingId, listingUrl FROM host_listings WHERE userId=? LIMIT 5", (user_id,))
        sample_listings = cur.fetchall()
        
        print(f"\n🏠 LISTINGS DATA:")
        print(f"   Declared total: {host_dict.get('total_listings', 'N/A')}")
        print(f"   Actually scraped: {actual_listings_count}")
        print(f"   Discrepancy: {(host_dict.get('total_listings', 0) or 0) - actual_listings_count}")
        
        if sample_listings:
            print(f"   Sample listings:")
            for listing_id, listing_url in sample_listings:
                print(f"     - {listing_id}: {listing_url}")
            if actual_listings_count > 5:
                print(f"     ... and {actual_listings_count - 5} more")
        
        # Show additional data
        cur.execute("SELECT COUNT(*) FROM host_guidebooks WHERE userId=?", (user_id,))
        guidebooks_count = cur.fetchone()[0]
        
        cur.execute("SELECT COUNT(*) FROM host_travels WHERE userId=?", (user_id,))
        travels_count = cur.fetchone()[0]
        
        cur.execute("SELECT COUNT(*) FROM host_reviews WHERE userId=?", (user_id,))
        reviews_count = cur.fetchone()[0]
        
        print(f"\n📚 ADDITIONAL DATA:")
        print(f"   Guidebooks: {guidebooks_count}")
        print(f"   Places visited: {travels_count}")
        print(f"   Reviews collected: {reviews_count}")
        
        # Check if listings exist in main listing_tracking table
        cur.execute("""
            SELECT COUNT(*) FROM listing_tracking lt 
            JOIN host_listings hl ON lt.id = hl.listingId 
            WHERE hl.userId=?
        """, (user_id,))
        detailed_listings = cur.fetchone()[0]
        
        print(f"\n🔍 LISTING DETAILS:")
        print(f"   Listings with full data: {detailed_listings}/{actual_listings_count}")
        
        if detailed_listings > 0:
            cur.execute("""
                SELECT lt.id, lt.title, lt.host, lt.location 
                FROM listing_tracking lt 
                JOIN host_listings hl ON lt.id = hl.listingId 
                WHERE hl.userId=? AND lt.has_detailed_data=1
                LIMIT 3
            """, (user_id,))
            detailed_sample = cur.fetchall()
            
            if detailed_sample:
                print("   Sample detailed listings:")
                for lid, title, host_name, location in detailed_sample:
                    print(f"     - {lid}: {title or 'No title'} | Host: {host_name or 'N/A'} | Location: {location or 'N/A'}")
    
    # Overall statistics
    print(f"\n📊 OVERALL STATISTICS:")
    print("=" * 30)
    
    cur.execute("SELECT COUNT(*) FROM host_tracking")
    total_hosts = cur.fetchone()[0]
    
    cur.execute("SELECT COUNT(*) FROM host_listings")
    total_listings = cur.fetchone()[0]
    
    cur.execute("SELECT COUNT(*) FROM host_tracking WHERE profile_photo_url IS NOT NULL AND profile_photo_url != ''")
    hosts_with_photos = cur.fetchone()[0]
    
    cur.execute("SELECT COUNT(*) FROM host_tracking WHERE about_text IS NOT NULL AND about_text != ''")
    hosts_with_bios = cur.fetchone()[0]
    
    cur.execute("SELECT AVG(total_listings) FROM host_tracking WHERE total_listings IS NOT NULL")
    avg_declared_listings = cur.fetchone()[0] or 0
    
    if total_hosts > 0:
        avg_actual_listings = total_listings / total_hosts
    else:
        avg_actual_listings = 0
    
    print(f"Total hosts: {total_hosts}")
    print(f"Total listings found: {total_listings}")
    print(f"Average declared listings per host: {avg_declared_listings:.1f}")
    print(f"Average actual listings per host: {avg_actual_listings:.1f}")
    print(f"Hosts with profile photos: {hosts_with_photos}/{total_hosts} ({100*hosts_with_photos/max(total_hosts,1):.1f}%)")
    print(f"Hosts with bio text: {hosts_with_bios}/{total_hosts} ({100*hosts_with_bios/max(total_hosts,1):.1f}%)")
    
    # Show table structures
    print(f"\n🗄️  DATABASE STRUCTURE:")
    print("=" * 30)
    
    for table in ['host_tracking', 'host_listings', 'host_guidebooks', 'host_travels', 'host_reviews']:
        cur.execute(f"SELECT COUNT(*) FROM {table}")
        count = cur.fetchone()[0]
        print(f"{table}: {count} rows")
    
    conn.close()

def fix_listing_count_discrepancy(user_id: str):
    """Fix discrepancy between declared and actual listing counts"""
    print(f"🔧 FIXING LISTING COUNT FOR USER {user_id}")
    
    conn = sqlite3.connect(Config.CONFIG_DB_FILE)
    cur = conn.cursor()
    
    # Get actual count
    cur.execute("SELECT COUNT(*) FROM host_listings WHERE userId=?", (user_id,))
    actual_count = cur.fetchone()[0]
    
    # Update host_tracking
    cur.execute("""
        UPDATE host_tracking 
        SET total_listings = ? 
        WHERE userId = ?
    """, (actual_count, user_id))
    
    conn.commit()
    conn.close()
    
    print(f"✅ Updated total_listings to {actual_count} for user {user_id}")

if __name__ == "__main__":
    import sys
    
    if len(sys.argv) > 1:
        if sys.argv[1] == "fix" and len(sys.argv) > 2:
            fix_listing_count_discrepancy(sys.argv[2])
        else:
            analyze_host_data(sys.argv[1])
    else:
        analyze_host_data()
        
    print(f"\n💡 USAGE TIPS:")
    print("python debug_host_data.py                    # Analyze all hosts")
    print("python debug_host_data.py 532236013         # Analyze specific host")
    print("python debug_host_data.py fix 532236013     # Fix listing count for host")