# test_profile_extraction.py
"""
Quick test script to verify profile data extraction is working
"""
import time
from playwright.sync_api import sync_playwright
from undetected_playwright import Tarnished
import Config
import host_utils as Utils
import ScrapingUtils
import logging

def test_profile_extraction(host_url: str):
    """Test profile extraction for a specific host"""
    
    logger = Utils.setup_logger()
    logger.setLevel(logging.DEBUG)  # More verbose
    
    print(f"🧪 TESTING PROFILE EXTRACTION")
    print(f"URL: {host_url}")
    print("=" * 60)
    
    with sync_playwright() as p:
        browser = p.chromium.launch(
            headless=False,  # Keep visible for debugging
            proxy=Config.CONFIG_PROXY,
            args=[
                "--disable-features=Translate,TranslateUI,LanguageSettings",
                "--lang=en-US",
            ],
        )
        context = browser.new_context(
            viewport={"width": 1400, "height": 900},
            locale="en-US",
            extra_http_headers={"Accept-Language": "en-US,en;q=0.9"},
        )
        context = Tarnished.apply_stealth(context)
        
        try:
            print("📄 Opening host page for DOM extraction...")
            page = context.new_page()
            page.goto(host_url, wait_until="domcontentloaded", timeout=60000)
            
            # Dismiss popups
            ScrapingUtils._dismiss_any_popups_enhanced(page, logger, max_attempts=4)
            page.wait_for_timeout(3000)
            
            # Test DOM extraction directly
            print("\n🔍 TESTING DOM EXTRACTION:")
            print("-" * 30)
            dom_profile = Utils.extract_profile_from_dom(page, logger)
            
            for key, value in dom_profile.items():
                if value:
                    if isinstance(value, str) and len(value) > 100:
                        print(f"✅ {key}: {value[:100]}...")
                    elif isinstance(value, list) and value:
                        print(f"✅ {key}: {len(value)} items")
                    else:
                        print(f"✅ {key}: {value}")
                else:
                    print(f"❌ {key}: No data")
            
            page.close()
            
            # Test full GraphQL capture
            print(f"\n🌐 TESTING GRAPHQL + DOM CAPTURE:")
            print("-" * 40)
            
            cap = Utils.capture_host_graphql(
                context,
                host_url,
                logger,
                dismiss_fn=ScrapingUtils._dismiss_any_popups_enhanced
            )
            
            dom_data = cap.get("dom_profile", {})
            graphql_responses = cap.get("profile_jsons", [])
            
            print(f"📊 GraphQL responses captured: {len(graphql_responses)}")
            print(f"📊 DOM profile fields: {len([k for k,v in dom_data.items() if v])}")
            
            # Parse combined data
            final_profile = Utils.parse_host_profile_from_jsons(
                graphql_responses, 
                logger, 
                dom_fallback=dom_data
            )
            
            print(f"\n🎯 FINAL COMBINED PROFILE:")
            print("-" * 30)
            
            for key, value in final_profile.items():
                if value:
                    if isinstance(value, str) and len(value) > 100:
                        print(f"✅ {key}: {value[:100]}...")
                    elif isinstance(value, list) and value:
                        print(f"✅ {key}: {len(value)} items - {value[:2]}...")
                    else:
                        print(f"✅ {key}: {value}")
                else:
                    print(f"❌ {key}: No data")
            
            # Summary
            populated_fields = len([k for k,v in final_profile.items() if v])
            total_fields = len(final_profile)
            
            print(f"\n📈 SUMMARY:")
            print(f"Fields populated: {populated_fields}/{total_fields} ({100*populated_fields/total_fields:.1f}%)")
            
            # Key data check
            essential_fields = ["name", "profilePhoto", "about", "ratingAverage", "isSuperhost"]
            essential_found = sum(1 for field in essential_fields if final_profile.get(field))
            
            print(f"Essential fields found: {essential_found}/{len(essential_fields)}")
            
            if essential_found >= 3:
                print("🎉 SUCCESS: Profile extraction working well!")
            elif essential_found >= 1:
                print("⚠️  PARTIAL: Some profile data found, needs improvement")
            else:
                print("❌ FAILURE: No essential profile data found")
                
        except Exception as e:
            print(f"❌ ERROR: {e}")
            logger.exception("Test failed")
        finally:
            try:
                context.close()
                browser.close()
            except Exception:
                pass

if __name__ == "__main__":
    import sys
    if len(sys.argv) < 2:
        print("Usage: python test_profile_extraction.py <host_profile_url>")
        print("Example: python test_profile_extraction.py https://www.airbnb.com/users/show/532236013")
        sys.exit(1)
    
    test_profile_extraction(sys.argv[1])