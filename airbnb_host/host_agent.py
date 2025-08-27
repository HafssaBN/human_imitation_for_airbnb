# --- START OF FILE host_agent.py ---
import re
import time
import json
import urllib.parse
import logging
import random
from datetime import datetime, timedelta
from typing import List, Optional, Set, Dict, Union, Any

from playwright.sync_api import (
    sync_playwright, Page, BrowserContext, Browser, Route, Request, Locator
)
from undetected_playwright import Tarnished

from .config import HostConfig
from . import host_utils as Utils
from . import host_SQL as SQL
from . import HostScrapingUtils
from .HumanMouseMovement import HumanMouseMovement

# (All helper functions like _extract_pdp_token_from_request, _parse_user_id_from_url, etc. are restored here)
def _extract_pdp_token_from_request(req: Request) -> Optional[str]:
    try:
        parsed = urllib.parse.urlparse(req.url)
        parts = [p for p in parsed.path.split('/') if p]
        for i, pth in enumerate(parts):
            if pth == "StaysPdpSections" and i + 1 < len(parts):
                cand = parts[i + 1]
                if cand: return cand
        qs = urllib.parse.parse_qs(parsed.query)
        ext = qs.get('extensions', [None])[0]
        if ext:
            try:
                ext_obj = json.loads(ext)
                cand = (ext_obj.get('persistedQuery') or {}).get('sha256Hash')
                if cand: return cand
            except Exception: pass
        try:
            body = req.post_data_json
            if body:
                ext2 = body.get('extensions') or {}
                cand = (ext2.get('persistedQuery') or {}).get('sha256Hash')
                if cand: return cand
        except Exception: pass
    except Exception: pass
    return None

def _parse_user_id_from_url(host_url: str) -> Optional[str]:
    try:
        path = urllib.parse.urlparse(host_url).path.strip("/")
        parts = path.split("/")
        digits = [p for p in parts if p.isdigit()]
        return digits[-1] if digits else None
    except Exception: return None

def _dedupe_keep_order(items: List[str]) -> List[str]:
    seen: Set[str] = set()
    out: List[str] = []
    for it in items:
        if it not in seen:
            out.append(it)
            seen.add(it)
    return out

def _click_if_exists(search_root, selectors, logger, label):
    for sel in selectors:
        try:
            el = search_root.locator(sel).first
            if el.count():
                try: el.wait_for(state="visible", timeout=1200)
                except Exception: continue
                logger.info(f'[host] Clicking "{label}"')
                el.click(timeout=4000, force=True)
                try: (search_root.page if isinstance(search_root, Locator) else search_root).wait_for_timeout(600)
                except Exception: pass
                return True
        except Exception: continue
    return False

def _ensure_pdp_token_via_link(context: BrowserContext, logger: logging.Logger, link: str) -> bool:
    page = context.new_page()
    try:
        logger.info(f"[pdp-capture] Opening PDP link to capture token: {link}")
        page.goto(link, wait_until="domcontentloaded", timeout=60000)
        HostScrapingUtils._dismiss_any_popups_enhanced(page, logger, max_attempts=3)
        try: page.wait_for_load_state("networkidle", timeout=20000)
        except Exception: pass
        page.wait_for_timeout(1000)
        return True
    except Exception as e:
        logger.info(f"[pdp-capture] failed: {e}")
        return False
    finally:
        try: page.close()
        except Exception: pass

def _open_all_listings_and_expand(page: Page, logger: logging.Logger) -> None:
    _click_if_exists(page, ['a:has-text("View all listings")', 'button:has-text("View all listings")', 'text=/View all \\d+ listings/i'], logger, "View all listings")
    try: page.wait_for_selector('a[href*="/rooms/"]', timeout=10000)
    except Exception: pass
    max_clicks, stagnant_after_clicks = 12, 0
    def anchor_count() -> int:
        try: return page.locator('a[href*="/rooms/"]').count()
        except Exception: return 0
    last = anchor_count()
    show_more_selectors = ['button[aria-label="Show more results"]', 'a[aria-label="Show more results"]', 'button[data-testid="pagination-button-next"]', 'nav[aria-label="Pagination"] button:has-text("Show more")']
    for i in range(max_clicks):
        if not _click_if_exists(page, show_more_selectors, logger, "Show more results"): break
        try: page.wait_for_load_state("networkidle", timeout=8000)
        except Exception: pass
        for _ in range(3):
            try: page.mouse.wheel(0, random.randint(800, 1200))
            except Exception: break
            time.sleep(random.uniform(0.25, 0.4))
        curr = anchor_count()
        if curr <= last: stagnant_after_clicks += 1
        else: stagnant_after_clicks, last = 0, curr
        if stagnant_after_clicks >= 2: logger.info('[host] No new results after two "Show more" clicks — stopping.'); break

def _collect_room_links_from_dom(page: Page, logger: logging.Logger, max_scrolls: int = 60) -> List[str]:
    all_links, last_len = [], 0
    for i in range(max_scrolls):
        try:
            anchors = page.locator('a[href*="/rooms/"]')
            count = anchors.count()
            for idx in range(min(count, 800)):
                href = anchors.nth(idx).get_attribute("href") or ""
                if "/rooms/" in href:
                    if href.startswith("/"): href = "https://www.airbnb.com" + href
                    all_links.append(href.split("?")[0])
        except Exception: pass
        try: page.mouse.wheel(0, random.randint(900, 1400))
        except Exception: break
        time.sleep(random.uniform(0.25, 0.45))
        if len(all_links) == last_len and i > 10: break
        last_len = len(all_links)
    deduped = _dedupe_keep_order(all_links)
    logger.info(f"[host] Collected {len(deduped)} unique room links from DOM after {i+1} scrolls")
    return deduped

def _extract_about_and_bio(page: Page, logger: logging.Logger) -> Dict[str, Optional[str]]:
    out: Dict[str, Optional[str]] = {"about_text": None, "bio_text": None}
    sec = page.locator('section:has-text("About")').first
    if sec.count() == 0: logger.info("[host] About section not found"); return out
    _click_if_exists(sec, ['button:has-text("Show all")', 'a:has-text("Show all")'], logger, "Show all (About)")
    para_text: Optional[str] = None
    try:
        candidates, best_len, best_txt = sec.locator("p, div"), -1, None
        for i in range(candidates.count()):
            t = candidates.nth(i).inner_text().strip()
            if len(t) > best_len and ' ' in t and '\n' not in t: best_len, best_txt = len(t), t
        para_text = best_txt
    except Exception as e: logger.warning(f"[host] Error finding bio paragraph: {e}")
    about_text: Optional[str] = None
    try:
        full_text = sec.inner_text()
        remaining_text = full_text.replace(para_text, "") if para_text and para_text in full_text else full_text
        bullets = [line.strip() for line in remaining_text.splitlines() if line.strip() and "About" not in line and "Show all" not in line]
        about_text = "\n".join(_dedupe_keep_order(bullets))
    except Exception as e: logger.warning(f"[host] Error extracting bullet points: {e}")
    out["about_text"], out["bio_text"] = about_text, para_text
    logger.info(f"[host] About parsed -> bullets: {len(about_text.splitlines() if about_text else [])} | bio_len: {len(para_text or '')}")
    return out

def _extract_guidebooks(page: Page, logger: logging.Logger) -> List[Dict[str, str]]:
    cards, out, seen = page.locator('a[href*="/guidebooks"]'), [], set()
    try:
        for i in range(min(cards.count(), 24)):
            a = cards.nth(i)
            url = a.get_attribute("href") or ""
            title = a.inner_text().strip().replace("\n", " ")
            if url and title and url not in seen:
                if url.startswith("/"): url = "https://www.airbnb.com" + url
                out.append({"title": title, "url": url}); seen.add(url)
    except Exception: pass
    if out: logger.info(f"✅ Found {len(out)} guidebooks")
    return out

MONTHS = "|".join(["January","February","March","April","May","June","July","August","September","October","November","December","Jan","Feb","Mar","Apr","Jun","Jul","Aug","Sep","Sept","Oct","Nov","Dec"])

def _extract_travels(page: Page, logger: logging.Logger) -> List[Dict[str, Union[str, int]]]:
    sec: Optional[Locator] = None
    for sel in ['section:has(h2:has-text("Where")):has(h2:has-text("has been"))']:
        node = page.locator(sel).first
        if node.count(): sec = node; break
    if not sec: return []
    try: txt = sec.inner_text()
    except Exception: txt = ""
    if not txt: return []
    results: List[Dict[str, Union[str, int]]] = []
    lines = [l.strip() for l in txt.splitlines() if l.strip()]
    for i in range(len(lines) - 1):
        line1, line2 = lines[i], lines[i+1]
        if re.match(r"^[A-Za-zÀ-ÿ'.\- ]+,\s+[A-Za-zÀ-ÿ'.\- ]+$", line1):
            if re.search(rf"({MONTHS})\s+\d{{4}}", line2, re.IGNORECASE) or re.search(r"\b\d+\s+trips?\b", line2, re.IGNORECASE):
                city, country, trips = line1.split(",")[0].strip(), line1.split(",")[1].strip(), 0
                m = re.search(r"\b(\d+)\s+trips?\b", line2, re.IGNORECASE)
                if m: trips = int(m.group(1))
                results.append({"place": city, "country": country, "trips": trips, "when": line2})
    if results: logger.info(f"✅ Parsed {len(results)} visited places")
    return results

# --- In host_agent.py ---

def _extract_some_reviews(page: Page, logger: logging.Logger, max_reviews: int = 0) -> List[Dict[str, Any]]:
    """
    Finds and scrapes all host reviews, intelligently handling both modal and on-page layouts.
    This version uses a more resilient initial selector to find the reviews container.
    """
    try:
        # --- NEW, MORE ROBUST SELECTOR ---
        # Instead of 'section', we look for any 'div' containing the h2 reviews heading.
        # This is more resilient to Airbnb changing container tags.
        reviews_section_selector = 'div:has(> h2:text-matches("reviews", "i"))'
        reviews_section = page.locator(reviews_section_selector).first

        if reviews_section.count() == 0:
            logger.warning("[host] The primary reviews section container could not be found. No reviews will be scraped.")
            return []
            
        logger.info("[host] Scrolling to the reviews section...")
        reviews_section.scroll_into_view_if_needed()
        page.wait_for_timeout(1000)

        show_reviews_selector = 'button:text-matches("Show (all|more|[0-9]+).*reviews", "i")'
        clicked_button = _click_if_exists(reviews_section, [show_reviews_selector], logger, "Show ... reviews button")
        
        if not clicked_button:
            # If the button doesn't exist, the reviews might already be visible, so we don't need to exit.
            logger.warning("[host] Found the reviews section but could not find a 'Show reviews' button inside it. Will try to scrape visible reviews.")

    except Exception as e:
        logger.error(f"[host] An error occurred while trying to find and click the reviews button: {e}")
        return []

    page.wait_for_timeout(3000)

    # (The rest of this function is already robust and should now work correctly)
    root = None
    is_modal = False
    modal_selector = 'div[role="dialog"]:has(h2:text-matches("Reviews", "i"))'
    
    if page.locator(modal_selector).is_visible():
        root = page.locator(modal_selector)
        is_modal = True
    else:
        root = reviews_section
    
    if max_reviews == 0:
        for i in range(50):
            try:
                show_more_button = root.locator('button:has-text("Show more reviews")').first
                if show_more_button.is_visible(timeout=3000):
                    show_more_button.click(timeout=3000)
                    page.wait_for_load_state('networkidle', timeout=5000)
                else: break
            except Exception: break
    
    out: List[Dict[str, Any]] = []
    review_block_selector = 'div:has(> div h3):has(span[aria-label*="out of 5 stars"])'
    blocks = root.locator(review_block_selector)
    total = blocks.count()

    if total > 0:
        logger.info(f"[host] Found {total} review blocks. Parsing...")
        for i in range(total):
            b = blocks.nth(i)
            try:
                # (Extraction logic remains the same)
                reviewer_name = b.locator('h3').first.inner_text().strip()
                date_text = (b.locator('span:has-text-matches("ago|week|month|year|202", "i")').first.inner_text().strip())
                text_element = b.locator('span[data-testid="review-text-main-span"], .ll4r2nl').first
                text = text_element.inner_text().strip() if text_element.count() > 0 else ""
                rating = None
                try:
                    aria_label = b.locator('span[aria-label*="out of 5 stars"]').first.get_attribute('aria-label') or ""
                    rating_match = re.search(r'Rated\s*(\d\.?\d*)', aria_label)
                    if rating_match: rating = float(rating_match.group(1))
                except Exception: pass
                if text:
                    out.append({"reviewId": f"rev_{i}_{hash(text[:50])}", "reviewer_name": reviewer_name, "rating": rating, "date_text": date_text, "text": text})
            except Exception as e: continue

    if out: logger.info(f"✅ Successfully collected {len(out)} reviews.")
    else: logger.warning("[host] No review data could be extracted.")

    if is_modal:
        try:
            root.locator('button[aria-label="Close"]').first.click(timeout=3000)
        except Exception:
            page.keyboard.press("Escape")

    return out
def scrape_host(host_url: str):
    logger = Utils.setup_logger()
    db = Utils.connect_db()
    SQL.init_all_tables(db)

    HOST_MAX_LISTINGS = getattr(HostConfig, "HOST_MAX_LISTINGS_PER_RUN", 500)
    HOST_DETAIL_SCRAPE_LIMIT = getattr(HostConfig, "HOST_DETAIL_SCRAPE_LIMIT", 500)

    user_id = _parse_user_id_from_url(host_url)
    if not user_id:
        logger.error(f"[host] Could not parse user id from url: {host_url}")
        return

    logger.info(f"🔎 Host scrape start | userId={user_id} | url={host_url}")

    request_headers: Dict[str, str] = {}
    request_item_token: Optional[str] = None
    request_item_client_id: Optional[str] = None
    x_airbnb_api_key = "d306zoyjsyarp7ifhu67rjxn52tv0t20"
    request_client_version: Optional[str] = None
    checkin_date = (datetime.now() + timedelta(days=90)).strftime('%Y-%m-%d')
    checkout_date = (datetime.now() + timedelta(days=95)).strftime('%Y-%m-%d')
    logger.info(f"[host] Using default search dates: {checkin_date} to {checkout_date}")
    with sync_playwright() as p:
        browser: Browser = p.chromium.launch(headless=False, proxy=HostConfig.CONFIG_PROXY, args=["--disable-features=Translate,TranslateUI,LanguageSettings", "--lang=en-US", "--disable-infobars", "--disable-extensions", "--no-first-run", "--disable-default-apps"])
        context: BrowserContext = browser.new_context(viewport={"width": 1400, "height": 900}, locale="en-US", extra_http_headers={"Accept-Language": "en-US,en;q=0.9"})
        context = Tarnished.apply_stealth(context)

        def handle_request(route: Route):
            nonlocal request_item_token, request_item_client_id, request_headers, x_airbnb_api_key, request_client_version
            req = route.request
            if "/api/v3/StaysPdpSections" in req.url:
                token = _extract_pdp_token_from_request(req)
                if token and not request_item_token:
                    request_item_token, request_item_client_id = token, req.headers.get('x-client-request-id')
                    logger.info(f"[route] PDP token = {request_item_token}")
                request_headers = req.headers.copy()
                api_k = req.headers.get('x-airbnb-api-key')
                if api_k: x_airbnb_api_key = api_k
                request_client_version = req.headers.get('x-client-version') or request_client_version
            route.continue_()

        context.route('**/api/v3/*', handle_request)

        def on_request(req: Request):
            nonlocal request_item_token, request_item_client_id, request_headers, x_airbnb_api_key, request_client_version
            if "/api/v3/StaysPdpSections" in req.url:
                token = _extract_pdp_token_from_request(req)
                if token and not request_item_token:
                    request_item_token, request_item_client_id = token, req.headers.get('x-client-request-id')
                    logger.info(f"[event] PDP token captured = {request_item_token}")
                request_headers = req.headers.copy()
                api_k = req.headers.get('x-airbnb-api-key')
                if api_k: x_airbnb_api_key = api_k
                request_client_version = req.headers.get('x-client-version') or request_client_version

        context.on("request", on_request)

        page = context.new_page()
        page.set_default_timeout(60000)
        logger.info(f"[host] Visiting host page…")
        page.goto(host_url, wait_until='domcontentloaded', timeout=60000)
        HostScrapingUtils._dismiss_any_popups_enhanced(page, logger, max_attempts=4)

        profile_photo_url: Optional[str] = None
        try:
            avatar = page.locator('img[src*="/user/"]').first
            if avatar.count() and avatar.is_visible(): profile_photo_url = avatar.get_attribute("src")
        except Exception: pass
        is_super = 1 if page.locator(':text("Superhost")').count() else 0
        is_ver = 1 if page.locator(':text("Identity verified"), :text("verified")').count() else 0
        ab = _extract_about_and_bio(page, logger)
        guidebooks = _extract_guidebooks(page, logger)
        if guidebooks: SQL.replace_host_guidebooks(db, user_id, guidebooks)
        travels = _extract_travels(page, logger)
        if travels: SQL.replace_host_travels(db, user_id, travels)
        reviews = _extract_some_reviews(page, logger)
        if reviews: SQL.upsert_host_reviews(db, user_id, reviews)
        SQL.upsert_host_profile(db, {"userId": user_id, "userUrl": host_url, "name": None, "isSuperhost": is_super, "isVerified": is_ver, "ratingAverage": None, "ratingCount": None, "profile_url": host_url, "scraping_time": int(time.time()), "profile_photo_url": profile_photo_url, "about_text": ab.get("about_text"), "bio_text": ab.get("bio_text")})
        human = HumanMouseMovement(page)
        vp = page.viewport_size or {"width": 1400, "height": 900}
        human.move_to(int(vp["width"] * 0.45), int(vp["height"] * 0.45))
        _open_all_listings_and_expand(page, logger)
        listing_links = _collect_room_links_from_dom(page, logger, max_scrolls=60)
        if not listing_links: logger.warning("[host] No /rooms/ links found on host page.")
        listing_items = []
        for link in listing_links:
            m = re.search(r"/rooms/(\d+)", link)
            if m: listing_items.append({"listingId": str(m.group(1)), "listingUrl": f"https://www.airbnb.com/rooms/{m.group(1)}"})
        try: SQL.replace_host_listings(db, user_id, listing_items); logger.info(f"[host] Saved {len(listing_items)} listing rows for userId={user_id}")
        except Exception as e: logger.warning(f"[host] Failed saving host_listings: {e}")
        SQL.upsert_host_profile(db, {"userId": user_id, "total_listings": len(listing_items), "profile_url": host_url, "scraping_time": int(time.time()), "profile_photo_url": profile_photo_url, "about_text": ab.get("about_text"), "bio_text": ab.get("bio_text")})
        if listing_items and not request_item_token: _ensure_pdp_token_via_link(context, logger, listing_items[0]["listingUrl"])
        
        processed, detailed = 0, 0
        HOST_MAX = min(HOST_MAX_LISTINGS, len(listing_items))
        for item in listing_items[:HOST_MAX]:
            _id = item["listingId"]; processed += 1
            if not SQL.check_if_listing_exists(db, _id):
                try: SQL.insert_basic_listing(db, {"ListingId": _id, 
                                                   "ListingUrl": item["listingUrl"],
                                                     "link": item["listingUrl"]})
                except Exception as e: logger.warning(f"[host] Could not insert basic listing {_id}: {e}")
            if request_item_token and detailed < HOST_DETAIL_SCRAPE_LIMIT:
                try:
                    info = {"id": _id,
                             "link": item["listingUrl"],
                             "checkin": checkin_date,
                             "checkout": checkout_date
                            }
                    dd = HostScrapingUtils.scrape_single_result(context=context, item_search_token=request_item_token, listing_info=info, logger=logger, api_key=x_airbnb_api_key, client_version=request_client_version or "", client_request_id=request_item_client_id or "", federated_search_id="", currency="MAD", locale="en", base_headers=request_headers)
                    if not dd.get("skip", False):
                        dd['checkin'] = checkin_date
                        dd['checkout'] = checkout_date
                        SQL.update_listing_with_details(db, _id, dd)
                        host_name = dd.get("host")
                        if host_name: SQL.update_host_listing_name(db, user_id, _id, host_name)
                        pics = dd.get("allPictures") or []
                        if pics:
                            try:
                                # Use the new horizontal storage method
                                from . import host_SQL as SQL_new  # Import updated functions
                                SQL_new.upsert_listing_pictures_horizontal(db, _id, pics)
                                logger.info(f"[host] ✅ Stored {len(pics)} pictures for {_id}")
                            except Exception as e: 
                                logger.warning(f"[host] saving pictures failed for {_id}: {e}")
                        detailed += 1
                        logger.info(f"[host] ✅ hydrated {_id} | host={host_name or '—'} | photos={len(pics)}")
                        if dd.get("userId") == user_id:
                            SQL.upsert_host_profile(db, {"userId": user_id, "userUrl": dd.get("userUrl") or host_url, "name": host_name, "isSuperhost": int(bool(dd.get("isSuperhost"))), "isVerified": int(bool(dd.get("isVerified"))), "ratingAverage": dd.get("ratingAverage") or dd.get("hostrAtingAverage"), "ratingCount": dd.get("ratingCount"), "years": dd.get("years"), "months": dd.get("months"), "total_listings": len(listing_items), "profile_url": host_url, "scraping_time": int(time.time()), "profile_photo_url": profile_photo_url, "about_text": ab.get("about_text"), "bio_text": ab.get("bio_text")})
                        SQL.backfill_host_child_names(db, user_id)
                        try:
                            if host_name: SQL.set_host_name_for_listings(db, user_id, host_name)
                        except Exception as e: logger.warning(f"[host] set_host_name_for_listings failed: {e}")
                except Exception as e:
                    logger.info(f"[host] ❌ PDP hydrate failed for {_id}: {e}")
            time.sleep(random.uniform(0.5, 1.2))
        
        logger.info(f"🎉 [host] COMPLETE | processed listings: {processed} | hydrated: {detailed}")
        SQL.backfill_host_listing_names_from_tracking(db, user_id)
        try: page.close()
        except Exception: pass
        try: context.close()
        except Exception: pass
        try: browser.close()
        except Exception: pass
    db.close()

if __name__ == "__main__":
    import sys
    if len(sys.argv) < 2:
        print("Usage: python -m airbnb_host.host_agent <host_profile_url>")
        sys.exit(1)
    scrape_host(sys.argv[1])