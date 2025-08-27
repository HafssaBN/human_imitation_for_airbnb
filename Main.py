import datetime
from datetime import tzinfo
import tls_client
import os
import time
import Config
import SQL
import ScrapingUtils
import Utils
import sqlite3
import random
import logging
import multiprocessing
from openpyxl import Workbook
import json
import re

from playwright.sync_api import (
    sync_playwright, Page, BrowserContext, Browser, Route, Request
)
from undetected_playwright import Tarnished
from HumanMouseMovement import HumanMouseMovement
import urllib.parse
from datetime import datetime as _dt


from dotenv import load_dotenv
import os

# Load .env file
load_dotenv()
# ------------------------------------------------------------
# Data Validation Functions (NEW)
# ------------------------------------------------------------
def validate_price_format(price_str):
    """Validate price format and extract numeric value"""
    if not price_str:
        return None, None
        
    # Expected format: "MAD2,283" or "MAD 2,283"
    numeric_match = re.search(r'MAD\s*([0-9,]+)', price_str)
    if not numeric_match:
        return None, None
        
    numeric_str = numeric_match.group(1).replace(',', '')
    
    try:
        numeric_value = float(numeric_str)
        
        # Sanity check: reasonable price range for Morocco (50-50,000 MAD)
        if 50 <= numeric_value <= 50000:
            return price_str, numeric_value
        else:
            print(f"⚠️  Price out of expected range: {price_str} (numeric: {numeric_value})")
            return price_str, numeric_value  # Keep it but warn
            
    except ValueError:
        return None, None

def validate_image_url(image_url):
    """Validate image URL format"""
    if not image_url:
        return None
        
    # Expected Airbnb image URL patterns
    valid_patterns = [
        'https://a0.muscache.com/im/pictures/',
        'https://a1.muscache.com/im/pictures/',
        'https://a2.muscache.com/im/pictures/',
    ]
    
    if any(image_url.startswith(pattern) for pattern in valid_patterns):
        return image_url
    else:
        print(f"⚠️  Unexpected image URL format: {image_url[:50]}...")
        return image_url  # Keep it but warn

def validate_listing_data(listing_data, logger):
    """Validate a single listing's data"""
    validation_results = {
        'valid': True,
        'warnings': [],
        'errors': []
    }
    
    # Validate ID
    if not listing_data.get('id') or not str(listing_data['id']).isdigit():
        validation_results['errors'].append(f"Invalid ID: {listing_data.get('id')}")
        validation_results['valid'] = False
    
    # Validate price
    price = listing_data.get('price')
    if price:
        validated_price, price_numeric = validate_price_format(price)
        if validated_price:
            listing_data['price_numeric'] = price_numeric
        else:
            validation_results['warnings'].append(f"Price format issue: {price}")
    
    # Validate image
    image = listing_data.get('picture')
    if image:
        validated_image = validate_image_url(image)
        if not validated_image:
            validation_results['warnings'].append(f"Image URL issue: {image[:50]}...")
    
    # Validate coordinates (if present)
    lat, lng = listing_data.get('lat'), listing_data.get('lng')
    if lat is not None and lng is not None:
        # Morocco bounds: approximately 27°N to 36°N, 13°W to 1°W
        if not (27.0 <= lat <= 36.0 and -13.0 <= lng <= -1.0):
            validation_results['warnings'].append(f"Coordinates outside Morocco: {lat}, {lng}")
    
    # Log validation results
    if validation_results['warnings']:
        logger.warning(f"Validation warnings for {listing_data.get('id')}: {validation_results['warnings']}")
    if validation_results['errors']:
        logger.error(f"Validation errors for {listing_data.get('id')}: {validation_results['errors']}")
    
    return validation_results

def log_scraping_summary(logger, results, boundary_info=""):
    """Log a summary of scraping results with validation info"""
    if not results:
        logger.info(f"No results found {boundary_info}")
        return
    
    total = len(results)
    with_prices = len([r for r in results if r.get('price')])
    with_images = len([r for r in results if r.get('picture')])
    price_range = []
    
    for result in results:
        price_numeric = result.get('price_numeric')
        if price_numeric:
            price_range.append(price_numeric)
    
    logger.info(f"📊 Scraping summary {boundary_info}:")
    logger.info(f"  Total listings: {total}")
    logger.info(f"  With prices: {with_prices}/{total}")
    logger.info(f"  With images: {with_images}/{total}")
    
    if price_range:
        logger.info(f"  Price range: {min(price_range):.0f} - {max(price_range):.0f} MAD")
        logger.info(f"  Average price: {sum(price_range)/len(price_range):.0f} MAD")

# ------------------------------------------------------------
# Helper available at module level (used in multiple places)
# ------------------------------------------------------------
def _extract_pdp_token_from_request(req: Request) -> str | None:
    """
    Extract the persistedQuery sha256Hash (or path segment) used by StaysPdpSections.
    Works with both GET and POST styles Airbnb uses.
    """
    try:
        parsed = urllib.parse.urlparse(req.url)
        parts = [p for p in parsed.path.split('/') if p]
        for i, pth in enumerate(parts):
            if pth == "StaysPdpSections" and i + 1 < len(parts):
                cand = parts[i + 1]
                if cand:
                    return cand
        qs = urllib.parse.parse_qs(parsed.query)
        ext = qs.get('extensions', [None])[0]
        if ext:
            try:
                ext_obj = json.loads(ext)
                cand = (ext_obj.get('persistedQuery') or {}).get('sha256Hash')
                if cand:
                    return cand
            except Exception:
                pass
        try:
            body = req.post_data_json
            if body:
                ext2 = body.get('extensions') or {}
                cand = (ext2.get('persistedQuery') or {}).get('sha256Hash')
                if cand:
                    return cand
        except Exception:
            pass
    except Exception:
        pass
    return None


def _dismiss_any_popups_local(page: Page, logger: logging.Logger):
    """Local popup dismissal function with improved translation handling"""
    attempts = 0
    max_attempts = 5

    while attempts < max_attempts:
        attempts += 1
        dismissed = False

        translation_selectors = [
            'div[role="dialog"] button[aria-label="Close"]',
            'button:has-text("Got it")',
            'button:has-text("No thanks")',
            'button:has-text("Not now")',
            'button:has-text("Continue in English")',
            'button:has-text("Keep using English")',
            'button:has-text("Skip")',
            'button:has-text("Dismiss")',
            '[data-testid="translation-banner-dismiss"]',
            '[data-testid="modal-container"] button[aria-label="Close"]',
            'div[role="dialog"]:has-text("Translation") button',
            'div[role="dialog"]:has-text("translation") button',
            '.translation-dialog button',
            '.translation-banner button',
        ]

        for selector in translation_selectors:
            try:
                element = page.locator(selector).first
                if element.is_visible(timeout=1000):
                    logger.info(f"[popup-local] Dismissing with selector: {selector}")
                    element.click(timeout=3000)
                    page.wait_for_timeout(500)
                    dismissed = True
                    break
            except Exception:
                continue

        if not dismissed:
            try:
                if page.locator('div[role="dialog"]').first.is_visible(timeout=1000):
                    logger.info("[popup-local] Using ESC key")
                    page.keyboard.press("Escape")
                    page.wait_for_timeout(300)
                    dismissed = True
            except Exception:
                pass

        if not dismissed:
            try:
                targets = ["div.gm-style", "main", "body"]
                for target in targets:
                    try:
                        elem = page.locator(target).first
                        if elem.count() > 0:
                            box = elem.bounding_box()
                            if box:
                                cx = int(box["x"] + box["width"] / 2)
                                cy = int(box["y"] + box["height"] / 2)
                                page.mouse.click(cx, cy)
                                page.wait_for_timeout(300)
                                dismissed = True
                                break
                    except Exception:
                        continue
            except Exception:
                pass

        try:
            still_visible = page.locator('div[role="dialog"]:visible').count() > 0
            if not still_visible:
                break
        except Exception:
            break

        if dismissed:
            logger.info(f"[popup-local] Dismissed something in attempt {attempts}")

        page.wait_for_timeout(500)

    return attempts < max_attempts


def _ensure_pdp_token_via_grid(context: BrowserContext, logger: logging.Logger) -> bool:
    """
    Open the search grid and click a card to trigger StaysPdpSections once.
    Relies on the context-level 'on_request' listener to capture headers/token.
    """
    page = context.new_page()
    try:
        logger.info("[pdp-capture] Opening search grid to capture PDP token…")
        page.goto("https://www.airbnb.com/s/Morocco/homes", wait_until="domcontentloaded", timeout=60000)
        _dismiss_any_popups_local(page, logger)
        selectors = [
            '[data-testid="item-card"] a',
            'a[href*="/rooms/"]',
            '[itemprop="itemListElement"] a',
            '[data-testid="listing-card"] a',
        ]
        clicked = False
        for sel in selectors:
            try:
                el = page.locator(sel).first
                el.wait_for(state="visible", timeout=8000)
                el.click()
                clicked = True
                break
            except Exception:
                continue
        if not clicked:
            try:
                page.locator('[data-testid="item-card"]').first.press("Enter")
                clicked = True
            except Exception:
                pass
        # wait for PDP request to fire
        try:
            page.wait_for_request(lambda r: "/api/v3/StaysPdpSections" in r.url, timeout=30000)
        except Exception:
            pass
        page.wait_for_timeout(1200)
        return True
    except Exception as e:
        logger.info(f"[pdp-capture] Grid method failed: {e}")
        return False
    finally:
        try:
            page.close()
        except Exception:
            pass


def _ensure_pdp_token_via_link(context: BrowserContext, logger: logging.Logger, link: str) -> bool:
    """
    Open a specific PDP link to trigger StaysPdpSections once.
    """
    page = context.new_page()
    try:
        logger.info(f"[pdp-capture] Opening PDP link to capture token: {link}")
        page.goto(link, wait_until="domcontentloaded", timeout=60000)
        _dismiss_any_popups_local(page, logger)
        try:
            page.wait_for_request(lambda r: "/api/v3/StaysPdpSections" in r.url, timeout=30000)
        except Exception:
            pass
        page.wait_for_timeout(1000)
        return True
    except Exception as e:
        logger.info(f"[pdp-capture] PDP link method failed: {e}")
        return False
    finally:
        try:
            page.close()
        except Exception:
            pass


def start_scraping(logger: logging.Logger, db: sqlite3.Connection):
    script_dir = os.path.dirname(os.path.abspath(__file__))
    search_token = None
    request_count = 0  # Track requests for token refresh

    # --- request context vars (search + PDP) ---
    request_locale = 'en'
    request_currency = 'MAD'
    request_operation = 'StaysSearch'
    request_client_version = None
    request_client_id = None
    request_skip_hydration = []
    request_monthly_end_date = []
    request_monthly_start_date = []
    request_place_id = []
    x_airbnb_api_key =  "d306zoyjsyarp7ifhu67rjxn52tv0t20"

    # PDP detail capture vars
    request_item_token = None
    request_item_client_id = None
    request_headers = {}

    # --- budgets (read from Config if present) ---
    MAX_LISTINGS_PER_RUN = getattr(Config, "MAX_LISTINGS_PER_RUN", 3)     # hard cap
    DETAIL_SCRAPE_LIMIT = getattr(Config, "DETAIL_SCRAPE_LIMIT", 3)       # PDP cap

    processed_total = 0
    details_saved_total = 0
    stop_everything = False

    # --- load boundaries & window ---
    logger.info('Loading boundaries...')
    geo_data_file = os.path.join(script_dir, 'geo_data/ma_surface_export.txt')
    total, all_boundaries = Utils.load_data_points(db, geo_data_file)

    start = SQL.get_tracking(db)
    if start >= len(all_boundaries):
        start = 0
        SQL.update_tracking(db, 0)

    end = min(start + Config.BOUNDARIES_PER_SCRAPING, len(all_boundaries))
    boundaries = all_boundaries[start:end]

    logger.info(f'Remaining boundaries: {len(boundaries)}/{total}')

    # Log current statistics
    stats = SQL.get_scraping_stats(db)
    logger.info(f'Current stats - Total: {stats["total_listings"]}, Basic: {stats["basic_only"]}, Detailed: {stats["with_details"]}, Pending: {stats["pending_details"]}')

    logger.info('Launching the web browser')

    with sync_playwright() as p:
        browser: Browser = p.chromium.launch(
            headless=False,
            proxy=Config.CONFIG_PROXY,
            args=[
                "--disable-features=Translate,TranslateUI,LanguageSettings",
                "--lang=en-US",
                "--disable-infobars",
                "--disable-extensions",
                "--no-first-run",
                "--disable-default-apps",
            ],
        )
        context: BrowserContext = browser.new_context(
            viewport={"width": 1400, "height": 900},
            locale="en-US",
            extra_http_headers={"Accept-Language": "en-US,en;q=0.9"},
        )
        context = Tarnished.apply_stealth(context)
        page = context.new_page()
        page.set_default_timeout(60000)

        # --------- ROUTE (intercept) ---------
        def handle_request(route: Route):
            nonlocal search_token, request_locale, request_currency, request_operation
            nonlocal request_client_version, request_client_id, x_airbnb_api_key
            nonlocal request_monthly_end_date, request_monthly_start_date, request_place_id
            nonlocal request_item_token, request_item_client_id
            nonlocal request_headers

            req: Request = route.request
            url = req.url

            if "/api/v3/StaysSearch" in url:
                parsed = urllib.parse.urlparse(url)
                token_local = parsed.path.split('/')[-1]
                if token_local and token_local != search_token:
                    logger.info(f"[route] search_token = {token_local}")
                if token_local:
                    search_token = token_local
                qs = urllib.parse.parse_qs(parsed.query)
                request_locale    = qs.get('locale', ['en'])[0]
                request_currency  = qs.get('currency', ['USD'])[0]
                request_operation = qs.get('operationName', ['StaysSearch'])[0]
                hdrs = req.headers
                request_client_version = hdrs.get('x-client-version')
                request_client_id      = hdrs.get('x-client-request-id')
                x_airbnb_api_key       = hdrs.get('x-airbnb-api-key')
                request_headers = hdrs.copy()

                try:
                    raw = req.post_data_json['variables']['staysMapSearchRequestV2']['rawParams']
                    for el in raw:
                        if el['filterName'] == "monthlyEndDate":   request_monthly_end_date = el['filterValues']
                        if el['filterName'] == "monthlyStartDate": request_monthly_start_date = el['filterValues']
                        if el['filterName'] == "placeId":          request_place_id = el['filterValues']
                except Exception:
                    pass
                route.continue_()
                return

            if "/api/v3/StaysPdpSections" in url:
                token = _extract_pdp_token_from_request(req)
                if token and not request_item_token:
                    request_item_token = token
                    logger.info(f"[route] PDP token = {request_item_token}")
                request_item_client_id = req.headers.get('x-client-request-id')
                request_headers = req.headers.copy()
                api_k = req.headers.get('x-airbnb-api-key')
                if api_k:
                    x_airbnb_api_key = api_k
                route.continue_()
                return

            route.continue_()

        context.route('**/api/v3/*', handle_request)

        # --------- EVENTS (extra capture) ---------
        def on_request(req: Request):
            nonlocal search_token, request_item_token, request_item_client_id, x_airbnb_api_key, request_headers
            if "/api/v3/StaysSearch/" in req.url:
                try:
                    parsed = urllib.parse.urlparse(req.url)
                    token_local = parsed.path.split('/')[-1]
                    if token_local and token_local != search_token:
                        logger.info(f"[event] search_token = {token_local}")
                    if token_local:
                        search_token = token_local
                except Exception:
                    pass
            if "/api/v3/StaysPdpSections" in req.url:
                token = _extract_pdp_token_from_request(req)
                if token and not request_item_token:
                    request_item_token = token
                    request_item_client_id = req.headers.get('x-client-request-id')
                    logger.info(f"[event] PDP token captured = {request_item_token}")
                request_headers = req.headers.copy()
                api_k = req.headers.get('x-airbnb-api-key')
                if api_k:
                    x_airbnb_api_key = api_k

        context.on("request", on_request)

        # --------- Go to search ---------
        logger.info("Navigating to Airbnb Morocco search...")
        page.goto('https://www.airbnb.com/s/Morocco/homes', wait_until='domcontentloaded', timeout=60000)

        logger.info("Initial comprehensive popup cleanup...")
        for i in range(10):
            _dismiss_any_popups_local(page, logger)
            page.wait_for_timeout(500)
            try:
                dialog_count = page.locator('div[role="dialog"]:visible').count()
                if dialog_count == 0:
                    logger.info(f"All popups cleared after {i+1} attempts")
                    break
                else:
                    logger.info(f"Still {dialog_count} dialogs visible, continuing...")
            except Exception:
                break

        # Wait for map to be ready
        try:
            page.get_by_test_id('map/ZoomInButton').wait_for(timeout=60000)
        except Exception:
            pass

        # Use the ScrapingUtils helper (referenced via the module to avoid circular imports)
        ScrapingUtils._wait_for_stable_page(page, logger, timeout=20000)
        try:
            ScrapingUtils.wait_for_network_idle_2(page, timeout=45000, max_connections=2)
        except Exception:
            pass

        # Make the map visible and ready
        map_canvas = page.locator("div.gm-style").first
        map_canvas.wait_for(state="visible", timeout=60000)
        bbox = map_canvas.bounding_box()
        logger.info(f"Map boundaries bbox: {bbox}")

        # Force at least one StaysSearch (drag/zoom/wheel)
        def _nudge_map(page: Page, logger: logging.Logger):
            _dismiss_any_popups_local(page, logger)
            page.wait_for_timeout(500)
            canvas = page.locator("div.gm-style").first
            try:
                box = canvas.bounding_box()
            except Exception:
                box = None
            if box:
                cx = int(box["x"] + box["width"]/2)
                cy = int(box["y"] + box["height"]/2)
                try:
                    page.mouse.move(cx, cy)
                    page.mouse.down()
                    page.mouse.move(cx + 120, cy, steps=12)
                    page.mouse.up()
                    page.wait_for_timeout(300)
                    return
                except Exception:
                    pass
            try:
                _dismiss_any_popups_local(page, logger)
                page.get_by_test_id('map/ZoomInButton').click(timeout=3000, force=True)
                page.wait_for_timeout(300)
                return
            except Exception:
                pass
            try:
                page.mouse.wheel(0, -400)
                page.wait_for_timeout(300)
            except Exception:
                pass

        def _wait_for_search_token(page: Page, logger: logging.Logger, tries=10, per_wait=8000):
            nonlocal search_token
            for attempt in range(1, tries + 1):
                if search_token:
                    return True
                logger.info(f"[search_token] Attempt {attempt}/{tries}")
                _dismiss_any_popups_local(page, logger)
                try:
                    req = page.wait_for_request(
                        lambda r: "/api/v3/StaysSearch/" in r.url and r.method in ("POST", "GET"),
                        timeout=per_wait
                    )
                    parsed = urllib.parse.urlparse(req.url)
                    token_local = parsed.path.split("/")[-1]
                    if token_local:
                        search_token = token_local
                        logger.info(f"search_token captured = {search_token}")
                        return True
                except Exception:
                    logger.info("[search_token] Not seen yet; nudging the map...")
                    _nudge_map(page, logger)
                    page.wait_for_timeout(600)
            return False

        _nudge_map(page, logger)
        if not _wait_for_search_token(page, logger, tries=10, per_wait=8000):
            for _ in range(5):
                _nudge_map(page, logger)
                if _wait_for_search_token(page, logger, tries=2, per_wait=6000):
                    break
            if not search_token:
                raise RuntimeError("Could not capture StaysSearch search_token after nudging the map")

        page.wait_for_timeout(500)

        # --------- Ensure a PDP token once (so we can fetch details) ---------
        if not request_item_token:
            _ensure_pdp_token_via_grid(context, logger)
            page.wait_for_timeout(800)

        # --------- Main scraping loop (basic + details in one run) ---------
        if not bbox:
            map_canvas = page.locator("div.gm-style").first
            map_canvas.wait_for(state="visible", timeout=60000)
            bbox = map_canvas.bounding_box()

        for global_idx, boundary in enumerate(boundaries, start=start):
            if stop_everything:
                break

            # Skip if boundary scraped recently
            if SQL.check_if_boundaries_exists(db, global_idx):
                logger.info(f'Skipping boundary {global_idx}, {boundary}')
                SQL.update_tracking(db, global_idx + 1)
                continue

            # Refresh tokens periodically for data freshness (NEW)
            request_count += 1
            if request_count % 10 == 0:  # Every 10 requests
                logger.info("[freshness] Refreshing tokens for data freshness...")
                search_token = None
                request_item_token = None
                _nudge_map(page, logger)
                _wait_for_search_token(page, logger, tries=3, per_wait=5000)
                if not request_item_token:
                    _ensure_pdp_token_via_grid(context, logger)

            # sanity check
            def _is_plausible_morocco_bbox(b):
                try:
                    sw_lat, sw_lng, ne_lat, ne_lng = float(b[0]), float(b[1]), float(b[2]), float(b[3])
                except Exception:
                    return False
                if not (20.0 <= sw_lat <= 37.5 and 20.0 <= ne_lat <= 37.5): return False
                if not (-17.5 <= sw_lng <= -0.5 and -17.5 <= ne_lng <= -0.5): return False
                if not (sw_lat < ne_lat and sw_lng < ne_lng): return False
                if (ne_lat - sw_lat) > 5.0 or (ne_lng - sw_lng) > 5.0: return False
                return True

            if not _is_plausible_morocco_bbox(boundary):
                logger.warning(f"Skipping invalid bbox {global_idx}: {boundary}")
                SQL.update_tracking(db, global_idx + 1)
                continue

            total_found = 0
            basic_saved = 0
            detailed_saved = 0
            logger.info(f'Scraping boundary {global_idx}, {boundary}')
            next_token = None

            while True:
                if stop_everything:
                    break

                logger.info(f'Next token: {next_token}')
                try:
                    # Enhanced API request with retry logic (NEW)
                    max_retries = 3
                    page_result = None
                    
                    for attempt in range(max_retries):
                        try:
                            page_result = ScrapingUtils.scrape_page_result(
                                context=context,
                                search_token=search_token,
                                operation=request_operation,
                                local=request_locale,
                                currency=request_currency,
                                boundary=boundary,
                                monthly_end_date=request_monthly_end_date,
                                monthly_start_date=request_monthly_start_date,
                                skip_hydration=request_skip_hydration,
                                place_id=request_place_id,
                                map_width_px=int(bbox['width']),
                                map_height_px=int(bbox['height']),
                                api_key=x_airbnb_api_key,
                                client_version=request_client_version,
                                request_id=request_client_id,
                                logger=logger,
                                page_token=next_token,
                                base_headers=request_headers,
                            )
                            break  # Success
                        except Exception as e:
                            if attempt < max_retries - 1:
                                delay = (2 ** attempt) + random.uniform(0, 1)
                                logger.warning(f"API request failed (attempt {attempt + 1}/{max_retries}): {e}")
                                logger.info(f"Retrying in {delay:.1f} seconds...")
                                time.sleep(delay)
                            else:
                                raise e
                    
                except Exception as e:
                    logger.error(f"Error scraping page after all retries: {e}")
                    break

                # Validate results (NEW)
                valid_results = []
                for result in page_result['searchResults']:
                    validation = validate_listing_data(result, logger)
                    if validation['valid']:
                        valid_results.append(result)
                    else:
                        logger.warning(f"Skipping invalid listing: {result.get('id')} - {validation['errors']}")

                page_result['searchResults'] = valid_results
                
                # Log summary with validation info (NEW)
                log_scraping_summary(logger, valid_results, f"for boundary {global_idx}")

                logger.info(f"Found {len(valid_results)} valid results | total pages: {page_result['totalPages']}")

                for result in valid_results:
                    if stop_everything:
                        break

                    total_found += 1

                    # Hard cap for this run: do not exceed MAX_LISTINGS_PER_RUN
                    if processed_total >= MAX_LISTINGS_PER_RUN:
                        logger.info(f"[limit] MAX_LISTINGS_PER_RUN ({MAX_LISTINGS_PER_RUN}) reached. Stopping.")
                        stop_everything = True
                        break

                    # Save basic row (if not seen in recent window)
                    if not SQL.check_if_listing_exists(db, result['id']):
                        try:
                            SQL.insert_basic_listing(db, result)
                            basic_saved += 1
                            processed_total += 1
                            
                            # Enhanced logging with price info (NEW)
                            price_info = f"{result.get('price', 'No price')}"
                            if result.get('price_numeric'):
                                price_info += f" ({result['price_numeric']:.0f} MAD)"
                            
                            logger.info(f"✅ Saved basic data for {result['id']} | {result.get('title', 'No title')} | {price_info} ({processed_total}/{MAX_LISTINGS_PER_RUN} this run)")
                        except Exception as e:
                            logger.error(f"Error saving listing {result['id']}: {e}")

                    # Immediately attempt to fetch details for this listing (if budget remains)
                    if details_saved_total < DETAIL_SCRAPE_LIMIT:
                        norm_id = ScrapingUtils._normalize_listing_id(result['id']) if result.get('id') is not None else None
                        link = result.get('link') or (f"https://www.airbnb.com/rooms/{norm_id}" if norm_id else None)

                        if not request_item_token and link:
                            _ensure_pdp_token_via_link(context, logger, link)
                            page.wait_for_timeout(400)

                        if request_item_token and norm_id and link:
                            try:
                                listing_info = {
                                    'id': norm_id,
                                    'link': link,
                                    'title': result.get('title'),
                                    'categoryTag': None,
                                    'photoId': None,
                                    'checkin': None,
                                    'checkout': None,
                                }
                                
                                # Enhanced detailed data scraping with retry (NEW)
                                detailed_data = None
                                for detail_attempt in range(2):  # 2 attempts for details
                                    try:
                                        detailed_data = ScrapingUtils.scrape_single_result(
                                            context=context,
                                            item_search_token=request_item_token,
                                            listing_info=listing_info,
                                            logger=logger,
                                            api_key=x_airbnb_api_key,
                                            client_version=request_client_version or "",
                                            client_request_id=request_item_client_id or "",
                                            federated_search_id="",
                                            currency="MAD",
                                            locale="en",
                                            base_headers=request_headers,
                                        )
                                        break  # Success
                                    except Exception as e:
                                        if detail_attempt == 0:
                                            logger.warning(f"[details] First attempt failed for {norm_id}: {e}, retrying...")
                                            time.sleep(1)
                                        else:
                                            logger.error(f"[details] All attempts failed for {norm_id}: {e}")
                                            detailed_data = {'skip': True}

                                if detailed_data and not detailed_data.get('skip', False):
                                    # Validate detailed data (NEW)
                                    detail_validation = validate_detailed_data(detailed_data, logger)
                                    if detail_validation['valid']:
                                        SQL.update_listing_with_details(db, norm_id, detailed_data)
                                        detailed_saved += 1
                                        details_saved_total += 1
                                        
                                        # Enhanced logging for details (NEW)
                                        host_info = f"Host: {detailed_data.get('host', 'Unknown')}"
                                        if detailed_data.get('isSuperhost'):
                                            host_info += " (Superhost)"
                                        location_info = detailed_data.get('location', 'Location unknown')
                                        
                                        logger.info(f"📋 Saved details for {norm_id} | {host_info} | {location_info} ({details_saved_total}/{DETAIL_SCRAPE_LIMIT} this run)")
                                    else:
                                        logger.warning(f"[details] Invalid detailed data for {norm_id}: {detail_validation['warnings']}")
                                        
                            except Exception as e:
                                logger.info(f"[details] Could not fetch details for {norm_id}: {e}")
                        
                        if details_saved_total >= DETAIL_SCRAPE_LIMIT:
                            logger.info("📊 [details] Budget exhausted — no more PDP requests this run.")

                    # Small delay to avoid hammering PDP
                    time.sleep(random.randint(
                        max(1, Config.CONFIG_PAGE_DELAY_MIN),
                        max(2, Config.CONFIG_PAGE_DELAY_MAX)
                    ))

                next_token = page_result['nextPageCursor']
                if stop_everything or next_token is None or len(page_result['searchResults']) < 13:
                    break

            logger.info(f"🎯 Boundary {global_idx} completed - Total found: {total_found}, Basic saved: {basic_saved}, Detailed saved: {detailed_saved}")

            now = _dt.now()
            SQL.insert_new_boundaries_tracking(db, data={
                "id": global_idx,
                "xmin": boundary[0],
                "ymin": boundary[1],
                "xmax": boundary[2],
                "ymax": boundary[3],
                "total": total_found,
                "timestamp": int(now.timestamp()),
            })
            SQL.update_tracking(db, global_idx + 1)

            if stop_everything:
                break

            time.sleep(1.5)


def validate_detailed_data(detailed_data, logger):
    """Validate detailed listing data from PDP API (NEW)"""
    validation_results = {
        'valid': True,
        'warnings': [],
        'errors': []
    }
    
    # Check for required fields
    if not detailed_data.get('host'):
        validation_results['warnings'].append("No host information")
    
    # Validate coordinates if present
    lat, lng = detailed_data.get('lat'), detailed_data.get('lng')
    if lat is not None and lng is not None:
        if not (27.0 <= lat <= 36.0 and -13.0 <= lng <= -1.0):
            validation_results['warnings'].append(f"Coordinates outside Morocco: {lat}, {lng}")
    
    # Validate ratings
    avg_rating = detailed_data.get('averageRating', 0)
    if avg_rating and (avg_rating < 0 or avg_rating > 5):
        validation_results['warnings'].append(f"Invalid average rating: {avg_rating}")
    
    host_rating = detailed_data.get('hostrAtingAverage', 0)  # Note: typo from original code
    if host_rating and (host_rating < 0 or host_rating > 5):
        validation_results['warnings'].append(f"Invalid host rating: {host_rating}")
    
    # Validate capacity
    max_capacity = detailed_data.get('maxGuestCapacity', 0)
    if max_capacity and (max_capacity < 1 or max_capacity > 50):  # Reasonable limits
        validation_results['warnings'].append(f"Unusual guest capacity: {max_capacity}")
    
    # Log warnings
    if validation_results['warnings']:
        logger.debug(f"Detail validation warnings: {validation_results['warnings']}")
    
    return validation_results


def main():
    script_dir = os.path.dirname(os.path.abspath(__file__))
    logger = Utils.setup_logger()
    
    # Enhanced startup logging (NEW)
    logger.info('🚀 Airbnb scraper started with enhanced validation...')
    logger.info(f'📊 Limits: MAX_LISTINGS_PER_RUN={getattr(Config, "MAX_LISTINGS_PER_RUN", 3)}, DETAIL_SCRAPE_LIMIT={getattr(Config, "DETAIL_SCRAPE_LIMIT", 3)}')
    
    db = Utils.connect_db()

    # Print current statistics
    stats = SQL.get_scraping_stats(db)
    logger.info(f'📈 Database stats - Total listings: {stats["total_listings"]}, Basic only: {stats["basic_only"]}, With details: {stats["with_details"]}, Pending details: {stats["pending_details"]}')

    try:
        start_scraping(logger, db)
        logger.info('✅ Airbnb scraper finished successfully')
    except KeyboardInterrupt:
        logger.info('⏹️  Scraping interrupted by user')
    except Exception as e:
        logger.error(f'❌ Scraping failed: {e}')
        import traceback
        logger.error(f'Full traceback: {traceback.format_exc()}')
    finally:
        # Print final statistics with enhanced formatting (NEW)
        final_stats = SQL.get_scraping_stats(db)
        logger.info('=' * 60)
        logger.info('📊 FINAL STATISTICS')
        logger.info('=' * 60)
        logger.info(f'Total listings in database: {final_stats["total_listings"]}')
        logger.info(f'Listings with basic data only: {final_stats["basic_only"]}')
        logger.info(f'Listings with detailed data: {final_stats["with_details"]}')
        logger.info(f'Boundaries processed: {final_stats.get("boundaries_processed", "Unknown")}')
        logger.info(f'Recent listings (24h): {final_stats.get("recent_listings", "Unknown")}')
        logger.info('=' * 60)
        
        # Additional data quality report (NEW)
        try:
            conn = sqlite3.connect(Config.CONFIG_DB_FILE)  # Adjust path as needed
            cursor = conn.cursor()
            cursor.execute("SELECT COUNT(*) FROM listing_tracking")  
            # Price statistics
            cursor.execute("SELECT COUNT(*) FROM listing_tracking WHERE price IS NOT NULL")
            with_prices = cursor.fetchone()[0]
            
            # Image statistics  
            cursor.execute("SELECT COUNT(*) FROM listing_tracking WHERE picture IS NOT NULL")
            with_images = cursor.fetchone()[0]
            
            # Location statistics
            cursor.execute("SELECT COUNT(*) FROM listing_tracking WHERE lat IS NOT NULL AND lng IS NOT NULL")
            with_coords = cursor.fetchone()[0]
            
            logger.info('📋 DATA QUALITY REPORT')
            logger.info(f'Listings with prices: {with_prices}/{final_stats["total_listings"]} ({with_prices/max(1,final_stats["total_listings"])*100:.1f}%)')
            logger.info(f'Listings with images: {with_images}/{final_stats["total_listings"]} ({with_images/max(1,final_stats["total_listings"])*100:.1f}%)')
            logger.info(f'Listings with coordinates: {with_coords}/{final_stats["total_listings"]} ({with_coords/max(1,final_stats["total_listings"])*100:.1f}%)')
            
            conn.close()
        except Exception as e:
            logger.warning(f"Could not generate data quality report: {e}")
        
        db.close()


def run_detailed_scraper():
    """
    Legacy separate entry (kept for compatibility). Not required anymore,
    as details are fetched during main scraping.
    """
    logger = Utils.setup_logger()
    logger.info('🔄 Starting detailed scraping session (legacy)…')
    db = Utils.connect_db()
    try:
          # if present elsewhere
        scrape_detailed_listings(logger, db, limit=50)
        logger.info('✅ Detailed scraping session finished')
    except Exception as e:
        logger.error(f'❌ Detailed scraping failed: {e}')
    finally:
        db.close()


# Additional utility functions for data verification (NEW)
def verify_scraped_data(db_path="airbnb_data.db"):
    """Utility function to verify scraped data quality"""
    import sqlite3
    import requests
    
    logger = Utils.setup_logger()
    logger.info("🔍 Starting data verification...")
    
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    
    # Sample verification
    cursor.execute("""
        SELECT id, title, price, picture, link 
        FROM listing_tracking 
        ORDER BY scraping_time DESC 
        LIMIT 5
    """)
    
    recent_listings = cursor.fetchall()
    
    for listing in recent_listings:
        listing_id, title, price, picture, link = listing
        logger.info(f"📝 Verifying listing {listing_id}: {title}")
        
        # Verify price format
        if price:
            validated_price, price_numeric = validate_price_format(price)
            if validated_price:
                logger.info(f"  ✅ Price valid: {price} ({price_numeric} MAD)")
            else:
                logger.warning(f"  ⚠️  Price format issue: {price}")
        
        # Verify image URL (quick check)
        if picture:
            validated_image = validate_image_url(picture)
            if validated_image:
                logger.info(f"  ✅ Image URL valid: {picture[:50]}...")
                # Optional: Check if image is accessible
                try:
                    response = requests.head(picture, timeout=3)
                    if response.status_code == 200:
                        logger.info(f"  ✅ Image accessible")
                    else:
                        logger.warning(f"  ⚠️  Image not accessible: {response.status_code}")
                except Exception as e:
                    logger.warning(f"  ⚠️  Could not check image: {e}")
        
        logger.info(f"  🔗 Manual check: {link}")
        logger.info("")
    
    conn.close()
    logger.info("✅ Data verification completed")


if __name__ == '__main__':
    # Single command run: python Main.py
    # Add command line argument support (NEW)
    import sys
    
    if len(sys.argv) > 1 and sys.argv[1] == "verify":
        verify_scraped_data()
    else:
        main()