# host_agent.py
import re
import time
import json
import urllib.parse
import logging
import random
from typing import List, Optional, Set, Dict, Union, Any

from playwright.sync_api import (
    sync_playwright, Page, BrowserContext, Browser, Route, Request, Locator
)
from undetected_playwright import Tarnished

import Config
import host_utils as Utils
import host_SQL as SQL
import ScrapingUtils
from HumanMouseMovement import HumanMouseMovement


# -----------------------------
# Small helpers
# -----------------------------

def _extract_pdp_token_from_request(req: Request) -> Optional[str]:
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


def _parse_user_id_from_url(host_url: str) -> Optional[str]:
    try:
        path = urllib.parse.urlparse(host_url).path.strip("/")
        parts = path.split("/")
        digits = [p for p in parts if p.isdigit()]
        return digits[-1] if digits else None
    except Exception:
        return None


def _dedupe_keep_order(items: List[str]) -> List[str]:
    seen: Set[str] = set()
    out: List[str] = []
    for it in items:
        if it not in seen:
            out.append(it)
            seen.add(it)
    return out


def _click_if_exists(search_root: Union[Page, Locator],
                     selectors: List[str],
                     logger: logging.Logger,
                     label: str) -> bool:
    """Click the first visible element under search_root that matches any selector."""
    for sel in selectors:
        try:
            el = search_root.locator(sel).first
            if el.count() and el.is_visible(timeout=1200):
                logger.info(f'[host] Clicking "{label}"')
                el.click(timeout=4000, force=True)
                # small settle time
                try:
                    (search_root.page if isinstance(search_root, Locator) else search_root).wait_for_timeout(600)
                except Exception:
                    pass
                return True
        except Exception:
            continue
    return False


# -----------------------------
# PDP token helper
# -----------------------------

def _ensure_pdp_token_via_link(context: BrowserContext,
                               logger: logging.Logger,
                               link: str) -> bool:
    page = context.new_page()
    try:
        logger.info(f"[pdp-capture] Opening PDP link to capture token: {link}")
        page.goto(link, wait_until="domcontentloaded", timeout=60000)
        ScrapingUtils._dismiss_any_popups_enhanced(page, logger, max_attempts=3)
        try:
            page.wait_for_request(lambda r: "/api/v3/StaysPdpSections" in r.url, timeout=30000)
        except Exception:
            pass
        page.wait_for_timeout(1000)
        return True
    except Exception as e:
        logger.info(f"[pdp-capture] failed: {e}")
        return False
    finally:
        try:
            page.close()
        except Exception:
            pass


# -----------------------------
# Listings grid expansion
# -----------------------------

def _open_all_listings_and_expand(page: Page, logger: logging.Logger) -> None:
    """
    If present, opens "View all listings" then clicks ONLY the grid "Show more results"
    a limited number of times. Stops if link count no longer increases.
    """
    _click_if_exists(
        page,
        [
            'a:has-text("View all listings")',
            'button:has-text("View all listings")',
            'text=/View all \\d+ listings/i'
        ],
        logger,
        "View all listings"
    )

    # Wait for grid presence
    try:
        page.wait_for_selector('a[href*="/rooms/"]', timeout=10000)
    except Exception:
        pass

    max_clicks = 12
    stagnant_after_clicks = 0

    def anchor_count() -> int:
        try:
            return page.locator('a[href*="/rooms/"]').count()
        except Exception:
            return 0

    last = anchor_count()

    # selectors that are specific to the results paginator
    show_more_selectors = [
        'button[aria-label="Show more results"]',
        'a[aria-label="Show more results"]',
        'button[data-testid="pagination-button-next"]',
        'nav[aria-label="Pagination"] button:has-text("Show more")'
    ]

    for i in range(max_clicks):
        clicked = _click_if_exists(page, show_more_selectors, logger, "Show more results")
        if not clicked:
            break

        # allow network/render
        try:
            page.wait_for_load_state("networkidle", timeout=8000)
        except Exception:
            pass

        # gentle scroll to trigger lazy-load
        for _ in range(3):
            try:
                page.mouse.wheel(0, random.randint(800, 1200))
            except Exception:
                break
            time.sleep(random.uniform(0.25, 0.4))

        curr = anchor_count()
        if curr <= last:
            stagnant_after_clicks += 1
        else:
            stagnant_after_clicks = 0
            last = curr

        if stagnant_after_clicks >= 2:
            logger.info('[host] No new results after two "Show more" clicks — stopping.')
            break


def _collect_room_links_from_dom(page: Page, logger: logging.Logger, max_scrolls: int = 60) -> List[str]:
    """Scroll (after expanding grid) and collect '/rooms/<id>' links."""
    all_links: List[str] = []
    last_len = 0
    for i in range(max_scrolls):
        try:
            anchors = page.locator('a[href*="/rooms/"]')
            count = anchors.count()
            for idx in range(min(count, 800)):
                href = anchors.nth(idx).get_attribute("href") or ""
                if "/rooms/" in href:
                    if href.startswith("/"):
                        href = "https://www.airbnb.com" + href
                    href = href.split("?")[0]
                    all_links.append(href)
        except Exception:
            pass

        try:
            page.mouse.wheel(0, random.randint(900, 1400))
        except Exception:
            break
        time.sleep(random.uniform(0.25, 0.45))

        if len(all_links) == last_len and i > 10:
            break
        last_len = len(all_links)

    deduped = _dedupe_keep_order(all_links)
    logger.info(f"[host] Collected {len(deduped)} unique room links from DOM after {i+1} scrolls")
    return deduped


# -----------------------------
# DOM extraction helpers
# -----------------------------

def _extract_about_and_bio(page: Page, logger: logging.Logger) -> Dict[str, Optional[str]]:
    """
    Returns {"about_text": "...(bullets one per line)...", "bio_text": "...paragraph..."}
    Supports headings like "About Abdel".
    """
    out: Dict[str, Optional[str]] = {"about_text": None, "bio_text": None}

    # broader match: "About" or "About <name>"
    section_sel = 'section:has(h2:text-matches("^\\s*About(\\b|\\s)", "i"))'
    sec = page.locator(section_sel).first
    if sec.count() == 0:
        logger.info("[host] About section not found")
        return out

    _click_if_exists(
        sec,
        [
            'button:has-text("Show all")',
            'a:has-text("Show all")'
        ],
        logger,
        "Show all (About)"
    )

    bullets: List[str] = []
    try:
        li = sec.locator("li:visible")
        if li.count():
            bullets = [s.strip() for s in li.all_text_contents() if s and s.strip()]
    except Exception:
        pass

    para_text: Optional[str] = None
    try:
        ps = sec.locator("p:visible")
        best_len, best_txt = -1, None  # type: ignore[assignment]
        for i in range(ps.count()):
            t = ps.nth(i).inner_text().strip()
            if len(t) > best_len:
                best_len, best_txt = len(t), t
        para_text = best_txt
    except Exception:
        pass

    if not bullets:
        try:
            text_full = sec.inner_text()
            pre = text_full.split(para_text)[0] if (para_text and para_text in text_full) else text_full
            for line in [l.strip() for l in pre.splitlines()]:
                if (3 <= len(line) <= 140
                    and not line.lower().startswith("about")
                    and "review" not in line.lower()):
                    bullets.append(line)
        except Exception:
            pass

    about_text = "\n".join(_dedupe_keep_order(bullets)) if bullets else None
    out["about_text"] = about_text
    out["bio_text"] = para_text

    logger.info(f"[host] About parsed -> bullets: {len(bullets)} | bio_len: {len(para_text or '')}")
    return out


def _extract_guidebooks(page: Page, logger: logging.Logger) -> List[Dict[str, str]]:
    cards = page.locator('a[href*="/guidebooks"]')
    out: List[Dict[str, str]] = []
    seen = set()
    try:
        n = min(cards.count(), 24)
        for i in range(n):
            a = cards.nth(i)
            url = a.get_attribute("href") or ""
            title = a.inner_text().strip().replace("\n", " ")
            if url and title and url not in seen:
                if url.startswith("/"):
                    url = "https://www.airbnb.com" + url
                out.append({"title": title, "url": url})
                seen.add(url)
    except Exception:
        pass
    if out:
        logger.info(f"✅ Found {len(out)} guidebooks")
    return out


MONTHS = "|".join([
    "January","February","March","April","May","June","July","August","September","October","November","December",
    "Jan","Feb","Mar","Apr","Jun","Jul","Aug","Sep","Sept","Oct","Nov","Dec"
])

def _extract_travels(page: Page, logger: logging.Logger) -> List[Dict[str, Union[str, int]]]:
    """
    Parse the 'Where <name> has been' block.
    line1: City, Country
    line2: Month Year or "<n> trips"
    """
    sel_candidates = [
        'section:has(h2:text-matches("^\\s*Where\\b.*\\bhas\\s+been\\b", "i"))',
        'section:has(h2:has-text("Where")):has(h2:has-text("has been"))'
    ]
    sec: Optional[Locator] = None
    for sel in sel_candidates:
        node = page.locator(sel).first
        if node.count():
            sec = node
            break
    if not sec:
        return []

    try:
        txt = sec.inner_text()
    except Exception:
        txt = ""
    if not txt:
        return []

    results: List[Dict[str, Union[str, int]]] = []
    lines = [l.strip() for l in txt.splitlines() if l.strip()]
    for i in range(len(lines) - 1):
        line1 = lines[i]
        line2 = lines[i+1]
        if re.match(r"^[A-Za-zÀ-ÿ'.\- ]+,\s+[A-Za-zÀ-ÿ'.\- ]+$", line1):
            if re.search(rf"({MONTHS})\s+\d{{4}}", line2, re.IGNORECASE) or re.search(r"\b\d+\s+trips?\b", line2, re.IGNORECASE):
                city = line1.split(",")[0].strip()
                country = line1.split(",")[1].strip()
                trips = 0
                m = re.search(r"\b(\d+)\s+trips?\b", line2, re.IGNORECASE)
                if m:
                    trips = int(m.group(1))
                results.append({
                    "place": city,
                    "country": country,
                    "trips": trips,
                    "when": line2
                })
    if results:
        logger.info(f"✅ Parsed {len(results)} visited places")
    return results


def _extract_some_reviews(page: Page, logger: logging.Logger, max_reviews: int = 0) -> List[Dict[str, Any]]:
    if max_reviews <= 0:
        return []
    _click_if_exists(
        page,
        ['button:has-text("Show more reviews")', 'a:has-text("Show more reviews")'],
        logger,
        "Show more reviews"
    )
    container = page.locator('div[role="dialog"]').first
    root: Union[Page, Locator] = container if container.count() else page
    blocks = root.locator('[data-testid*="review"], section:has(h2:has-text("reviews")) div')
    out: List[Dict[str, Any]] = []
    try:
        total = min(blocks.count(), max_reviews)
        for i in range(total):
            b = blocks.nth(i)
            text = b.inner_text().strip()
            if not text or len(text) < 20:
                continue
            lines = [l.strip() for l in text.splitlines() if l.strip()]
            reviewer = lines[0] if lines else None
            when_line = None
            rating = None
            for l in lines[:5]:
                if "day" in l or "ago" in l or "Today" in l or re.search(r"\b\d{4}\b", l):
                    when_line = l
                    break
            m = re.search(r'(\d(?:\.\d)?)\s*out of\s*5', text, re.IGNORECASE)
            if m:
                try:
                    rating = float(m.group(1))
                except Exception:
                    rating = None
            out.append({
                "reviewId": f"rev_{i}_{hash(text)%10_000_000}",
                "sourceListingId": None,
                "reviewer_name": reviewer,
                "reviewer_location": None,
                "rating": rating,
                "date_text": when_line,
                "text": text[:2000]
            })
    except Exception:
        pass
    if out:
        logger.info(f"✅ Collected {len(out)} reviews")
    return out


# -----------------------------
# Main scraper
# -----------------------------

def scrape_host(host_url: str):
    logger = Utils.setup_logger()
    db = Utils.connect_db()
    SQL.init_all_tables(db)

    HOST_MAX_LISTINGS = getattr(Config, "HOST_MAX_LISTINGS_PER_RUN", 500)
    HOST_DETAIL_SCRAPE_LIMIT = getattr(Config, "HOST_DETAIL_SCRAPE_LIMIT", 500)

    user_id = _parse_user_id_from_url(host_url)
    if not user_id:
        logger.error(f"[host] Could not parse user id from url: {host_url}")
        return

    logger.info(f"🔎 Host scrape start | userId={user_id} | url={host_url}")

    request_headers: Dict[str, str] = {}
    request_item_token: Optional[str] = None
    request_item_client_id: Optional[str] = None
    x_airbnb_api_key = 
    request_client_version: Optional[str] = None

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

        def handle_request(route: Route):
            nonlocal request_item_token, request_item_client_id, request_headers, x_airbnb_api_key, request_client_version
            req = route.request
            url = req.url
            if "/api/v3/StaysPdpSections" in url:
                token = _extract_pdp_token_from_request(req)
                if token and not request_item_token:
                    request_item_token = token
                    logger.info(f"[route] PDP token = {request_item_token}")
                request_item_client_id = req.headers.get('x-client-request-id')
                request_headers = req.headers.copy()  # type: ignore[assignment]
                api_k = req.headers.get('x-airbnb-api-key')
                if api_k:
                    x_airbnb_api_key = api_k
                request_client_version = req.headers.get('x-client-version') or request_client_version
                route.continue_()
                return
            route.continue_()

        context.route('**/api/v3/*', handle_request)

        def on_request(req: Request):
            nonlocal request_item_token, request_item_client_id, request_headers, x_airbnb_api_key, request_client_version
            if "/api/v3/StaysPdpSections" in req.url:
                token = _extract_pdp_token_from_request(req)
                if token and not request_item_token:
                    request_item_token = token
                    request_item_client_id = req.headers.get('x-client-request-id')
                    logger.info(f"[event] PDP token captured = {request_item_token}")
                request_headers = req.headers.copy()  # type: ignore[assignment]
                api_k = req.headers.get('x-airbnb-api-key')
                if api_k:
                    x_airbnb_api_key = api_k
                request_client_version = req.headers.get('x-client-version') or request_client_version

        context.on("request", on_request)

        page = context.new_page()
        page.set_default_timeout(60000)

        logger.info(f"[host] Visiting host page…")
        page.goto(host_url, wait_until='domcontentloaded', timeout=60000)
        ScrapingUtils._dismiss_any_popups_enhanced(page, logger, max_attempts=4)

        # --- ABOUT/BIO/PHOTO + badges ---
        profile_photo_url: Optional[str] = None
        try:
            avatar = page.locator('img[src*="/user/"]').first
            if avatar.count() and avatar.is_visible():
                profile_photo_url = avatar.get_attribute("src")
        except Exception:
            pass

        is_super = 1 if page.locator(':text("Superhost")').count() else 0
        is_ver = 1 if page.locator(':text("Identity verified"), :text("verified")').count() else 0

        ab = _extract_about_and_bio(page, logger)
        about_text = ab.get("about_text")
        bio_text = ab.get("bio_text")

        # Guidebooks & travels
        guidebooks = _extract_guidebooks(page, logger)
        if guidebooks:
            SQL.replace_host_guidebooks(db, user_id, guidebooks)

        travels = _extract_travels(page, logger)
        if travels:
            SQL.replace_host_travels(db, user_id, travels)

        # Optional reviews sampling (disabled by default)
        reviews = _extract_some_reviews(page, logger, max_reviews=0)
        if reviews:
            SQL.upsert_host_reviews(db, user_id, reviews)

        # Save early snapshot
        SQL.upsert_host_profile(db, {
            "userId": user_id,
            "name": None,
            "isSuperhost": is_super,
            "isVerified": is_ver,
            "ratingAverage": None,
            "ratingCount": None,
            "profile_url": host_url,
            "scraping_time": int(time.time()),
            "profile_photo_url": profile_photo_url,
            "about_text": about_text,
            "bio_text": bio_text
        })

        # --- Human-ish move
        human = HumanMouseMovement(page)
        vp = page.viewport_size or {"width": 1400, "height": 900}
        human.move_to(int(vp["width"] * 0.45), int(vp["height"] * 0.45))

        # --- Open all listings & expand grid
        _open_all_listings_and_expand(page, logger)

        # --- Gather listing links
        listing_links = _collect_room_links_from_dom(page, logger, max_scrolls=60)
        if not listing_links:
            logger.warning("[host] No /rooms/ links found on host page.")

        listing_items = []
        for link in listing_links:
            m = re.search(r"/rooms/(\d+)", link)
            if not m:
                continue
            lid = m.group(1)
            listing_items.append({
                "listingId": str(lid),
                "listingUrl": f"https://www.airbnb.com/rooms/{lid}"
            })

        try:
            SQL.replace_host_listings(db, user_id, listing_items)
            logger.info(f"[host] Saved {len(listing_items)} listing rows for userId={user_id}")
        except Exception as e:
            logger.warning(f"[host] Failed saving host_listings: {e}")

        # Update profile row with actual count
        SQL.upsert_host_profile(db, {
            "userId": user_id,
            "total_listings": len(listing_items),
            "profile_url": host_url,
            "scraping_time": int(time.time()),
            "profile_photo_url": profile_photo_url,
            "about_text": about_text,
            "bio_text": bio_text
        })

        # Ensure PDP token
        if listing_items and not request_item_token:
            _ensure_pdp_token_via_link(context, logger, listing_items[0]["listingUrl"])

        # --- Hydrate PDP details
        processed = 0
        detailed = 0
        HOST_MAX = min(HOST_MAX_LISTINGS, len(listing_items))

        for item in listing_items[:HOST_MAX]:
            _id = item["listingId"]
            processed += 1

            if not SQL.check_if_listing_exists(db, _id):
                try:
                    SQL.insert_basic_listing(db, {
                        "id": _id,
                        "ListingObjType": "REGULAR",
                        "roomTypeCategory": "unavailable",
                        "title": None,
                        "name": None,
                        "picture": None,
                        "checkin": None,
                        "checkout": None,
                        "price": None,
                        "discounted_price": None,
                        "original_price": None,
                        "link": item["listingUrl"],
                    })
                except Exception as e:
                    logger.warning(f"[host] Could not insert basic listing {_id}: {e}")

            if request_item_token and detailed < HOST_DETAIL_SCRAPE_LIMIT:
                try:
                    info = {
                        "id": _id,
                        "link": item["listingUrl"],
                        "title": None,
                        "categoryTag": None,
                        "photoId": None,
                        "checkin": None,
                        "checkout": None,
                    }
                    dd = ScrapingUtils.scrape_single_result(
                        context=context,
                        item_search_token=request_item_token,
                        listing_info=info,
                        logger=logger,
                        api_key="d306zoyjsyarp7ifhu67rjxn52tv0t20",
                        client_version=request_client_version or "",
                        client_request_id=request_item_client_id or "",
                        federated_search_id="",
                        currency="MAD",
                        locale="en",
                        base_headers=request_headers,
                    )
                    if not dd.get("skip", False):
                        SQL.update_listing_with_details(db, _id, dd)
                        detailed += 1
                        logger.info(f"[host] ✅ hydrated {_id}")

                        if dd.get("userId") == user_id:
                            SQL.upsert_host_profile(db, {
                                "userId": user_id,
                                "name": dd.get("host"),
                                "isSuperhost": int(bool(dd.get("isSuperhost"))),
                                "isVerified": int(bool(dd.get("isVerified"))),
                                "ratingAverage": dd.get("hostrAtingAverage"),
                                "ratingCount": dd.get("ratingCount"),
                                "years": dd.get("years"),
                                "months": dd.get("months"),
                                "total_listings": len(listing_items),
                                "profile_url": host_url,
                                "scraping_time": int(time.time()),
                                "profile_photo_url": profile_photo_url,
                                "about_text": about_text,
                                "bio_text": bio_text
                            })
                except Exception as e:
                    logger.info(f"[host] ❌ PDP hydrate failed for {_id}: {e}")

            time.sleep(random.uniform(0.5, 1.2))

        logger.info(f"🎉 [host] COMPLETE | processed listings: {processed} | hydrated: {detailed}")

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
        print("Usage: python host_agent.py <host_profile_url>")
        print(" e.g. python host_agent.py https://www.airbnb.com/users/show/532236013")
        raise SystemExit(1)
    scrape_host(sys.argv[1])
