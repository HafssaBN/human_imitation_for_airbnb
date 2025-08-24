# host_agent.py
import os
import re
import time
import json
import urllib.parse
import logging
import random
import sqlite3
from typing import List, Optional, Set

from playwright.sync_api import (
    sync_playwright, Page, BrowserContext, Browser, Route, Request
)
from undetected_playwright import Tarnished

import Config
import host_utils as Utils
import host_SQL as SQL
import ScrapingUtils
from HumanMouseMovement import HumanMouseMovement


def _extract_pdp_token_from_request(req: Request) -> Optional[str]:
    """Clone of your PDP token extractor."""
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
    """
    Accepts:
      - https://www.airbnb.com/users/show/599812664
      - https://www.airbnb.com/users/599812664
    Returns "599812664" or None.
    """
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


def _collect_room_links_from_dom(page: Page, logger: logging.Logger, max_scrolls: int = 30) -> List[str]:
    """Scroll host page and collect all '/rooms/<id>' links."""
    all_links: List[str] = []
    last_len = 0
    for i in range(max_scrolls):
        try:
            anchors = page.locator('a[href*="/rooms/"]')
            count = anchors.count()
            for idx in range(min(count, 400)):
                href = anchors.nth(idx).get_attribute("href") or ""
                if "/rooms/" in href:
                    if href.startswith("/"):
                        href = "https://www.airbnb.com" + href
                    href = href.split("?")[0]
                    all_links.append(href)
        except Exception:
            pass

        # human-ish scroll
        page.mouse.wheel(0, random.randint(900, 1400))
        time.sleep(random.uniform(0.25, 0.6))

        # stop if no new links are appearing
        if len(all_links) == last_len and i > 5:
            break
        last_len = len(all_links)

    deduped = _dedupe_keep_order(all_links)
    logger.info(f"[host] Collected {len(deduped)} unique room links from DOM")
    return deduped


def _ensure_pdp_token_via_link(context: BrowserContext, logger: logging.Logger, link: str) -> bool:
    """Open a PDP once to trigger StaysPdpSections (token captured by listeners)."""
    page = context.new_page()
    try:
        logger.info(f"[pdp-capture] Opening PDP link to capture token: {link}")
        page.goto(link, wait_until="domcontentloaded", timeout=60000)
        ScrapingUtils._dismiss_any_popups_enhanced(page, logger, max_attempts=3)
        page.wait_for_timeout(1200)
        return True
    except Exception as e:
        logger.info(f"[pdp-capture] failed: {e}")
        return False
    finally:
        try: page.close()
        except Exception: pass


def scrape_host(host_url: str):
    logger = Utils.setup_logger()
    db = Utils.connect_db()
    SQL.init_all_tables(db)

    # ensure host table exists
    SQL.execute_sql_query_no_results(db, SQL.create_listing_tracking_table)
    SQL.execute_sql_query_no_results(db, SQL.create_listing_index)
    SQL.execute_sql_query_no_results(db, SQL.create_tracking_table)
    SQL.execute_sql_query_no_results(db, SQL.create_boundaries_tracking_table)
    SQL.execute_sql_query_no_results(db, SQL.create_host_tracking_table)

    HOST_MAX_LISTINGS = getattr(Config, "HOST_MAX_LISTINGS_PER_RUN", 500)
    HOST_DETAIL_SCRAPE_LIMIT = getattr(Config, "HOST_DETAIL_SCRAPE_LIMIT", 500)

    user_id = _parse_user_id_from_url(host_url)
    if not user_id:
        logger.error(f"[host] Could not parse user id from url: {host_url}")
        return

    logger.info(f"🔎 Host scrape start | userId={user_id} | url={host_url}")

    # PDP token & headers
    request_headers = {}
    request_item_token: Optional[str] = None
    request_item_client_id: Optional[str] = None
    x_airbnb_api_key = 
    request_client_version = None

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
                request_headers = req.headers.copy()
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
                request_headers = req.headers.copy()
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

        # Human-like move
        human = HumanMouseMovement(page)
        vp = page.viewport_size or {"width": 1400, "height": 900}
        human.move_to(int(vp["width"] * 0.45), int(vp["height"] * 0.45))

        # Grab listing links from host page
        listing_links = _collect_room_links_from_dom(page, logger, max_scrolls=30)
        if not listing_links:
            logger.warning("[host] No /rooms/ links found on host page.")

        # Ensure PDP token: open first listing if needed
        if listing_links and not request_item_token:
            _ensure_pdp_token_via_link(context, logger, listing_links[0])

        # Store a host snapshot (basic)
        SQL.upsert_host_profile(db, {
            "userId": user_id,
            "name": None,
            "isSuperhost": None,
            "isVerified": None,
            "ratingAverage": None,
            "ratingCount": None,
            "years": None,
            "months": None,
            "total_listings": len(listing_links),
            "profile_url": host_url,
            "scraping_time": int(time.time())
        })

        processed = 0
        detailed = 0

        for link in listing_links[:HOST_MAX_LISTINGS]:
            _id = ScrapingUtils._normalize_listing_id(link)
            if not _id:
                m = re.search(r"/rooms/(\d+)", link)
                _id = m.group(1) if m else None
            if not _id:
                logger.info(f"[host] Skip non-numeric listing link: {link}")
                continue

            processed += 1

            # Ensure a row exists (basic)
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
                        "link": f"https://www.airbnb.com/rooms/{_id}",
                    })
                except Exception as e:
                    logger.warning(f"[host] Could not insert basic listing {_id}: {e}")

            # Hydrate with PDP details
            if request_item_token and detailed < HOST_DETAIL_SCRAPE_LIMIT:
                try:
                    info = {
                        "id": _id,
                        "link": f"https://www.airbnb.com/rooms/{_id}",
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
                        api_key=x_airbnb_api_key,
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

                        # If PDP host block matches this user, update host profile
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
                                "total_listings": len(listing_links),
                                "profile_url": host_url,
                                "scraping_time": int(time.time())
                            })
                except Exception as e:
                    logger.info(f"[host] ❌ PDP hydrate failed for {_id}: {e}")

            time.sleep(random.uniform(0.6, 1.4))

        logger.info(f"[host] DONE | processed listings: {processed} | hydrated: {detailed}")

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
        print(" e.g. python host_agent.py https://www.airbnb.com/users/show/599812664")
        raise SystemExit(1)
    scrape_host(sys.argv[1])
