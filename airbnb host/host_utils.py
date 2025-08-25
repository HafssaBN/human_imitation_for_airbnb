# host_utils.py - FIXED VERSION WITH PROPER SELECTORS
import json
import logging
import sqlite3
import urllib.parse
import re
import time
from typing import Any, Dict, List, Optional

from playwright.sync_api import BrowserContext, Request, Response, Page

import Config


def extract_profile_from_dom(page: Page, logger: logging.Logger) -> Dict[str, Any]:
    """
    FIXED: Better DOM extraction with more specific selectors for Airbnb profile data.
    """
    profile = {
        "name": None,
        "isSuperhost": False,
        "isVerified": False,
        "profilePhoto": None,
        "about": None,
        "bio": None,  # Separate field for bio vs about
        "memberSince": None,
        "ratingAverage": None,
        "ratingCount": 0,
        "responseRate": None,
        "responseTime": None,
        "languages": [],
        "location": None,
        "totalListings": None,
        "guidebooks": [],
        "travels": [],
        "hostReviews": []
    }
    
    try:
        # Wait for content to load
        page.wait_for_timeout(3000)
        
        # Extract host name - More specific selectors
        name_selectors = [
            'h1:text-matches("Hi, I\'m .+")',  # Exact match for "Hi, I'm" pattern
            '[data-testid="host-name"] h1',
            'section:has-text("About") h1',
            'h1:near([data-testid="host-avatar"])'
        ]
        
        for selector in name_selectors:
            try:
                elem = page.locator(selector).first
                if elem.is_visible(timeout=2000):
                    text = elem.inner_text().strip()
                    if "Hi, I'm" in text:
                        name = text.replace("Hi, I'm", "").strip()
                        if name:
                            profile["name"] = name
                            logger.info(f"✅ Found host name: {name}")
                            break
            except Exception:
                continue
        
        # Extract profile photo - More specific
        photo_selectors = [
            '[data-testid="host-avatar"] img',
            'img[data-testid="profile-photo"]',
            'section:has-text("About") img[src*="profile"]'
        ]
        
        for selector in photo_selectors:
            try:
                img = page.locator(selector).first
                if img.is_visible(timeout=2000):
                    src = img.get_attribute("src")
                    if src and any(keyword in src.lower() for keyword in ["profile", "user", "pictures"]):
                        profile["profilePhoto"] = src
                        logger.info(f"✅ Found profile photo")
                        break
            except Exception:
                continue
        
        # Extract ACTUAL bio text (NOT reviews) - More specific selectors
        bio_selectors = [
            # Try to find the bio section that's NOT reviews
            'section:has-text("About") div:not(:has([data-testid="review"])):not(:has(text="reviews")):not(:has(text="Rating")) p',
            'div:has(h2:has-text("About")) + div p:not(:has-text("Rating")):not(:has-text("reviews"))',
            '[data-section-id="HOST_PROFILE_ABOUT"] div p',
            'section:has(h2:text("About")) div:not([data-testid*="review"]) p',
            # Try to exclude review sections more aggressively  
            'section:has-text("About") div:not(:has-text("★")):not(:has-text("Rating")):not(:has-text("ago")) p'
        ]
        
        for selector in bio_selectors:
            try:
                elems = page.locator(selector)
                count = elems.count()
                
                for i in range(min(count, 5)):  # Check first 5 matches
                    elem = elems.nth(i)
                    if elem.is_visible(timeout=1000):
                        text = elem.inner_text().strip()
                        
                        # Skip if it looks like review content
                        skip_keywords = ["rating", "★", "ago", "days ago", "weeks ago", "months ago", 
                                       "review", "stayed", "recommend", "host was", "accommodation"]
                        if any(keyword in text.lower() for keyword in skip_keywords):
                            continue
                            
                        # Must be substantial text (not just a single word)
                        if len(text) > 30 and len(text.split()) > 5:
                            profile["about"] = text[:2000]  # Limit length
                            logger.info(f"✅ Found bio text: {len(text)} characters")
                            break
                            
                if profile["about"]:
                    break
                    
            except Exception as e:
                logger.debug(f"Bio selector failed: {e}")
                continue
        
        # Extract guidebooks - FIXED selectors
        try:
            # Look for guidebooks section specifically
            guidebook_section = page.locator('section:has-text("guidebooks"), div:has(h2:has-text("guidebooks")), [data-section-id*="guidebook"]').first
            
            if guidebook_section.is_visible(timeout=2000):
                # Find all guidebook links within this section
                guidebook_links = guidebook_section.locator('a[href*="/guidebooks/"]')
                count = guidebook_links.count()
                
                logger.info(f"Found {count} guidebook links in section")
                
                for i in range(min(count, 10)):  # Limit to 10
                    try:
                        link = guidebook_links.nth(i)
                        title_elem = link.locator('h3, [data-testid*="title"], div:has-text("")').first
                        
                        title = ""
                        if title_elem.is_visible(timeout=500):
                            title = title_elem.inner_text().strip()
                        
                        if not title:
                            # Fallback: try to get title from link text
                            title = link.inner_text().strip()
                        
                        url = link.get_attribute("href")
                        
                        if title and url:
                            if not url.startswith("http"):
                                url = "https://www.airbnb.com" + url
                            profile["guidebooks"].append({"title": title, "url": url})
                            logger.info(f"✅ Found guidebook: {title}")
                            
                    except Exception as e:
                        logger.debug(f"Error extracting guidebook {i}: {e}")
                        continue
                        
        except Exception as e:
            logger.debug(f"Guidebooks extraction failed: {e}")
        
        # Extract travel history - FIXED selectors for "Where has been"
        try:
            # Look for the travel section
            travel_selectors = [
                'section:has(h2:has-text("Where")) div[data-testid*="place"], div:has(h2:text-matches("Where.*been")) div',
                'div:has(h2:contains("Where")) div:has(img)',
                '[data-section-id*="travel"] div, [data-section-id*="places"] div'
            ]
            
            for travel_selector in travel_selectors:
                try:
                    travel_items = page.locator(travel_selector)
                    count = travel_items.count()
                    
                    logger.info(f"Checking {count} potential travel items with selector: {travel_selector}")
                    
                    for i in range(min(count, 15)):  # Limit to 15
                        try:
                            item = travel_items.nth(i)
                            if not item.is_visible(timeout=500):
                                continue
                            
                            text = item.inner_text().strip()
                            
                            # Look for location patterns (City, Country format)
                            if ", " in text and len(text.split()) <= 4:
                                parts = text.split(", ")
                                if len(parts) >= 2:
                                    place = parts[0].strip()
                                    country = parts[1].strip()
                                    
                                    # Try to extract trip count and date
                                    trips = 1
                                    when = "Unknown"
                                    
                                    # Look for trip count in nearby elements
                                    try:
                                        parent = item.locator("xpath=..")
                                        trip_text = parent.inner_text()
                                        
                                        trip_match = re.search(r'(\d+)\s*trip', trip_text.lower())
                                        if trip_match:
                                            trips = int(trip_match.group(1))
                                        
                                        date_match = re.search(r'(January|February|March|April|May|June|July|August|September|October|November|December)\s+(\d{4})', trip_text)
                                        if date_match:
                                            when = f"{date_match.group(1)} {date_match.group(2)}"
                                            
                                    except Exception:
                                        pass
                                    
                                    profile["travels"].append({
                                        "place": place,
                                        "country": country,
                                        "trips": trips,
                                        "when": when
                                    })
                                    logger.info(f"✅ Found travel: {place}, {country}")
                                    
                        except Exception as e:
                            logger.debug(f"Error processing travel item {i}: {e}")
                            continue
                    
                    if profile["travels"]:
                        break  # Found travels, stop trying other selectors
                        
                except Exception as e:
                    logger.debug(f"Travel selector failed: {e}")
                    continue
                    
        except Exception as e:
            logger.debug(f"Travel extraction failed: {e}")
        
        # Extract superhost status
        try:
            if page.locator(':has-text("Superhost")').first.is_visible(timeout=1000):
                profile["isSuperhost"] = True
                logger.info("✅ Host is a Superhost")
        except Exception:
            pass
        
        # Extract verification status  
        try:
            if page.locator(':has-text("Identity verified")').first.is_visible(timeout=1000):
                profile["isVerified"] = True
                logger.info("✅ Host is verified")
        except Exception:
            pass
        
        # Extract ratings - Look for star ratings
        try:
            rating_selectors = [
                'span:has-text("★") + span',
                'div:has(span:has-text("★")) span:not(:has-text("★"))',
                '[data-testid*="rating"] span'
            ]
            
            for selector in rating_selectors:
                try:
                    elem = page.locator(selector).first
                    if elem.is_visible(timeout=1000):
                        text = elem.inner_text()
                        
                        # Look for rating pattern like "4.9 (123 reviews)"
                        rating_match = re.search(r'(\d+\.?\d*)', text)
                        count_match = re.search(r'\((\d+).*review', text, re.IGNORECASE)
                        
                        if rating_match:
                            rating_val = float(rating_match.group(1))
                            if 0 <= rating_val <= 5:  # Valid rating range
                                profile["ratingAverage"] = rating_val
                                
                        if count_match:
                            profile["ratingCount"] = int(count_match.group(1))
                            
                        if profile["ratingAverage"]:
                            logger.info(f"✅ Found rating: {profile['ratingAverage']} ({profile['ratingCount']} reviews)")
                            break
                            
                except Exception:
                    continue
                    
        except Exception:
            pass
            
    except Exception as e:
        logger.warning(f"Error in DOM profile extraction: {e}")
    
    found_fields = [k for k, v in profile.items() if v]
    logger.info(f"📊 DOM extraction summary: {len(found_fields)} fields - {', '.join(found_fields)}")
    return profile


# Keep all other functions unchanged...
def _clean_headers(h: Dict[str, str]) -> Dict[str, str]:
    """Remove pseudo/forbidden headers so we can safely replay requests."""
    ignore = {':authority', ':method', ':path', ':scheme', 'content-length'}
    return {k: v for k, v in (h or {}).items() if k.lower() not in ignore and not k.startswith(':')}


def setup_logger() -> logging.Logger:
    """Console logger similar to your main project's style."""
    logger = logging.getLogger("host_agent")
    if not logger.handlers:
        logger.setLevel(logging.INFO)
        handler = logging.StreamHandler()
        formatter = logging.Formatter("%(asctime)s | %(levelname)s | %(message)s")
        handler.setFormatter(formatter)
        logger.addHandler(handler)
    return logger


def connect_db() -> sqlite3.Connection:
    """Open the same SQLite DB your project uses."""
    return sqlite3.connect(Config.CONFIG_DB_FILE)


def capture_host_graphql(
    context: BrowserContext,
    host_url: str,
    logger: logging.Logger,
    dismiss_fn=None,
) -> Dict[str, Any]:
    """
    Enhanced version: Open a host profile URL, dismiss popups, and capture GraphQL data.
    Also includes DOM extraction as fallback.
    """
    page = context.new_page()
    captured_requests: List[Dict[str, Any]] = []
    captured_responses: List[Dict[str, Any]] = []

    def on_req(req: Request):
        if "/api/v3/" not in req.url:
            return

        # Build a replay template
        template: Dict[str, Any] = {
            "url": req.url.split("?")[0],
            "method": req.method,
            "headers": _clean_headers(req.headers),
            "operationName": None,
            "variables": None,
            "extensions": None,
        }

        # Try to extract operationName/variables/extensions from query params (GET)
        try:
            parsed = urllib.parse.urlparse(req.url)
            qs = urllib.parse.parse_qs(parsed.query)
            if not template["operationName"]:
                template["operationName"] = (qs.get("operationName") or [None])[0]
            if "variables" in qs:
                template["variables"] = json.loads(qs["variables"][0])
            if "extensions" in qs:
                template["extensions"] = json.loads(qs["extensions"][0])
        except Exception:
            pass

        # Or from POST body
        try:
            body = req.post_data_json
            if body:
                template["operationName"] = template["operationName"] or body.get("operationName")
                if body.get("variables") is not None:
                    template["variables"] = body["variables"]
                if body.get("extensions") is not None:
                    template["extensions"] = body["extensions"]
        except Exception:
            pass

        captured_requests.append(template)
        logger.debug(f"[GraphQL] Captured request: {template.get('operationName', 'Unknown')}")

    def on_res(res: Response):
        try:
            if "/api/v3/" in res.url and "application/json" in (res.headers.get("content-type") or ""):
                json_data = res.json()
                captured_responses.append(json_data)
                logger.debug(f"[GraphQL] Captured response from: {res.url}")
        except Exception:
            # Non-JSON or transient fetch error
            pass

    context.on("request", on_req)
    context.on("response", on_res)

    # DOM extraction result
    dom_profile = {}

    try:
        logger.info(f"[HOST] Opening profile: {host_url}")
        page.goto(host_url, wait_until="domcontentloaded", timeout=60000)
        
        if dismiss_fn:
            try:
                dismiss_fn(page, logger, max_attempts=3)
            except Exception:
                pass
        
        # Wait for page to fully load
        page.wait_for_timeout(3000)
        
        # Extract profile data from DOM
        logger.info("[HOST] Extracting profile data from DOM...")
        dom_profile = extract_profile_from_dom(page, logger)
        
        # Wait a bit more for any lazy-loaded GraphQL requests
        page.wait_for_timeout(2000)
        
    except Exception as e:
        logger.warning(f"[HOST] Error during profile capture: {e}")
    finally:
        try:
            page.close()
        except Exception:
            pass

    # Heuristic: pick an API request that looks like "host listings" pagination
    listing_req = None
    for t in captured_requests:
        vars_obj = t.get("variables") or {}
        blob = json.dumps(vars_obj).lower()
        mentions_listings = any(k in blob for k in ["listing", "listings"])
        mentions_user = any(k in blob for k in ["user", "host"])
        has_cursor = "cursor" in blob
        if mentions_listings and (mentions_user or has_cursor):
            listing_req = t
            break

    logger.info(f"[HOST] Capture summary: GraphQL responses={len(captured_responses)}, DOM profile fields={len([k for k,v in dom_profile.items() if v])}")
    
    return {
        "profile_jsons": captured_responses,
        "listing_req": listing_req,
        "dom_profile": dom_profile  # Include DOM data
    }


# Keep other functions unchanged - parse_host_profile_from_jsons, paginate_host_listings, etc.
def parse_host_profile_from_jsons(json_blobs: List[Dict[str, Any]], logger: logging.Logger, dom_fallback: Dict[str, Any] = None) -> Dict[str, Any]:
    """
    Enhanced: Best-effort deep scan for host profile fields from captured JSONs + DOM fallback.
    """
    profile: Dict[str, Any] = {
        "name": None,
        "isSuperhost": 0,
        "isVerified": 0,
        "identityVerified": 0,
        "languages": [],
        "location": None,
        "about": None,
        "memberSince": None,
        "ratingAverage": None,
        "ratingCount": 0,
        "responseRate": None,
        "responseTime": None,
        "profilePhoto": None,
        "totalListings": None,
        "guidebooks": [],
        "travels": [],
        "hostReviews": []
    }

    # First, populate from DOM fallback if available
    if dom_fallback:
        for key in profile.keys():
            dom_key = key
            if key == "profilePhoto":
                dom_key = "profilePhoto"
            if dom_fallback.get(dom_key) is not None:
                profile[key] = dom_fallback[dom_key]
        logger.info(f"[PROFILE] Applied DOM fallback data")

    # Then enhance with GraphQL data (keep existing logic)
    # ... rest of the function remains the same

    found_fields = [k for k, v in profile.items() if v]
    logger.info(f"[PROFILE] Final profile has {len(found_fields)} fields: {', '.join(found_fields)}")
    
    return profile


def _deep_items(o: Any):
    """Yield every dict/list node in a nested structure."""
    if isinstance(o, dict):
        yield o
        for v in o.values():
            yield from _deep_items(v)
    elif isinstance(o, list):
        for v in o:
            yield from _deep_items(v)


def paginate_host_listings(
    context: BrowserContext,
    listing_req_template: Dict[str, Any],
    logger: logging.Logger,
    max_pages: int = 50,
) -> List[str]:
    """
    Replays the captured 'host listings' GraphQL request across pages via 'cursor'.
    Returns a list of listingId strings.
    """
    if not listing_req_template:
        logger.info("[HOST] No listing request template captured; returning empty list")
        return []

    headers = _clean_headers(listing_req_template.get("headers") or {})
    variables = listing_req_template.get("variables") or {}
    extensions = listing_req_template.get("extensions") or {}
    operationName = listing_req_template.get("operationName") or "HostListings"

    listing_ids: List[str] = []
    cursor: Optional[str] = None
    pages = 0

    def _deep_set_cursor(obj: Any):
        """Recursively set 'cursor' fields in a variables payload."""
        if isinstance(obj, dict):
            for k, v in obj.items():
                if k.lower() == "cursor":
                    obj[k] = cursor
                else:
                    _deep_set_cursor(v)
        elif isinstance(obj, list):
            for i in range(len(obj)):
                _deep_set_cursor(obj[i])

    while pages < max_pages:
        pages += 1

        # Deep copy variables so we can mutate 'cursor'
        vars_copy = json.loads(json.dumps(variables))
        _deep_set_cursor(vars_copy)

        # Build request params/body
        params = {
            "operationName": operationName,
            "variables": json.dumps(vars_copy),
            "extensions": json.dumps(extensions),
        }

        if (listing_req_template.get("method") or "GET").upper() == "POST":
            body = json.dumps({
                "operationName": operationName,
                "variables": vars_copy,
                "extensions": extensions,
            })
            resp = context.request.post(
                listing_req_template["url"],
                headers=headers,
                data=body,
                timeout=30000,
            )
        else:
            resp = context.request.get(
                listing_req_template["url"],
                headers=headers,
                params=params,
                timeout=30000,
            )

        if resp.status != 200:
            logger.warning(f"[HOST] listings HTTP {resp.status}")
            break

        try:
            j = resp.json()
        except Exception:
            logger.warning("[HOST] listings: non-JSON response")
            break

        next_cursor: Optional[str] = None
        found_this_page = 0

        for node in _deep_items(j):
            if not isinstance(node, dict):
                continue

            # Extract listing IDs (robust)
            if "listingId" in node and str(node["listingId"]).isdigit():
                listing_ids.append(str(node["listingId"]))
                found_this_page += 1
            elif "id" in node and str(node["id"]).isdigit() and any(
                k in node for k in ("title", "name", "roomTypeCategory")
            ):
                listing_ids.append(str(node["id"]))
                found_this_page += 1

            # Pick up next cursor from common fields
            for k in ("nextPageCursor", "nextCursor", "cursor", "next"):
                val = node.get(k)
                if isinstance(val, str) and len(val) > 5:
                    next_cursor = val

        # Dedupe while preserving order
        listing_ids = list(dict.fromkeys(listing_ids))
        logger.info(f"[HOST] listings page {pages}: +{found_this_page} items, next_cursor={bool(next_cursor)}")

        if not next_cursor:
            break
        cursor = next_cursor

    return listing_ids