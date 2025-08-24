# ScrapingUtils.py
import base64
import json
import logging
import re
import time
from typing import Any, Dict, Optional

from playwright.sync_api import BrowserContext, Page


# -------------------------------
# ID normalizer (robust)
# -------------------------------
def _normalize_listing_id(raw_id: Any, item: Optional[Dict[str, Any]] = None) -> Optional[str]:
    """
    Normalize different ID formats to a numeric string.
    Tries ints, plain strings, URLs (/rooms/12345), Airbnb global IDs, and base64 blobs.
    """
    if raw_id is None:
        if item and isinstance(item, dict):
            for k in ("listingId", "id", "roomId"):
                v = item.get(k)
                if v:
                    return _normalize_listing_id(v)
        return None

    if isinstance(raw_id, int):
        return str(raw_id)

    if isinstance(raw_id, str):
        s = raw_id.strip()

        if s.isdigit():
            return s

        # handle scientific notation strings like "1.234e+10"
        try:
            if "e+" in s.lower():
                return str(int(float(s)))
        except ValueError:
            pass

        # prefixes and URL forms
        for prefix in ("StayListing:", "DemandStayListing:", "StayListingProduct:", "listing:", "rooms/"):
            if prefix in s:
                tail = s.split(prefix)[-1]
                digits = re.findall(r"\d+", tail)
                if digits:
                    return digits[0]

        # /rooms/<id> in arbitrary URLs
        if "/" in s and "rooms" in s:
            parts = [p for p in s.split("/") if p]
            for p in reversed(parts):
                if p.isdigit():
                    return p

        # base64-encoded global ids
        if len(s) > 10 and not s.isdigit():
            try:
                missing = len(s) % 4
                if missing:
                    s += "=" * (4 - missing)
                decoded = base64.b64decode(s).decode("utf-8", errors="ignore")
                for prefix in ("StayListing:", "DemandStayListing:", "StayListingProduct:"):
                    if prefix in decoded:
                        num = decoded.split(prefix)[-1].split(",")[0].strip()
                        if num.isdigit():
                            return num
                if decoded.strip().isdigit():
                    return decoded.strip()
            except Exception:
                pass

    return None


# -------------------------------
# Popup killer used by host_agent
# -------------------------------
def _dismiss_any_popups_enhanced(page: Page, logger: Optional[logging.Logger] = None, max_attempts: int = 3):
    attempts = 0
    while attempts < max_attempts:
        attempts += 1

        # translation / language modals first
        translation_selectors = [
            'div[role="dialog"]:has-text("Translation on") button[aria-label="Close"]',
            'div[role="dialog"]:has-text("Translation") button[aria-label="Close"]',
            'button:has-text("Got it")',
            'button:has-text("No thanks")',
            'button:has-text("Continue in English")',
            '[data-testid="translation-banner-dismiss"]',
            '[data-testid="language-detector-decline"]',
        ]

        general_selectors = [
            'div[role="dialog"] button[aria-label="Close"]',
            '[data-testid="modal-container"] button[aria-label="Close"]',
            'button[aria-label="Dismiss"]',
            'button:has-text("Accept all cookies")',
            'button:has-text("Accept")',
            'button:has-text("OK")',
            'button:has-text("Close")',
        ]

        def try_click(selectors):
            clicked = False
            for sel in selectors:
                try:
                    els = page.locator(sel)
                    count = els.count()
                    for i in range(count):
                        el = els.nth(i)
                        if el.is_visible(timeout=400):
                            el.click(timeout=2000, force=True)
                            page.wait_for_timeout(300)
                            clicked = True
                except Exception:
                    continue
            return clicked

        did = try_click(translation_selectors)
        did = try_click(general_selectors) or did

        # ESC fallback
        try:
            if page.locator('div[role="dialog"]:visible').count() > 0:
                for _ in range(2):
                    page.keyboard.press("Escape")
                    page.wait_for_timeout(150)
                did = True
        except Exception:
            pass

        if not did:
            break

        page.wait_for_timeout(250)

    return True


# -------------------------------
# PDP hydrate (StaysPdpSections)
# -------------------------------
def scrape_single_result(
    context: BrowserContext,
    item_search_token: str,
    listing_info: Dict[str, Any],
    logger: logging.Logger,
    api_key: str,
    client_version: str,
    client_request_id: str,
    federated_search_id: str,
    currency: str,
    locale: str,
    base_headers: Optional[Dict[str, str]] = None,
) -> Dict[str, Any]:
    """
    Fetch detailed listing info via the StaysPdpSections GraphQL endpoint.
    Returns a dict with host/location/ratings/etc. Keys are the ones host_SQL.update_listing_with_details expects.
    """
    _id = _normalize_listing_id(listing_info.get("id"))
    if not _id:
        raise ValueError(f"listing_info.id is not numeric: {listing_info.get('id')!r}")

    logger.info(f"=> Hydrating listing {_id} | {listing_info.get('link')}")

    url = f"https://www.airbnb.com/api/v3/StaysPdpSections/{item_search_token}"
    item_id = base64.b64encode(f"StayListing:{_id}".encode("utf-8")).decode("utf-8")

    variables = {
        "id": item_id,
        "useContextualUser": False,
        "pdpSectionsRequest": {
            "adults": "1",
            "amenityFilters": None,
            "bypassTargetings": False,
            "categoryTag": listing_info.get("categoryTag"),
            "children": "0",
            "federatedSearchId": federated_search_id,
            "hostPreview": False,
            "infants": "0",
            "layouts": ["SIDEBAR", "SINGLE_COLUMN"],
            "pets": 0,
            "photoId": listing_info.get("photoId"),
            "translateUgc": False,
            "checkIn": listing_info.get("checkin"),
            "checkOut": listing_info.get("checkout"),
            "p3ImpressionId": f"p3_{int(time.time())}_P3lbdkkYZMTFJexg",
        },
    }
    extensions = {"persistedQuery": {"version": 1, "sha256Hash": item_search_token}}

    # build headers (prefer intercepted headers if provided)
    def _clean_headers(h: Dict[str, str]) -> Dict[str, str]:
        ignore = {":authority", ":method", ":path", ":scheme", "content-length"}
        return {k: v for k, v in (h or {}).items() if k.lower() not in ignore and not k.startswith(":")}

    if base_headers:
        headers = _clean_headers(base_headers)
    else:
        headers = {}

    headers.update({
        "x-airbnb-supports-airlock-v2": "true",
        "x-airbnb-graphql-platform": "web",
        "x-airbnb-graphql-platform-client": "minimalist-niobe",
        "x-niobe-short-circuited": "true",
        "x-csrf-without-token": "1",
        "origin": "https://www.airbnb.com",
        "referer": listing_info.get("link", "https://www.airbnb.com/"),
        "accept-language": "en-US,en;q=0.9",
    })
    if api_key:
        headers["x-airbnb-api-key"] = api_key
    if client_version:
        headers["x-client-version"] = client_version
    if client_request_id:
        headers["x-client-request-id"] = client_request_id

    params = {
        "operationName": "StaysPdpSections",
        "locale": locale,
        "currency": currency,
        "variables": json.dumps(variables),
        "extensions": json.dumps(extensions),
    }

    resp = context.request.get(url=url, headers=headers, params=params, timeout=30000)
    txt = resp.text()
    if resp.status != 200:
        logger.error(f"[PDP] HTTP {resp.status} {resp.status_text}\n{txt[:600]}")
        return {"skip": True}

    try:
        data = resp.json()
    except Exception:
        logger.error("[PDP] non-JSON response")
        return {"skip": True}

    if data.get("errors"):
        logger.error(f"[PDP] GraphQL errors: {json.dumps(data['errors'])[:800]}")
        return {"skip": True}

    root = (((data.get("data") or {}).get("presentation") or {}).get("stayProductDetailPage") or {})
    if not root:
        logger.info("[PDP] No data payload present; skipping.")
        return {"skip": True}

    sections = (root.get("sections") or {})
    try:
        sbui_sections = (((sections.get("sbuiData") or {}).get("sectionConfiguration") or {}).get("root") or {}).get("sections") or []
    except Exception:
        sbui_sections = []

    out = {
        "airbnbLuxe": False,
        "location": None,
        "maxGuestCapacity": 0,
        "isGuestFavorite": False,
        "host": None,
        "isSuperhost": False,
        "isVerified": False,
        "ratingCount": 0,
        "userId": None,
        "hostrAtingAverage": 0.0,
        "reviewsCount": 0,
        "averageRating": 0.0,
        "years": 0,
        "months": 0,
        "lat": None,
        "lng": None,
    }

    for s in sbui_sections:
        if s.get("sectionId") == "LUXE_BANNER":
            out["airbnbLuxe"] = True

    # 'sections' can be dict or list
    section_list = sections.get("sections") if isinstance(sections, dict) else sections
    if not isinstance(section_list, list):
        return {"skip": True}

    for sec in section_list:
        try:
            sid = sec.get("sectionId")
            payload = sec.get("section", {}) or {}
            if sid == "AVAILABILITY_CALENDAR_DEFAULT":
                out["location"] = payload.get("localizedLocation") or out["location"]
                out["maxGuestCapacity"] = payload.get("maxGuestCapacity", out["maxGuestCapacity"])
            elif sid == "REVIEWS_DEFAULT":
                out["isGuestFavorite"] = bool(payload.get("isGuestFavorite", out["isGuestFavorite"]))
                out["reviewsCount"] = payload.get("overallCount", out["reviewsCount"])
                out["averageRating"] = payload.get("overallRating", out["averageRating"])
            elif sid == "LOCATION_DEFAULT":
                out["lat"] = payload.get("lat", out["lat"])
                out["lng"] = payload.get("lng", out["lng"])
            elif sid == "MEET_YOUR_HOST":
                card = payload.get("cardData", {}) or {}
                out["host"] = card.get("name", out["host"])
                out["isSuperhost"] = card.get("isSuperhost", out["isSuperhost"])
                out["isVerified"] = card.get("isVerified", out["isVerified"])
                out["ratingCount"] = card.get("ratingCount", out["ratingCount"])
                user_id_b64 = card.get("userId")
                if user_id_b64:
                    try:
                        decoded = base64.b64decode(user_id_b64.encode("utf-8")).decode("utf-8")
                        out["userId"] = decoded.split(":")[-1]
                    except Exception:
                        out["userId"] = str(user_id_b64)
                time_as_host = card.get("timeAsHost", {}) or {}
                out["years"] = time_as_host.get("years", out["years"])
                out["months"] = time_as_host.get("months", out["months"])
                out["hostrAtingAverage"] = card.get("ratingAverage", out["hostrAtingAverage"])
        except Exception:
            continue

    return out
