# --- START OF FILE HostScrapingUtils.py ---
import base64
import json
import logging
import re
import time
from typing import Any, Dict, Optional

from playwright.sync_api import BrowserContext, Page

def _normalize_listing_id(raw_id: Any, item: Optional[Dict[str, Any]] = None) -> Optional[str]:
    if raw_id is None:
        if item and isinstance(item, dict):
            for k in ("listingId", "id", "roomId"):
                v = item.get(k)
                if v: return _normalize_listing_id(v)
        return None
    if isinstance(raw_id, int): return str(raw_id)
    if isinstance(raw_id, str):
        s = raw_id.strip()
        if s.isdigit(): return s
        try:
            if "e+" in s.lower(): return str(int(float(s)))
        except ValueError: pass
        for prefix in ("StayListing:", "DemandStayListing:", "StayListingProduct:", "listing:", "rooms/"):
            if prefix in s:
                tail = s.split(prefix)[-1]
                digits = re.findall(r"\d+", tail)
                if digits: return digits[0]
        if "/" in s and "rooms" in s:
            parts = [p for p in s.split("/") if p]
            for p in reversed(parts):
                if p.isdigit(): return p
        if len(s) > 10 and not s.isdigit():
            try:
                missing = len(s) % 4
                if missing: s += "=" * (4 - missing)
                decoded = base64.b64decode(s).decode("utf-8", errors="ignore")
                for prefix in ("StayListing:", "DemandStayListing:", "StayListingProduct:"):
                    if prefix in decoded:
                        num = decoded.split(prefix)[-1].split(",")[0].strip()
                        if num.isdigit(): return num
                if decoded.strip().isdigit(): return decoded.strip()
            except Exception: pass
    return None

def _dismiss_any_popups_enhanced(page: Page, logger: Optional[logging.Logger] = None, max_attempts: int = 3):
    attempts = 0
    while attempts < max_attempts:
        attempts += 1
        translation_selectors = ['div[role="dialog"]:has-text("Translation on") button[aria-label="Close"]', 'div[role="dialog"]:has-text("Translation") button[aria-label="Close"]', 'button:has-text("Got it")', 'button:has-text("No thanks")', 'button:has-text("Continue in English")', '[data-testid="translation-banner-dismiss"]', '[data-testid="language-detector-decline"]']
        general_selectors = ['div[role="dialog"] button[aria-label="Close"]', '[data-testid="modal-container"] button[aria-label="Close"]', 'button[aria-label="Dismiss"]', 'button:has-text("Accept all cookies")', 'button:has-text("Accept")', 'button:has-text("OK")', 'button:has-text("Close")']
        def try_click(selectors):
            clicked = False
            for sel in selectors:
                try:
                    els = page.locator(sel)
                    for i in range(els.count()):
                        el = els.nth(i)
                        if el.is_visible(timeout=400):
                            el.click(timeout=2000, force=True)
                            page.wait_for_timeout(300)
                            clicked = True
                except Exception: continue
            return clicked
        did = try_click(translation_selectors) or try_click(general_selectors)
        try:
            if page.locator('div[role="dialog"]:visible').count() > 0:
                for _ in range(2): page.keyboard.press("Escape"); page.wait_for_timeout(150)
                did = True
        except Exception: pass
        if not did: break
        page.wait_for_timeout(250)
    return True

def scrape_single_result(context: BrowserContext, item_search_token: str, listing_info: Dict[str, Any], logger: logging.Logger, api_key: str, client_version: str, client_request_id: str, federated_search_id: str, currency: str, locale: str, base_headers: Optional[Dict[str, str]] = None) -> Dict[str, Any]:
    _id = _normalize_listing_id(listing_info.get("id"))
    if not _id: raise ValueError(f"listing_info.id is not numeric: {listing_info.get('id')!r}")
    logger.info(f"=> Hydrating listing {_id} | {listing_info.get('link')}")
    url = f"https://www.airbnb.com/api/v3/StaysPdpSections/{item_search_token}"
    item_id = base64.b64encode(f"StayListing:{_id}".encode("utf-8")).decode("utf-8")
    variables = {"id": item_id, "useContextualUser": False, "pdpSectionsRequest": {"adults": "1", "categoryTag": listing_info.get("categoryTag"), "children": "0", "federatedSearchId": federated_search_id, "infants": "0", "layouts": ["SIDEBAR", "SINGLE_COLUMN"], "pets": 0, "photoId": listing_info.get("photoId"), "checkIn": listing_info.get("checkin"), "checkOut": listing_info.get("checkout"), "p3ImpressionId": f"p3_{int(time.time())}_P3lbdkkYZMTFJexg"}}
    extensions = {"persistedQuery": {"version": 1, "sha256Hash": item_search_token}}
    def _clean_headers(h: Dict[str, str]) -> Dict[str, str]:
        ignore = {":authority", ":method", ":path", ":scheme", "content-length"}
        return {k: v for k, v in (h or {}).items() if k.lower() not in ignore and not k.startswith(":")}
    headers = _clean_headers(base_headers) if base_headers else {}
    headers.update({"x-airbnb-supports-airlock-v2": "true", "x-airbnb-graphql-platform": "web", "x-airbnb-graphql-platform-client": "minimalist-niobe", "x-niobe-short-circuited": "true", "x-csrf-without-token": "1", "origin": "https://www.airbnb.com", "referer": listing_info.get("link", "https://www.airbnb.com/"), "accept-language": "en-US,en;q=0.9"})
    if api_key: headers["x-airbnb-api-key"] = api_key
    if client_version: headers["x-client-version"] = client_version
    if client_request_id: headers["x-client-request-id"] = client_request_id
    params = {"operationName": "StaysPdpSections", "locale": locale, "currency": currency, "variables": json.dumps(variables), "extensions": json.dumps(extensions)}
    resp = context.request.get(url=url, headers=headers, params=params, timeout=30000)
    txt = resp.text()
    if resp.status != 200: logger.error(f"[PDP] HTTP {resp.status} {resp.status_text}\n{txt[:600]}"); return {"skip": True}
    try: data = resp.json()
    except Exception: logger.error("[PDP] non-JSON response"); return {"skip": True}
    if data.get("errors"): logger.error(f"[PDP] GraphQL errors: {json.dumps(data['errors'])[:800]}"); return {"skip": True}
    root = (((data.get("data") or {}).get("presentation") or {}).get("stayProductDetailPage") or {})
    if not root: logger.info("[PDP] No data payload present; skipping."); return {"skip": True}
    out = {"title": root.get("name"), "roomTypeCategory": root.get("roomTypeCategory"), "allPictures": [], "airbnbLuxe": False, "location": None, "maxGuestCapacity": 0, "isGuestFavorite": False, "host": None, "Urlhost": None, "isSuperhost": False, "isVerified": False, "ratingCount": 0, "userId": None, "userUrl": None, "hostrAtingAverage": 0.0, "reviewsCount": 0, "averageRating": 0.0, "years": 0, "months": 0, "lat": None, "lng": None, "co_hosts": []}
    urls = [(p or {}).get("url") for p in (root.get("photos") or [])]
    out["picture"] = urls[0] if urls else None
    out["allPictures"] = list(dict.fromkeys(urls))
    sbui_sections = (((root.get("sections", {}).get("sbuiData", {}).get("sectionConfiguration", {}).get("root", {}).get("sections", []))))
    for s in sbui_sections:
        if s.get("sectionId") == "LUXE_BANNER": out["airbnbLuxe"] = True; break
    section_list = root.get("sections", {}).get("sections", [])
    for sec in section_list:
        if not isinstance(sec, dict): continue
        try:
            sid, payload = sec.get("sectionId"), sec.get("section", {})
            if sid == "AVAILABILITY_CALENDAR_DEFAULT": out["location"], out["maxGuestCapacity"] = payload.get("localizedLocation"), payload.get("maxGuestCapacity", 0)
            elif sid == "REVIEWS_DEFAULT": out["isGuestFavorite"], out["reviewsCount"], out["averageRating"] = bool(payload.get("isGuestFavorite")), payload.get("overallCount", 0), payload.get("overallRating", 0.0)
            elif sid == "LOCATION_DEFAULT": out["lat"], out["lng"] = payload.get("lat"), payload.get("lng")
            elif sid == "TITLE_DEFAULT":
                out["title"] = payload.get("title")
                out["roomTypeCategory"] = payload.get("roomTypeCategory")
                try:
                    # The primary picture is often found in this section
                    pic_url = payload.get("shareSave", {}).get("embedData", {}).get("pictureUrl")
                    if pic_url:
                        out["picture"] = pic_url
                except (KeyError, TypeError):
                    pass

            elif sid == "BOOK_IT_SIDEBAR":
                try:
                    # Prices are nested inside the "structuredDisplayPrice" object
                    price_data = payload.get("structuredDisplayPrice", {}).get("primaryLine", {})
                    out["price"] = price_data.get("price")
                    out["discounted_price"] = price_data.get("discountedPrice")
                    out["original_price"] = price_data.get("originalPrice")
                except (KeyError, TypeError):
                    pass
            elif sid == "MEET_YOUR_HOST":
                card = payload.get("cardData", {})
                out["host"], out["isSuperhost"], out["isVerified"], out["ratingCount"] = card.get("name"), card.get("isSuperhost", False), card.get("isVerified", False), card.get("ratingCount", 0)
                user_id_b64 = card.get("userId")
                if user_id_b64:
                    try: out["userId"] = base64.b64decode(user_id_b64.encode("utf-8")).decode("utf-8").split(":")[-1]
                    except Exception: out["userId"] = str(user_id_b64)
                if out["userId"]: out["userUrl"] = f"https://www.airbnb.com/users/show/{out['userId']}"; out["Urlhost"] = out["userUrl"]
                time_as_host = card.get("timeAsHost", {})
                out["years"], out["months"], out["hostrAtingAverage"] = time_as_host.get("years", 0), time_as_host.get("months", 0), card.get("ratingAverage", 0.0)
                for ch in (card.get("coHosts") or []):
                    ch_id_raw = ch.get("userId") or ch.get("id")
                    ch_id = None
                    if isinstance(ch_id_raw, str):
                        try: ch_id = base64.b64decode(ch_id_raw.encode("utf-8")).decode("utf-8").split(":")[-1]
                        except Exception: ch_id = ch_id_raw
                    elif isinstance(ch_id_raw, int): ch_id = str(ch_id_raw)
                    out["co_hosts"].append({"co_host_id": ch_id, "co_host_name": ch.get("name"), "co_host_url": f"https://www.airbnb.com/users/show/{ch_id}" if ch_id else None, "co_host_picture": (ch.get("picture") or {}).get("baseUrl") or ch.get("pictureUrl")})
        except Exception: continue
    return out