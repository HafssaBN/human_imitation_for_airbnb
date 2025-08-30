
import base64
import json
import logging
import re
import time
from typing import Any, Dict, Optional, Union, List

from playwright.sync_api import BrowserContext, Page


def _normalize_listing_id(raw_id: Any, item: Optional[Dict[str, Any]] = None) -> Optional[str]:
    # This function is correct.
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
        try:
            if "e+" in s.lower():
                return str(int(float(s)))
        except ValueError:
            pass
        for prefix in ("StayListing:", "DemandStayListing:", "StayListingProduct:", "listing:", "rooms/"):
            if prefix in s:
                tail = s.split(prefix)[-1]
                digits = re.findall(r"\d+", tail)
                if digits:
                    return digits[0]
        if "/" in s and "rooms" in s:
            parts = [p for p in s.split("/") if p]
            for p in reversed(parts):
                if p.isdigit():
                    return p
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


def _dismiss_any_popups_enhanced(page: Page, logger: Optional[logging.Logger] = None, max_attempts: int = 3) -> bool:
    attempts = 0
    while attempts < max_attempts:
        attempts += 1
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

        def try_click(selectors: List[str]) -> bool:
            clicked = False
            for sel in selectors:
                try:
                    els = page.locator(sel)
                    cnt = min(els.count(), 8)
                    for i in range(cnt):
                        el = els.nth(i)
                        if el.is_visible(timeout=300):
                            el.click(timeout=1200, force=True)
                            try:
                                page.wait_for_timeout(200)
                            except Exception:
                                pass
                            clicked = True
                except Exception:
                    continue
            return clicked

        try:
            did = try_click(translation_selectors) or try_click(general_selectors)
        except Exception:
            break

        try:
            # quick escape on any visible dialog
            if page.locator('div[role="dialog"]').first.is_visible(timeout=200):
                for _ in range(2):
                    page.keyboard.press("Escape")
                    page.wait_for_timeout(120)
                did = True
        except Exception:
            pass

        if not did:
            break
        try:
            page.wait_for_timeout(200)
        except Exception:
            break
    return True


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
            "categoryTag": listing_info.get("categoryTag"),
            "children": "0",
            "federatedSearchId": federated_search_id,
            "infants": "0",
            "layouts": ["SIDEBAR", "SINGLE_COLUMN"],
            "pets": 0,
            "photoId": listing_info.get("photoId"),
            "checkIn": listing_info.get("checkin"),
            "checkOut": listing_info.get("checkout"),
            "p3ImpressionId": f"p3_{int(time.time())}_P3lbdkkYZMTFJexg",
        },
    }
    extensions = {"persistedQuery": {"version": 1, "sha256Hash": item_search_token}}

    def _clean_headers(h: Dict[str, str]) -> Dict[str, str]:
        ignore = {":authority", ":method", ":path", ":scheme", "content-length"}
        return {k: v for k, v in (h or {}).items() if k.lower() not in ignore and not k.startswith(":")}

    headers = _clean_headers(base_headers) if base_headers else {}
    headers.update(
        {
            "x-airbnb-supports-airlock-v2": "true",
            "x-airbnb-graphql-platform": "web",
            "x-airbnb-graphql-platform-client": "minimalist-niobe",
            "x-niobe-short-circuited": "true",
            "x-csrf-without-token": "1",
            "origin": "https://www.airbnb.com",
            "referer": listing_info.get("link", "https://www.airbnb.com/"),
            "accept-language": "en-US,en;q=0.9",
        }
    )
    if api_key:
        headers["x-airbnb-api-key"] = api_key
    if client_version:
        headers["x-client-version"] = client_version
    if client_request_id:
        headers["x-client-request-id"] = client_request_id

    params: Dict[str, Union[str, float, bool]] = {
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

    out = {
        "title": None,
        "roomTypeCategory": None,
        "allPictures": [],  # This will contain ALL picture URLs in order
        "picture": None,  # This will be the primary/first picture
        "airbnbLuxe": False,
        "location": None,
        "maxGuestCapacity": 0,
        "isGuestFavorite": False,
        "host": None,
        "userId": None,
        "userUrl": None,
        "isSuperhost": False,
        "isVerified": False,
        "ratingCount": 0,
        "hostrAtingAverage": 0.0,
        "reviewsCount": 0,
        "averageRating": 0.0,
        "years": 0,
        "months": 0,
        "lat": None,
        "lng": None,
    }
    out["productType"] = root.get("productType") or ""
    out["__typename"] = root.get("__typename") or ""
    out["pdpType"] = root.get("pdpType") or ""

    # ENHANCED PHOTO EXTRACTION - Get ALL photos from multiple sources
    all_photos: List[str] = []

    # Method 1: Extract from root photos array (fallback)
    initial_urls = [(p or {}).get("url") for p in (root.get("photos") or []) if p and p.get("url")]
    if initial_urls:
        all_photos.extend(initial_urls)
        logger.info(f"Found {len(initial_urls)} photos from root.photos")


    # --- Luxe detection (strict, no banner fallback) ---
    ptype = (root.get("productType") or "").upper()
    tname = (root.get("__typename") or "").upper()
    pdp   = (root.get("pdpType") or "").upper()

    out["airbnbLuxe"] = bool(
        ptype == "LUXE" or pdp == "LUXE" or ("LUXE" in tname)
    )


    section_list = root.get("sections", {}).get("sections", [])
    for sec in section_list:
        if not isinstance(sec, dict):
            continue

        try:
            sid, payload = sec.get("sectionId"), sec.get("section", {})

            # Method 2: COMPREHENSIVE PHOTO GALLERY EXTRACTION
            if sid and ("PHOTOGALLERY" in sid or "PHOTO" in sid):
                try:
                    photo_sources = [
                        payload.get("mediaItems", []),
                        payload.get("photos", []),
                        payload.get("images", []),
                        payload.get("galleryItems", []),
                    ]

                    for source in photo_sources:
                        if not source:
                            continue

                        for item in source:
                            if not item or not isinstance(item, dict):
                                continue

                            # Try different URL fields
                            photo_url = None
                            url_fields = ["baseUrl", "url", "originalUrl", "largeUrl", "pictureUrl"]

                            for field in url_fields:
                                if item.get(field):
                                    photo_url = item[field]
                                    break

                            # Also check nested structures
                            if not photo_url:
                                picture_obj = item.get("picture", {})
                                if isinstance(picture_obj, dict):
                                    for field in url_fields:
                                        if picture_obj.get(field):
                                            photo_url = picture_obj[field]
                                            break

                            if photo_url and photo_url not in all_photos:
                                all_photos.append(photo_url)

                    if all_photos:
                        logger.info(f"Found {len(all_photos)} total photos from gallery section {sid}")

                except Exception as e:
                    logger.warning(f"Could not parse photo gallery section {sid} for {_id}: {e}")

            elif sid == "TITLE_DEFAULT":
                out["title"] = payload.get("title")
                out["roomTypeCategory"] = payload.get("roomTypeCategory")
                try:
                    # Get primary picture but don't overwrite our comprehensive list
                    pic_url = payload.get("shareSave", {}).get("embedData", {}).get("pictureUrl")
                    if pic_url and pic_url not in all_photos:
                        # Insert at beginning as it's likely the main photo
                        all_photos.insert(0, pic_url)
                except (KeyError, TypeError):
                    pass

            elif sid == "AVAILABILITY_CALENDAR_DEFAULT":
                out["location"] = payload.get("localizedLocation")
                out["maxGuestCapacity"] = payload.get("maxGuestCapacity", 0)

            elif sid == "REVIEWS_DEFAULT":
                out["isGuestFavorite"] = bool(payload.get("isGuestFavorite"))
                out["reviewsCount"] = payload.get("overallCount", 0)
                out["averageRating"] = payload.get("overallRating", 0.0)

            elif sid == "LOCATION_DEFAULT":
                out["lat"] = payload.get("lat")
                out["lng"] = payload.get("lng")

            elif sid == "MEET_YOUR_HOST":
                card = payload.get("cardData", {})
                out["host"] = card.get("name")
                out["isSuperhost"] = card.get("isSuperhost", False)
                out["isVerified"] = card.get("isVerified", False)
                out["ratingCount"] = card.get("ratingCount", 0)

                user_id_b64 = card.get("userId")
                if user_id_b64:
                    try:
                        out["userId"] = base64.b64decode(user_id_b64.encode("utf-8")).decode("utf-8").split(":")[-1]
                    except Exception:
                        out["userId"] = str(user_id_b64)

                if out["userId"]:
                    out["userUrl"] = f"https://www.airbnb.com/users/show/{out['userId']}"

                time_as_host = card.get("timeAsHost", {})
                out["years"] = time_as_host.get("years", 0)
                out["months"] = time_as_host.get("months", 0)
                out["hostrAtingAverage"] = card.get("ratingAverage", 0.0)

                # co-hosts present but not used downstream; kept for completeness
                for ch in (card.get("coHosts") or []):
                    ch_id_raw = ch.get("userId") or ch.get("id")
                    ch_id = None
                    if isinstance(ch_id_raw, str):
                        try:
                            ch_id = base64.b64decode(ch_id_raw.encode("utf-8")).decode("utf-8").split(":")[-1]
                        except Exception:
                            ch_id = ch_id_raw
                    elif isinstance(ch_id_raw, int):
                        ch_id = str(ch_id_raw)
                    # (no storage here)

        except Exception as e:
            logger.warning(f"Error processing section {sid}: {e}")
            continue

    # Final photo processing - remove duplicates while preserving order
    unique_photos = list(dict.fromkeys([url for url in all_photos if url]))
    out["allPictures"] = unique_photos

    # Set primary picture
    if unique_photos:
        out["picture"] = unique_photos[0]

    logger.info(f"Final photo count for {_id}: {len(unique_photos)} unique photos")

    return out

