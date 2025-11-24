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


def _scrape_images_from_dom(context: BrowserContext, url: str, logger: logging.Logger, max_imgs: int = 200) -> List[str]:
    """
    Fallback: Open PDP, click 'Show all photos', scrape WHILE scrolling using Keyboard.
    """
    page = context.new_page()
    collected_urls = set()
    
    try:
        logger.info(f"[PDP DOM] Opening PDP to collect images: {url}")
        page.goto(url, wait_until="domcontentloaded", timeout=60000)
        _dismiss_any_popups_enhanced(page, logger, max_attempts=3)

        # --- STEP 1: CLICK "SHOW ALL PHOTOS" ---
        grid_button_clicked = False
        trigger_selectors = [
            '[data-testid="photogallery-grid-view-nearby-button"]',
            'button:has-text("Show all photos")',
            'button[aria-label="Show all photos"]',
        ]

        for sel in trigger_selectors:
            try:
                btn = page.locator(sel).first
                if btn.count() and btn.is_visible():
                    logger.info(f"[PDP DOM] Found gallery button via '{sel}'. Force clicking...")
                    btn.click(timeout=4000, force=True)
                    
                    # Wait for animation
                    page.wait_for_timeout(2500)
                    
                    # --- FIX FOR TRANSLATION POPUP ---
                    # Force press Escape to close any overlapping "Translation on" popups
                    page.keyboard.press("Escape")
                    page.wait_for_timeout(500)
                    # ---------------------------------

                    # Detect if modal is truly open
                    if page.locator('[data-testid="photo-viewer-section"]').count() > 0 or \
                       page.locator('div[role="dialog"][aria-label*="Photo"]').count() > 0:
                        logger.info("[PDP DOM] Photo gallery modal detected open.")
                        grid_button_clicked = True
                        break
            except Exception:
                continue

        # --- STEP 2: SCROLL & SCRAPE (PROGRESSIVE) ---
        # We scrape INSIDE the loop to catch images before they unload (virtual scrolling)
        loops = 60 if grid_button_clicked else 20
        logger.info(f"[PDP DOM] Starting keyboard-driven scroll (Modal: {grid_button_clicked}, Loops: {loops})...")

        # If modal is open, click inside it to ensure keyboard focus
        if grid_button_clicked:
            try:
                page.click('div[role="dialog"]', timeout=1000, force=True)
            except Exception:
                pass

        image_selectors = [
            '[data-testid="photo-viewer-section"] img', 
            'div[role="dialog"] img',                   
            '[data-testid="main-gallery-grid"] img',    
            'img[src*="imagedelivery"]',
            'picture img',
        ]

        for i in range(loops):
            # 1. Scrape what is currently visible
            try:
                for sel in image_selectors:
                    elements = page.locator(sel).all()
                    for el in elements:
                        src = el.get_attribute("src")
                        if not src: continue
                        
                        src = src.strip()
                        lower_src = src.lower()

                        # Filter junk
                        if "/pictures/user/" in lower_src: continue
                        if "airbnb-platform-assets" in lower_src: continue
                        if "static/packages" in lower_src: continue

                        clean_url = src.split("?")[0]
                        collected_urls.add(clean_url)
            except Exception:
                pass

            # Stop if we have plenty
            if len(collected_urls) >= max_imgs:
                break

            # 2. Scroll using Keyboard (Robust)
            try:
                if grid_button_clicked:
                    # Press PageDown twice to move faster
                    page.keyboard.press("PageDown")
                    page.wait_for_timeout(100)
                    page.keyboard.press("PageDown")
                else:
                    # Main page scroll
                    page.mouse.wheel(0, 3000)
                
                # Wait for network lazy load
                page.wait_for_timeout(400)
            except Exception:
                break

        final_list = list(collected_urls)
        logger.info(f"[PDP DOM] Collected {len(final_list)} unique image URLs")
        return final_list

    except Exception as e:
        logger.info(f"[PDP DOM] Fallback failed: {e}")
        return []
    finally:
        try:
            page.close()
        except Exception:
            pass

def _scrape_details_from_dom(context: BrowserContext, url: str, logger: logging.Logger) -> Dict[str, Any]:
    """
    Open PDP and extract details.
    1. Tries to parse hidden 'niobeClientData' JSON (FAST & ACCURATE).
    2. Falls back to visual scraping if JSON is missing.
    """
    page = context.new_page()
    details: Dict[str, Any] = {
        "title": None,
        "location": None,
        "roomTypeCategory": None,
        "maxGuestCapacity": 0,
        "isGuestFavorite": False,
        "reviewsCount": 0,
        "averageRating": 0.0,
        "lat": None,
        "lng": None,
        "host": None,
        "userId": None,
        "userUrl": None,
        "isSuperhost": False,
        "isVerified": False,
        "ratingCount": 0,
        "hostrAtingAverage": 0.0,
        "years": 0,
        "months": 0,
        "allPictures": [] 
    }
    
    try:
        logger.info(f"[PDP DOM] Opening PDP for details: {url}")
        page.goto(url, wait_until="domcontentloaded", timeout=60000)
        _dismiss_any_popups_enhanced(page, logger, max_attempts=3)
        
        # --- STRATEGY 1: PARSE EMBEDDED JSON ---
        try:
            script_content = page.locator('#data-deferred-state-0').inner_text()
            
            if script_content:
                json_data = json.loads(script_content)
                niobe_data = json_data.get("niobeClientData", [])
                
                for item in niobe_data:
                    if not isinstance(item, list) or len(item) < 2: continue
                    payload = item[1]
                    if not isinstance(payload, dict): continue
                    
                    data_root = payload.get("data", {}).get("presentation", {}).get("stayProductDetailPage", {})
                    if not data_root: continue
                    
                    sections = data_root.get("sections", {}).get("sections", [])
                    for sec in sections:
                        sec_data = sec.get("section", {})
                        sec_id = sec.get("sectionId", "")

                        # 1. HOST DETAILS
                        if sec_id == "MEET_YOUR_HOST" or "HOST_OVERVIEW" in sec_id:
                            card = sec_data.get("cardData", {})
                            if card:
                                details["host"] = card.get("name")
                                details["isSuperhost"] = bool(card.get("isSuperhost"))
                                details["isVerified"] = bool(card.get("isVerified"))
                                
                                time_host = card.get("timeAsHost", {})
                                details["years"] = time_host.get("years", 0)
                                details["months"] = time_host.get("months", 0)
                                
                                if card.get("ratingAverage"):
                                    details["hostrAtingAverage"] = float(card.get("ratingAverage"))
                                if card.get("ratingCount"):
                                    details["ratingCount"] = int(card.get("ratingCount"))

                                # ID FIX
                                raw_uid = card.get("userId")
                                raw_context_uid = card.get("contextualUserId")
                                target_id_raw = raw_context_uid if raw_context_uid else raw_uid
                                
                                if target_id_raw:
                                    if "User" in target_id_raw and not target_id_raw.isdigit():
                                        try:
                                            decoded = base64.b64decode(target_id_raw).decode('utf-8')
                                            details["userId"] = decoded.split(":")[-1]
                                        except: 
                                            details["userId"] = target_id_raw
                                    else:
                                        details["userId"] = target_id_raw
                                        
                                if details["userId"]:
                                    details["userUrl"] = f"https://www.airbnb.com/users/show/{details['userId']}"
                        
                        # 2. IMAGES (Photos)
                        if "PHOTO" in sec_id or "HERO" in sec_id:
                            media_items = sec_data.get("mediaItems") or sec_data.get("previewImages") or []
                            for img in media_items:
                                if isinstance(img, dict):
                                    u = img.get("baseUrl") or img.get("url")
                                    if u:
                                        details["allPictures"].append(u)

                        # 3. REVIEWS & RATING
                        if sec_id == "REVIEWS_DEFAULT":
                            details["reviewsCount"] = sec_data.get("overallCount") or details["reviewsCount"]
                            details["averageRating"] = sec_data.get("overallRating") or details["averageRating"]
                            details["isGuestFavorite"] = bool(sec_data.get("isGuestFavorite"))

                        # 4. LOCATION
                        if sec_id == "LOCATION_DEFAULT":
                            details["lat"] = sec_data.get("lat")
                            details["lng"] = sec_data.get("lng")
                            if sec_data.get("subtitle"):
                                details["location"] = sec_data.get("subtitle")
                            elif sec_data.get("previewLocationDetails"):
                                try:
                                    details["location"] = sec_data["previewLocationDetails"][0]["title"]
                                except: pass

                        # 5. TITLE
                        if sec_id == "TITLE_DEFAULT":
                            details["title"] = sec_data.get("title")
                            details["roomTypeCategory"] = sec_data.get("roomTypeCategory")

                        # 6. CAPACITY
                        if "AVAILABILITY" in sec_id:
                            details["maxGuestCapacity"] = sec_data.get("maxGuestCapacity") or details["maxGuestCapacity"]

                    logger.info("[PDP DOM] Successfully extracted data (including images) from embedded JSON.")
                    return details 
        except Exception as e:
            logger.warning(f"[PDP DOM] JSON parsing failed ({e}), attempting visual fallback.")

        # --- STRATEGY 2: VISUAL FALLBACK (Only runs if JSON fails) ---
        
        if not details["location"] or details["location"] == "Where you’ll be":
            try:
                loc_text = page.locator('[data-section-id="LOCATION_DEFAULT"] :text-matches(",")').first.inner_text()
                if loc_text and len(loc_text) < 100:
                    details["location"] = loc_text
            except: pass

        if not details["title"]:
            for sel in ['h1', '[data-testid="title"]']:
                if page.locator(sel).count():
                    details["title"] = page.locator(sel).first.inner_text()
                    break

        if not details["host"]:
            host_el = page.locator('h2:has-text("Hosted by"), h2:has-text("Meet your host")').first
            if host_el.count():
                text = host_el.inner_text()
                details["host"] = text.replace("Hosted by", "").replace("Meet your host", "").strip()

        return details

    except Exception as e:
        logger.info(f"[PDP DOM] Details fallback completely failed: {e}")
        return details
    finally:
        try:
            page.close()
        except Exception:
            pass
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
            "adults": 1,
            "children": 0,
            "infants": 0,
            "pets": 0,
            "layouts": ["SIDEBAR", "SINGLE_COLUMN"],
            "checkIn": listing_info.get("checkin"),
            "checkOut": listing_info.get("checkout"),
            "categoryTag": listing_info.get("categoryTag"),
            "photoId": listing_info.get("photoId"),
            "federatedSearchId": federated_search_id,
            "p3ImpressionId": f"p3_{int(time.time())}_P3lbdkkYZMTFJexg",
        },
    }
    extensions = {"persistedQuery": {"version": 1, "sha256Hash": item_search_token}}

    # Build headers
    headers = dict(base_headers or {})
    headers.setdefault("accept", "application/json, text/plain, */*")
    headers.setdefault("accept-language", "en-US,en;q=0.9")
    headers.setdefault("origin", "https://www.airbnb.com")
    headers.setdefault("referer", listing_info.get("link", "https://www.airbnb.com/"))
    headers.setdefault("x-airbnb-graphql-platform", "web")
    headers.setdefault("x-airbnb-graphql-platform-client", "web")
    headers.setdefault("content-type", "application/json")
    if api_key:
        headers["x-airbnb-api-key"] = api_key
    if client_version:
        headers.setdefault("x-client-version", client_version)
    if client_request_id:
        headers.setdefault("x-client-request-id", client_request_id)

    # Default output structure
    out: Dict[str, Any] = {
        "title": None,
        "roomTypeCategory": None,
        "allPictures": [],
        "picture": None,
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
        "productType": "",
        "__typename": "",
        "pdpType": "",
    }

    # ---- 1. Try GraphQL POST ----
    try:
        resp = context.request.post(url=url, headers=headers, data=json.dumps({
            "operationName": "StaysPdpSections",
            "variables": variables,
            "extensions": extensions,
        }), timeout=30000)

        if resp.status == 200:
            try:
                data = resp.json()
            except Exception:
                data = None
            
            if data and not data.get("errors"):
                root = (((data.get("data") or {}).get("presentation") or {}).get("stayProductDetailPage") or {})
                if root:
                    out["productType"] = root.get("productType") or ""
                    out["__typename"] = root.get("__typename") or ""
                    out["pdpType"] = root.get("pdpType") or ""

                    # Photos from root
                    all_photos: List[str] = []
                    initial_urls = [(p or {}).get("url") for p in (root.get("photos") or []) if p and p.get("url")]
                    if initial_urls:
                        all_photos.extend(initial_urls)
                        logger.info(f"Found {len(initial_urls)} photos from root.photos")

                    # Luxe detection
                    ptype = (root.get("productType") or "").upper()
                    tname = (root.get("__typename") or "").upper()
                    pdp = (root.get("pdpType") or "").upper()
                    out["airbnbLuxe"] = bool(ptype == "LUXE" or pdp == "LUXE" or ("LUXE" in tname))

                    # Sections
                    section_list = ((root.get("sections") or {}).get("sections")) or []
                    for sec in section_list:
                        if not isinstance(sec, dict):
                            continue
                        try:
                            sid = (sec.get("sectionId") or "")
                            payload = sec.get("section") or {}

                            if sid and ("PHOTOGALLERY" in sid or "PHOTO" in sid):
                                try:
                                    photo_sources = [
                                        payload.get("mediaItems", []),
                                        payload.get("photos", []),
                                        payload.get("images", []),
                                        payload.get("galleryItems", []),
                                    ]
                                    for source in photo_sources:
                                        if not source: continue
                                        for item in source:
                                            if not item or not isinstance(item, dict): continue
                                            photo_url = None
                                            url_fields = ["baseUrl", "url", "originalUrl", "largeUrl", "pictureUrl"]
                                            for field in url_fields:
                                                if item.get(field):
                                                    photo_url = item[field]
                                                    break
                                            if not photo_url:
                                                picture_obj = item.get("picture", {})
                                                if isinstance(picture_obj, dict):
                                                    for field in url_fields:
                                                        if picture_obj.get(field):
                                                            photo_url = picture_obj[field]
                                                            break
                                            if photo_url:
                                                all_photos.append(photo_url)
                                except Exception as e:
                                    logger.warning(f"Could not parse photo gallery section {sid}: {e}")

                            elif sid == "TITLE_DEFAULT":
                                out["title"] = payload.get("title")
                                out["roomTypeCategory"] = payload.get("roomTypeCategory")
                                try:
                                    pic_url = payload.get("shareSave", {}).get("embedData", {}).get("pictureUrl")
                                    if pic_url: all_photos.insert(0, pic_url)
                                except: pass

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
                                card = payload.get("cardData") or {}
                                out["host"] = card.get("name")
                                out["isSuperhost"] = bool(card.get("isSuperhost"))
                                out["isVerified"] = bool(card.get("isVerified"))
                                out["ratingCount"] = card.get("ratingCount", 0)

                                for key in ("about", "description", "bio", "hostBio", "hostDescription"):
                                    val = card.get(key)
                                    if isinstance(val, str) and len(val.strip()) >= 40:
                                        out["hostAboutText"] = val.strip()
                                        break

                                user_id_b64 = card.get("userId")
                                if user_id_b64:
                                    try:
                                        out["userId"] = base64.b64decode(user_id_b64.encode("utf-8")).decode("utf-8").split(":")[-1]
                                    except:
                                        out["userId"] = str(user_id_b64)
                                if out["userId"]:
                                    out["userUrl"] = f"https://www.airbnb.com/users/show/{out['userId']}"

                                time_as_host = card.get("timeAsHost") or {}
                                out["years"] = time_as_host.get("years", 0)
                                out["months"] = time_as_host.get("months", 0)
                                out["hostrAtingAverage"] = card.get("ratingAverage", 0.0)
                        except Exception:
                            continue

                    # Success! Process photos and return
                    unique_photos = list(dict.fromkeys([u for u in all_photos if u]))
                    out["allPictures"] = unique_photos
                    if unique_photos:
                        out["picture"] = unique_photos[0]

                    logger.info(f"Final photo count for {_id}: {len(unique_photos)} unique photos")
                    return out
            else:
                logger.error(f"[PDP] GraphQL errors")
        else:
            logger.error(f"[PDP] HTTP {resp.status}")

    except Exception as e:
        logger.error(f"[PDP] Request failed: {e}")

    # ---- 2. Fallback: Scrape details + images from DOM ----
    logger.info(f"[PDP] Falling back to DOM scraping for {_id}")
    dom_details = _scrape_details_from_dom(context, listing_info.get("link", ""), logger)
    
    if dom_details:
        # Merge details
        for k, v in dom_details.items():
            if v not in (None, "", []):
                out[k] = v
        
        # Merge images extracted from JSON
        if dom_details.get("allPictures"):
            out["allPictures"].extend(dom_details["allPictures"])
            out["allPictures"] = list(dict.fromkeys(out["allPictures"])) # Dedup immediately

    # ---- 3. Last Resort: Visual Scrolling ----
    # ONLY run if we found NO images in the JSON to save time
    if not out["allPictures"]:
        logger.info("[PDP DOM] No images found in JSON, running slow visual scraper...")
        photos = _scrape_images_from_dom(context, listing_info.get("link", ""), logger)
        if photos:
            out["allPictures"].extend(photos)

    # ---- 4. Final Cleanup & Dedup ----
    clean_photos = []
    seen_urls = set()
    combined = out.get("allPictures") or []
    
    for p in combined:
        if not p: continue
        p_clean = p.split("?")[0] 
        lower_p = p_clean.lower()
        
        # Filter junk
        if "/pictures/user/" in lower_p: continue
        if "airbnb-platform-assets" in lower_p: continue
        if "static/packages" in lower_p: continue
        
        if p_clean not in seen_urls:
            clean_photos.append(p_clean)
            seen_urls.add(p_clean)

    out["allPictures"] = clean_photos
    if clean_photos:
        out["picture"] = clean_photos[0]

    logger.info(f"Fallback photo count for {_id}: {len(clean_photos)}")
    return out

    
