import json
import os.path
import sqlite3
import base64
import Config
import logging
import re
import urllib.parse
from datetime import datetime
from playwright.sync_api import (
    Page, BrowserContext, Request, APIResponse, TimeoutError
)
from undetected_playwright import Tarnished

import SQL
import Utils
from HumanMouseMovement import HumanMouseMovement
import time

logging.getLogger().setLevel(logging.DEBUG)


def _normalize_listing_id(raw_id, item=None):
    """
    Improved ID normalization with better handling of large numbers
    """
    if raw_id is None:
        if item and isinstance(item, dict):
            for k in ("listingId", "id", "roomId"):
                v = item.get(k)
                if v:
                    return _normalize_listing_id(v)
        return None

    # Handle integers and large numbers carefully
    if isinstance(raw_id, int):
        return str(raw_id)

    if isinstance(raw_id, str):
        s = raw_id.strip()
        
        # Check if it's already a clean numeric string
        if s.isdigit():
            return s
            
        # Handle very large numbers that might have been converted to scientific notation
        try:
            if 'e+' in s.lower():
                return str(int(float(s)))
        except ValueError:
            pass

        # Extract from prefixed formats
        for prefix in ("StayListing:", "DemandStayListing:", "StayListingProduct:", "listing:", "rooms/"):
            if prefix in s:
                tail = s.split(prefix)[-1]
                # Extract only the numeric part
                digits = re.findall(r"\d+", tail)
                if digits:
                    return digits[0]

        # URL extraction
        if "/" in s and "rooms" in s:
            parts = [p for p in s.split("/") if p]
            for p in reversed(parts):  # Check from end first
                if p.isdigit():
                    return p

        # Base64 decoding - but be more careful
        if len(s) > 10 and not s.isdigit():  # Only try base64 on longer strings
            try:
                # Add padding if missing
                missing = len(s) % 4
                if missing:
                    s += "=" * (4 - missing)
                decoded = base64.b64decode(s).decode("utf-8", errors="ignore")
                
                # Extract numeric part from decoded string
                for prefix in ("StayListing:", "DemandStayListing:", "StayListingProduct:"):
                    if prefix in decoded:
                        numeric_part = decoded.split(prefix)[-1].split(",")[0].strip()
                        if numeric_part.isdigit():
                            return numeric_part
                            
                # If decoded is just digits
                if decoded.strip().isdigit():
                    return decoded.strip()
            except Exception:
                pass

    return None

def execute_max_tries(function, logger: logging.Logger):
    tries = 0
    exp = None
    while tries < Config.CONFIG_MAX_RETRIES:
        try:
            return function()
        except Exception as e:
            tries += 1
            logger.info(f'({tries}/{Config.CONFIG_MAX_RETRIES}) Error happened while executing function {function.__name__}: {e}')
            exp = e
    if tries == Config.CONFIG_MAX_RETRIES:
        logger.error(f'Maximum tries reached while executing {function.__name__} \n exception: {exp}')
        raise Exception(f'Maximum tries reached while executing {function.__name__} \n exception: {exp}')


def login_user(context: BrowserContext, page: Page, logger: logging.Logger):
    logger.info('Visiting the main page...')
    page.goto("https://www.airbnb.com/morocco/stays")
    page.wait_for_load_state('networkidle')
    page.wait_for_timeout(2000)
    search_form = page.get_by_role(role='search')
    search_button = search_form.get_by_role('button')
    with context.expect_page() as new_page_info:
        search_button.click()
    new_page = new_page_info.value
    new_page.wait_for_load_state('domcontentloaded')
    new_page.wait_for_load_state('networkidle')
    height = page.evaluate('window.innerHeight')

    human_mouse = HumanMouseMovement(new_page)
    human_mouse.move_to(int(height/2), int(height/2))

    menu_btn = new_page.get_by_test_id('cypress-headernav-profile')
    if menu_btn is not None:
        logger.info('Navigation menu found')
    menu_btn.click()
    logger.info("looking for the login button")
    new_page.get_by_test_id('cypress-headernav-profile').locator('..').get_by_text('Log in').click()
    page.wait_for_load_state('networkidle')
    logger.info('Continue with email')
    new_page.get_by_text('Continue with email').click()
    page.wait_for_load_state('networkidle')
    logger.info('Filling login email')
    new_page.get_by_placeholder('Email').press_sequentially(Config.CONFIG_USERNAME, delay=100)
    new_page.get_by_test_id('signup-login-submit-btn').click()
    page.wait_for_load_state('networkidle')
    logger.info('Filling password')
    new_page.get_by_placeholder('Password').press_sequentially(Config.CONFIG_PASSWORD, delay=100)
    logger.info('Clicking the loging button')
    new_page.get_by_test_id('signup-login-submit-btn').click()
    page.wait_for_load_state('networkidle')
    page.close()
    return new_page


def _dismiss_any_popups_enhanced(page: Page, logger: logging.Logger | None = None, max_attempts=3):
    """
    Enhanced popup dismissal that handles translation dialogs and other Airbnb modals
    """
    attempts = 0
    dismissed_something = False

    while attempts < max_attempts:
        attempts += 1
        current_dismissed = False

        if logger:
            logger.info(f"[popup] Dismissal attempt {attempts}/{max_attempts}")

        # Translation-specific selectors (HIGHEST PRIORITY)
        translation_selectors = [
            # Direct translation modal close button
            'div[role="dialog"]:has-text("Translation on") button[aria-label="Close"]',
            'div[role="dialog"]:has-text("Translation") button[aria-label="Close"]',
            'div[role="dialog"]:has-text("translation") button[aria-label="Close"]',

            # Translation modal with specific text
            'div:has-text("Translation on") button[aria-label="Close"]',
            'div:has-text("This symbol shows when content") button[aria-label="Close"]',
            'div:has-text("automatically translated") button[aria-label="Close"]',

            # Translation settings and buttons
            'button:has-text("Got it")',
            'button:has-text("No thanks")',
            'button:has-text("Not now")',
            'button:has-text("Continue in English")',
            'button:has-text("Keep using English")',
            'button:has-text("Dismiss")',

            # Translation banner elements
            '[data-testid="translation-banner-dismiss"]',
            '[data-testid="language-detector-decline"]',
            '[data-testid="language-banner-dismiss"]',
            'div[data-testid="translation-bar"] button',

            # Generic close buttons in translation context
            '[aria-label="Close translation dialog"]',
            '[aria-label="Close translation modal"]',
        ]

        # Try translation selectors first
        for sel in translation_selectors:
            try:
                elements = page.locator(sel)
                count = elements.count()
                if logger and count > 0:
                    logger.info(f"[popup] Found {count} translation elements with selector: {sel}")

                for i in range(count):
                    try:
                        element = elements.nth(i)
                        if element.is_visible(timeout=500):
                            if logger:
                                logger.info(f"[popup] Clicking translation element {i+1}: {sel}")
                            element.click(timeout=3000, force=True)
                            page.wait_for_timeout(800)
                            current_dismissed = True
                            dismissed_something = True
                    except Exception as e:
                        if logger:
                            logger.info(f"[popup] Failed to click translation element {i+1}: {e}")
                        continue

                if current_dismissed:
                    break

            except Exception as e:
                if logger:
                    logger.info(f"[popup] Translation selector failed: {sel} - {e}")
                continue

        # General modal close selectors (lower priority)
        if not current_dismissed:
            general_selectors = [
                # Generic dialog close buttons
                'div[role="dialog"] button[aria-label="Close"]',
                'div[role="dialog"] [data-testid="modal-sheet-close-button"]',
                'div[role="dialog"] button:has-text("Close")',
                '[data-testid="modal-container"] button[aria-label="Close"]',
                'button[aria-label="Close dialog"]',
                'button[aria-label="Dismiss"]',

                # Cookie banners
                'button:has-text("Accept")',
                'button:has-text("Accept all cookies")',
                '[data-testid="accept-btn"]',

                # Other common dismissal buttons
                'button:has-text("OK")',
                'button:has-text("Continue")',
                'button:has-text("Skip")',
            ]

            for sel in general_selectors:
                try:
                    elements = page.locator(sel)
                    count = elements.count()

                    for i in range(count):
                        try:
                            element = elements.nth(i)
                            if element.is_visible(timeout=500):
                                if logger:
                                    logger.info(f"[popup] Clicking general element {i+1}: {sel}")
                                element.click(timeout=3000, force=True)
                                page.wait_for_timeout(500)
                                current_dismissed = True
                                dismissed_something = True
                        except Exception:
                            continue

                    if current_dismissed:
                        break

                except Exception:
                    continue

        # Enhanced ESC key handling for stubborn modals
        if not current_dismissed:
            try:
                dialogs = page.locator('div[role="dialog"]:visible')
                if dialogs.count() > 0:
                    if logger:
                        logger.info(f"[popup] Found {dialogs.count()} visible dialogs, pressing Escape")

                    for _ in range(3):
                        page.keyboard.press("Escape")
                        page.wait_for_timeout(200)

                    current_dismissed = True
                    dismissed_something = True
            except Exception as e:
                if logger:
                    logger.info(f"[popup] ESC key handling failed: {e}")

        # Enhanced click-away with multiple targets
        try:
            targets = [
                "div.gm-style",   # Map (preferred)
                '[data-testid="map"]',
                "main",
                "body"
            ]

            clicked_away = False
            for target_sel in targets:
                try:
                    target = page.locator(target_sel).first
                    if target.count() > 0:
                        box = target.bounding_box()
                        if box and box["width"] > 0 and box["height"] > 0:
                            positions = [
                                (int(box["x"] + box["width"] * 0.3), int(box["y"] + box["height"] * 0.3)),
                                (int(box["x"] + box["width"] * 0.7), int(box["y"] + box["height"] * 0.7)),
                                (int(box["x"] + box["width"] * 0.5), int(box["y"] + box["height"] * 0.5)),
                            ]

                            for cx, cy in positions:
                                try:
                                    if logger:
                                        logger.info(f"[popup] Click-away on {target_sel} at ({cx}, {cy})")
                                    page.mouse.click(cx, cy)
                                    page.wait_for_timeout(200)
                                    clicked_away = True
                                    break
                                except Exception:
                                    continue

                            if clicked_away:
                                break
                except Exception:
                    continue

            # Viewport center fallback with multiple clicks
            if not clicked_away:
                try:
                    vp = page.viewport_size or {"width": 1400, "height": 900}
                    positions = [
                        (int(vp["width"] * 0.5), int(vp["height"] * 0.3)),
                        (int(vp["width"] * 0.5), int(vp["height"] * 0.7)),
                        (int(vp["width"] * 0.3), int(vp["height"] * 0.5)),
                    ]

                    for cx, cy in positions:
                        if logger:
                            logger.info(f"[popup] Click-away on viewport at ({cx}, {cy})")
                        page.mouse.click(cx, cy)
                        page.wait_for_timeout(200)
                except Exception:
                    pass

        except Exception as e:
            if logger:
                logger.info(f"[popup] Click-away failed: {e}")

        # Wait for overlays to disappear with timeout
        try:
            overlay_selectors = [
                'div[role="dialog"]:visible',
                '[data-testid="modal-container"]:visible',
                '[data-testid="translation-banner"]:visible',
                '.translation-bar:visible'
            ]

            for overlay_sel in overlay_selectors:
                try:
                    overlays = page.locator(overlay_sel)
                    if overlays.count() > 0:
                        if logger:
                            logger.info(f"[popup] Waiting for {overlays.count()} overlays to disappear: {overlay_sel}")
                        overlays.first.wait_for(state="hidden", timeout=2000)
                except Exception:
                    pass
        except Exception:
            pass

        # Check if we need to continue (if any dialogs are still visible)
        try:
            still_has_dialog = page.locator('div[role="dialog"]:visible').count() > 0
            if logger:
                logger.info(f"[popup] Still has dialogs: {still_has_dialog}")
            if not still_has_dialog:
                break
        except Exception:
            break

        # Additional wait between attempts
        if attempts < max_attempts:
            page.wait_for_timeout(500)

    if logger and dismissed_something:
        logger.info(f"[popup] Completed dismissal after {attempts} attempts")
    elif logger:
        logger.info(f"[popup] No popups found to dismiss after {attempts} attempts")

    return dismissed_something


def _wait_for_stable_page(page: Page, logger: logging.Logger | None = None, timeout=15000):
    """
    Wait for the page to be stable (no popups, overlays loaded)
    """
    start_time = time.time()

    if logger:
        logger.info("[stability] Waiting for page stability...")

    while (time.time() - start_time) * 1000 < timeout:
        _dismiss_any_popups_enhanced(page, logger, max_attempts=2)

        try:
            dialogs = page.locator('div[role="dialog"]:visible')
            has_dialogs = dialogs.count() > 0

            loaders = page.locator('[data-testid="loading"], .loading, [aria-label*="loading" i]')
            has_loaders = loaders.count() > 0

            translation_elements = page.locator('div:has-text("Translation on"):visible, div:has-text("automatically translated"):visible')
            has_translation = translation_elements.count() > 0

            if logger:
                logger.info(f"[stability] Dialogs: {has_dialogs}, Loaders: {has_loaders}, Translation: {has_translation}")

            if not has_dialogs and not has_loaders and not has_translation:
                page.wait_for_timeout(1000)

                final_dialogs = page.locator('div[role="dialog"]:visible').count() > 0
                final_translation = page.locator('div:has-text("Translation on"):visible').count() > 0

                if not final_dialogs and not final_translation:
                    if logger:
                        logger.info("[stability] Page appears stable")
                    return True
                elif logger:
                    logger.info(f"[stability] Still not stable - dialogs: {final_dialogs}, translation: {final_translation}")
        except Exception as e:
            if logger:
                logger.info(f"[stability] Stability check error: {e}")

        page.wait_for_timeout(500)

    if logger:
        logger.warning(f"[stability] Page stability timeout after {timeout}ms")
    return False


def move_map_randomly(page: Page, logger: logging.Logger):
    if logger:
        logger.info("Enhanced map adjustment to capture search_token")

    _dismiss_any_popups_enhanced(page, logger, max_attempts=5)
    _wait_for_stable_page(page, logger, timeout=5000)

    success = False

    # Method 1: Zoom button
    try:
        _dismiss_any_popups_enhanced(page, logger, max_attempts=2)
        gmap = page.get_by_test_id('map/ZoomInButton')
        if gmap.count() > 0:
            gmap.click(timeout=5000, force=True)
            page.wait_for_timeout(1000)
            success = True
            if logger:
                logger.info("[map] Zoom button clicked successfully")
    except Exception as e:
        if logger:
            logger.info(f"[map] Zoom button failed: {e}")

    # Method 2: Map drag
    if not success:
        try:
            _dismiss_any_popups_enhanced(page, logger, max_attempts=2)
            canvas = page.locator("div.gm-style").first
            if canvas.count() > 0:
                box = canvas.bounding_box()
                if box:
                    cx = int(box["x"] + box["width"] / 2)
                    cy = int(box["y"] + box["height"] / 2)

                    if logger:
                        logger.info(f"[map] Dragging map from ({cx}, {cy})")

                    page.mouse.move(cx, cy)
                    page.mouse.down()
                    page.mouse.move(cx + 80, cy + 40, steps=10)
                    page.mouse.up()
                    page.wait_for_timeout(1000)
                    success = True
                    if logger:
                        logger.info("[map] Map drag completed")
        except Exception as e:
            if logger:
                logger.info(f"[map] Map drag failed: {e}")

    # Method 3: Mouse wheel
    if not success:
        try:
            _dismiss_any_popups_enhanced(page, logger, max_attempts=1)
            canvas = page.locator("div.gm-style").first
            if canvas.count() > 0:
                box = canvas.bounding_box()
                if box:
                    cx = int(box["x"] + box["width"] / 2)
                    cy = int(box["y"] + box["height"] / 2)
                    page.mouse.move(cx, cy)
                    page.mouse.wheel(0, -300)
                    page.wait_for_timeout(1000)
                    success = True
                    if logger:
                        logger.info("[map] Mouse wheel completed")
        except Exception as e:
            if logger:
                logger.info(f"[map] Mouse wheel failed: {e}")

    _dismiss_any_popups_enhanced(page, logger, max_attempts=2)
    return success


def wait_for_network_idle(page: Page, timeout=30000, max_concurrent_requests=0, min_idle_time=500):
    start_time = time.time()
    idle_start_time = None

    def get_active_requests():
        # Best-effort; Playwright Python doesn't expose page.request queue directly.
        # Keep as-is for compatibility with existing callers.
        try:
            return len(page.request.all)  # type: ignore[attr-defined]
        except Exception:
            return 0

    while True:
        active_requests = get_active_requests()

        if active_requests <= max_concurrent_requests:
            if idle_start_time is None:
                idle_start_time = time.time()
            elif (time.time() - idle_start_time) * 1000 >= min_idle_time:
                return True
        else:
            idle_start_time = None

        if (time.time() - start_time) * 1000 >= timeout:
            raise TimeoutError("Network idle timeout")

        time.sleep(0.1)


def scrape_page_result(
    context: BrowserContext, search_token: str, operation: str, local: str, currency: str,
    boundary: tuple[float, float, float, float],
    monthly_end_date: list, monthly_start_date: list, skip_hydration: list,
    place_id: list, map_width_px: int, map_height_px: int,
    api_key: str, client_version: str, request_id: str,
    logger: logging.Logger, page_token: str = None,
    base_headers: dict | None = None,
):
    if not search_token:
        raise RuntimeError("scrape_page_result called with empty search_token")

    # -------- payload --------
    zoom_level = Utils.get_zoom_level(
        boundary[0], boundary[1], boundary[2], boundary[3], map_width_px, map_height_px
    )
    url = f"https://www.airbnb.com/api/v3/StaysSearch/{search_token}"

    querystring = {
        "operationName": operation,
        "locale": local,
        "currency": currency,
    }

    payload = {
        "operationName": operation,
        "variables": {
            "aiSearchEnabled": False,
            "staysSearchRequest": {
                "maxMapItems": 9999,
                "requestedPageType": "STAYS_SEARCH",
                "metadataOnly": False,
                "treatmentFlags": [
                    "feed_map_decouple_m11_treatment", "recommended_filters_2024_treatment_b",
                    "m1_2024_monthly_stays_dial_treatment_flag", "recommended_amenities_2024_treatment_b",
                    "filter_redesign_2024_treatment", "filter_reordering_2024_roomtype_treatment",
                    "selected_filters_2024_treatment", "m13_search_input_phase2_treatment"
                ],
                "searchType": "user_map_move",
                "rawParams": [
                    {"filterName": "adults", "filterValues": ["1"]},
                    {"filterName": "cdnCacheSafe", "filterValues": ["false"]},
                    {"filterName": "channel", "filterValues": ["EXPLORE"]},
                    {"filterName": "flexibleTripLengths", "filterValues": ["one_week"]},
                    {"filterName": "itemsPerGrid", "filterValues": ["18"]},
                    {"filterName": "monthlyEndDate", "filterValues": monthly_end_date},
                    {"filterName": "monthlyLength", "filterValues": ["3"]},
                    {"filterName": "monthlyStartDate", "filterValues": monthly_start_date},
                    {"filterName": "neLat", "filterValues": [str(boundary[2])]},
                    {"filterName": "neLng", "filterValues": [str(boundary[3])]},
                    {"filterName": "placeId", "filterValues": place_id},
                    {"filterName": "priceFilterInputType", "filterValues": ["0"]},
                    {"filterName": "priceFilterNumNights", "filterValues": ["5"]},
                    {"filterName": "query", "filterValues": ["Morocco"]},
                    {"filterName": "refinementPaths", "filterValues": ["/homes"]},
                    {"filterName": "screenSize", "filterValues": ["large"]},
                    {"filterName": "searchByMap", "filterValues": ["true"]},
                    {"filterName": "searchMode", "filterValues": ["regular_search"]},
                    {"filterName": "swLat", "filterValues": [str(boundary[0])]},
                    {"filterName": "swLng", "filterValues": [str(boundary[1])]},
                    {"filterName": "tabId", "filterValues": ["home_tab"]},
                    {"filterName": "version", "filterValues": ["1.8.3"]},
                    {"filterName": "zoomLevel", "filterValues": [str(zoom_level)]},
                ],
                "skipHydrationListingIds": skip_hydration
            },
            "staysMapSearchRequestV2": {
                "requestedPageType": "STAYS_SEARCH",
                "metadataOnly": False,
                "treatmentFlags": [
                    "feed_map_decouple_m11_treatment", "recommended_filters_2024_treatment_b",
                    "m1_2024_monthly_stays_dial_treatment_flag", "recommended_amenities_2024_treatment_b",
                    "filter_redesign_2024_treatment", "filter_reordering_2024_roomtype_treatment",
                    "selected_filters_2024_treatment", "m13_search_input_phase2_treatment"
                ],
                "searchType": "user_map_move",
                "rawParams": [
                    {"filterName": "adults", "filterValues": ["1"]},
                    {"filterName": "cdnCacheSafe", "filterValues": ["false"]},
                    {"filterName": "channel", "filterValues": ["EXPLORE"]},
                    {"filterName": "flexibleTripLengths", "filterValues": ["one_week"]},
                    {"filterName": "monthlyEndDate", "filterValues": monthly_end_date},
                    {"filterName": "monthlyLength", "filterValues": ["3"]},
                    {"filterName": "monthlyStartDate", "filterValues": monthly_start_date},
                    {"filterName": "neLat", "filterValues": [str(boundary[2])]},
                    {"filterName": "neLng", "filterValues": [str(boundary[3])]},
                    {"filterName": "placeId", "filterValues": place_id},
                    {"filterName": "priceFilterInputType", "filterValues": ["0"]},
                    {"filterName": "priceFilterNumNights", "filterValues": ["5"]},
                    {"filterName": "query", "filterValues": ["Morocco"]},
                    {"filterName": "refinementPaths", "filterValues": ["/homes"]},
                    {"filterName": "screenSize", "filterValues": ["large"]},
                    {"filterName": "searchByMap", "filterValues": ["true"]},
                    {"filterName": "searchMode", "filterValues": ["regular_search"]},
                    {"filterName": "swLat", "filterValues": [str(boundary[0])]},
                    {"filterName": "swLng", "filterValues": [str(boundary[1])]},
                    {"filterName": "tabId", "filterValues": ["home_tab"]},
                    {"filterName": "version", "filterValues": ["1.8.3"]},
                    {"filterName": "zoomLevel", "filterValues": [str(zoom_level)]},
                ],
                "skipHydrationListingIds": skip_hydration
            },
            "isLeanTreatment": False,
            "skipExtendedSearchParams": False,
            "includeLegacyListingCardFieldsForSxS": False,
            "includeDemandStayListing": True,
            "includeDemandStayListingFieldsErf1": False,
            "includeDemandStayListingFieldsErf2": True,
            "includeDemandStayListingFieldsErf3": False,
            "includeDemandStayListingFieldsErf4": False,
            "includeDemandStayListingFieldsErf5": False,
            "skipLegacyListingCardFieldsErf2": True,
            "skipLegacyListingCardFieldsErf3": False,
            "skipLegacyListingCardFieldsErf4": False
        },
        "extensions": {"persistedQuery": {"version": 1, "sha256Hash": search_token}}
    }
    if page_token is not None:
        payload['variables']['staysSearchRequest']['cursor'] = page_token
        payload['variables']['staysMapSearchRequestV2']['cursor'] = page_token

    # -------- request --------
    if base_headers:
        ignore = {':authority', ':method', ':path', ':scheme', 'content-length'}
        headers = {k: v for k, v in base_headers.items()
                   if k.lower() not in ignore and not k.startswith(':')}
        headers.update({
            "content-type": "application/json",
            "origin": "https://www.airbnb.com",
            "x-airbnb-supports-airlock-v2": "true",
            "x-airbnb-graphql-platform": "web",
            "x-airbnb-graphql-platform-client": "minimalist-niobe",
            "x-niobe-short-circuited": "true",
            "x-csrf-without-token": "1",
        })
        if api_key:
            headers["x-airbnb-api-key"] = api_key
        if client_version:
            headers["x-client-version"] = client_version
        if request_id:
            headers["x-client-request-id"] = request_id
    else:
        headers = {
            "x-airbnb-supports-airlock-v2": "true",
            "x-airbnb-api-key": api_key,
            "x-csrf-without-token": "1",
            "x-airbnb-graphql-platform": "web",
            "x-airbnb-graphql-platform-client": "minimalist-niobe",
            "x-niobe-short-circuited": "true",
            "x-client-version": client_version,
            "x-client-request-id": request_id,
            "origin": "https://www.airbnb.com",
            "connection": "keep-alive",
            "priority": "u=4",
            "content-type": "application/json",
        }

    if headers.get("x-airbnb-api-key"):
        k = headers["x-airbnb-api-key"]
        logger.info(f"[StaysSearch] Using API key: {k[:6]}…{k[-4:]}")

    # Debug: Log boundary info
    logger.info(f"[StaysSearch] Boundary: SW({boundary[0]}, {boundary[1]}) NE({boundary[2]}, {boundary[3]}) Zoom: {zoom_level}")

    try:
        response: APIResponse = context.request.post(
            url=url,
            headers=headers,
            params=querystring,
            data=json.dumps(payload),
            timeout=30000,
        )
    except Exception as e:
        logger.error(f"[StaysSearch] Request failed: {e}")
        raise RuntimeError(f"StaysSearch request failed: {e}")

    txt = response.text()
    if response.status != 200:
        logger.error(f"[StaysSearch] HTTP {response.status} {response.status_text}\n{txt}")
        raise RuntimeError(f"StaysSearch HTTP {response.status}")

    try:
        json_data = json.loads(txt)
    except Exception as e:
        logger.error(f"[StaysSearch] JSON parse error: {e}\nRaw: {txt[:600]}")
        raise

    # Debug: Save raw response for inspection
    if logger.level <= logging.DEBUG:
        try:
            with open(f"debug_response_{int(time.time())}.json", "w") as f:
                json.dump(json_data, f, indent=2)
        except Exception:
            pass

    if json_data.get("errors"):
        logger.error(f"[StaysSearch] GraphQL errors: {json.dumps(json_data['errors'], indent=2)[:1000]}")

    # Enhanced data extraction with debugging
    data_root = (json_data.get('data') or {}).get('presentation', {}).get('staysSearch', {})
    if not data_root:
        logger.warning(f"[StaysSearch] Primary path failed. Exploring response structure...")

        def log_structure(obj, path="", max_depth=3, current_depth=0):
            if current_depth > max_depth:
                return
            if isinstance(obj, dict):
                for k, v in obj.items():
                    new_path = f"{path}.{k}" if path else k
                    if isinstance(v, (dict, list)):
                        logger.debug(f"[StaysSearch] Structure: {new_path} -> {type(v).__name__}")
                        log_structure(v, new_path, max_depth, current_depth + 1)
                    else:
                        logger.debug(f"[StaysSearch] Structure: {new_path} -> {type(v).__name__}: {str(v)[:50]}")
            elif isinstance(obj, list) and obj:
                new_path = f"{path}[0]"
                logger.debug(f"[StaysSearch] Structure: {new_path} -> List with {len(obj)} items")
                if obj:
                    log_structure(obj[0], new_path, max_depth, current_depth + 1)

        log_structure(json_data)

        alternative_paths = [
            ('data', 'staysSearch'),
            ('data', 'presentation', 'explore'),
            ('data', 'presentation', 'search'),
            ('data', 'explore', 'sections', 'sectionedResults'),
        ]

        for path in alternative_paths:
            temp_root = json_data
            try:
                for key in path:
                    temp_root = temp_root.get(key, {})
                if temp_root and isinstance(temp_root, dict):
                    logger.info(f"[StaysSearch] Found alternative data path: {' -> '.join(path)}")
                    data_root = temp_root
                    break
            except Exception:
                continue

        if not data_root:
            logger.error(f"[StaysSearch] No valid data found. Response sample: {str(json_data)[:500]}...")
            return {
                "searchResults": [],
                "nextPageCursor": None,
                "totalPages": 0,
                "federatedSearchId": None,
                "federatedSearchSessionId": None,
            }

    results_obj = data_root.get('results') or data_root

    # Enhanced helper functions
    def _deep_iter(obj, path="", visited=None):
        if visited is None:
            visited = set()

        if id(obj) in visited:  # Prevent infinite recursion
            return
        visited.add(id(obj))

        if isinstance(obj, dict):
            yield obj, path
            for k, v in obj.items():
                yield from _deep_iter(v, f"{path}.{k}" if path else k, visited)
        elif isinstance(obj, list):
            yield obj, path
            for i, v in enumerate(obj):
                yield from _deep_iter(v, f"{path}[{i}]", visited)

        visited.remove(id(obj))

    def _collect_search_results(obj):
        candidates = []

        # Primary search paths
        search_paths = [
            'searchResults',
            ('staysSearchResults', 'searchResults'),
            ('staysSearchResultsV2', 'searchResults'),
            ('staysMapSearchResults', 'mapResults'),
            ('staysMapSearchResultsV2', 'mapResults'),
            'sectionedResults',
            'results',
            'mapResults',
            'listings',
            # common alt arrays
            'items',
            'exploreItems',
        ]

        for k in search_paths:
            if isinstance(k, tuple):
                a, b = k
                inner = obj.get(a) if isinstance(obj, dict) else None
                if isinstance(inner, dict) and b in inner and isinstance(inner[b], list):
                    candidates.extend(inner[b])
                    logger.info(f"[StaysSearch] Found results via path: {a} -> {b} ({len(inner[b])} items)")
            else:
                if isinstance(obj, dict) and k in obj and isinstance(obj[k], list):
                    candidates.extend(obj[k])
                    logger.info(f"[StaysSearch] Found results via path: {k} ({len(obj[k])} items)")

        # Deep scan
        if not candidates:
            logger.info("[StaysSearch] No results found via standard paths, doing enhanced deep scan...")

            for node, path in _deep_iter(obj):
                if isinstance(node, dict):
                    # typ: {"listing": {...}}
                    if 'listing' in node and isinstance(node['listing'], dict):
                        listing = node['listing']
                        # accept global id formats
                        if listing.get('id') or listing.get('listingId'):
                            candidates.append(node)
                            logger.debug(f"[StaysSearch] Found listing via deep scan at: {path}")

                    # nodes that directly look like listings
                    elif ('id' in node or 'listingId' in node) and ('title' in node or 'name' in node or 'structuredDisplayPrice' in node):
                        candidates.append({'listing': node})
                        logger.debug(f"[StaysSearch] Found direct listing at: {path}")

                elif isinstance(node, list):
                    for item in node[:3]:
                        if isinstance(item, dict):
                            if ('listing' in item and isinstance(item['listing'], dict)) or \
                               (('id' in item or 'listingId' in item) and ('title' in item or 'name' in item)):
                                candidates.extend(node)
                                logger.info(f"[StaysSearch] Found listing array at: {path} ({len(node)} items)")
                                break

        logger.info(f"[StaysSearch] Total candidates found: {len(candidates)}")

        # Debug: first few candidates
        for i, candidate in enumerate(candidates[:2]):
            logger.debug(f"[StaysSearch] Candidate {i} structure: {list(candidate.keys()) if isinstance(candidate, dict) else type(candidate)}")
            if isinstance(candidate, dict) and 'listing' in candidate:
                listing = candidate['listing']
                if isinstance(listing, dict):
                    logger.debug(f"[StaysSearch] Candidate {i} listing keys: {list(listing.keys())}")

        return candidates

    def _extract_pagination(obj):
        # Try known keys
        if isinstance(obj, dict):
            if 'paginationInfo' in obj:
                return obj['paginationInfo']
            if 'pageInfo' in obj:
                return obj['pageInfo']
            if 'pagination' in obj:
                return obj['pagination']
            if 'pagingInfo' in obj:
                return obj['pagingInfo']

        # Deep search for pagination-like
        for node, path in _deep_iter(obj):
            if isinstance(node, dict):
                for key in ('paginationInfo', 'pageInfo', 'pagination', 'pagingInfo'):
                    if key in node and isinstance(node[key], (dict, list)):
                        logger.debug(f"[StaysSearch] Found pagination at: {path}")
                        return node[key]

        return {}

    def _extract_legacy_logging(obj):
        try:
            return obj['loggingMetadata']['legacyLoggingContext']
        except Exception:
            pass

        for node, path in _deep_iter(obj):
            if isinstance(node, dict) and 'legacyLoggingContext' in node:
                logger.debug(f"[StaysSearch] Found logging context at: {path}")
                return node['legacyLoggingContext']

        # Alternative logging paths
        alt_logging_keys = ['federatedSearchId', 'searchSessionId', 'requestId']
        logging_data = {}
        for key in alt_logging_keys:
            for node, path in _deep_iter(obj):
                if isinstance(node, dict) and key in node:
                    logging_data[key] = node[key]
                    logger.debug(f"[StaysSearch] Found {key} at: {path}")

        return logging_data

    # Extract results with enhanced error handling
    raw_items = _collect_search_results(results_obj)

    export_results = []
    now_ts = int(datetime.now().timestamp())

    for item in raw_items:
        try:
            listing = None

            # Multiple ways to extract listing data
            if isinstance(item, dict):
                if 'listing' in item and isinstance(item['listing'], dict):
                    listing = item['listing']
                elif item.get('id') or item.get('listingId'):
                    listing = item
                else:
                    # search nested objects that look like listings
                    for key, value in item.items():
                        if isinstance(value, dict) and (value.get('id') or value.get('listingId')):
                            listing = value
                            break

            if not isinstance(listing, dict):
                logger.debug(f"[StaysSearch] Skipping item - no valid listing found: {type(item)}")
                continue

            # Extract listing ID (robust)
            raw_id = listing.get('id') or listing.get('listingId')
            _id = _normalize_listing_id(raw_id, item=listing)  # pass listing for fallbacks
            if not _id:
                logger.debug(f"[StaysSearch] Skipping listing - no usable id from {raw_id!r}")
                continue

            # Title fallbacks
            title = (
                listing.get('title')
                or listing.get('name')
                or listing.get('localizedTitle')
                or (listing.get('presentation', {}) or {}).get('title')
                or item.get('title')
                or item.get('name')
            )

            # Price extraction (robust)
            def _pick(*vals):
                for v in vals:
                    if isinstance(v, str) and v.strip():
                        return v

            price_info = {'price': None, 'discounted_price': None, 'original_price': None}

            structuredDisplayPrice = (
                item.get('structuredDisplayPrice')
                or listing.get('structuredDisplayPrice')
                or {}
            )
            if structuredDisplayPrice:
                pl = structuredDisplayPrice.get('primaryLine') or {}
                price_info['price'] = _pick(pl.get('price'), pl.get('priceString'), pl.get('displayPrice'))
                price_info['discounted_price'] = _pick(pl.get('discountedPrice'))
                price_info['original_price'] = _pick(pl.get('originalPrice'))

            if not price_info['price']:
                for node in (item, listing):
                    if not isinstance(node, dict):
                        continue
                    price_info['price'] = _pick(
                        (node.get('price', {}) or {}).get('amountFormatted'),
                        (node.get('pricingQuote', {}) or {}).get('priceString'),
                        (node.get('priceMetadata', {}) or {}).get('displayRate'),
                        node.get('displayPrice'),
                        node.get('price'),
                    )
                    if price_info['price']:
                        break

            # Picture extraction with multiple fallbacks
            picture_url = None
            picture_paths = [
                'contextualPictures', 'listingContextualPictures',
                'pictures', 'images', 'photos', 'media', 'cardPhotos'
            ]
            for path in picture_paths:
                pics = item.get(path) or listing.get(path)
                if pics and isinstance(pics, list) and len(pics) > 0:
                    first_pic = pics[0]
                    if isinstance(first_pic, dict):
                        picture_url = (
                            first_pic.get('picture') or
                            first_pic.get('url') or
                            first_pic.get('src') or
                            first_pic.get('uri')
                        )
                        if picture_url:
                            break

            if not picture_url:
                pic_fields = ['previewImage', 'mainImage', 'heroImage', 'thumbnail', 'image']
                for field in pic_fields:
                    pic_data = item.get(field) or listing.get(field)
                    if pic_data:
                        if isinstance(pic_data, dict):
                            picture_url = pic_data.get('url') or pic_data.get('src')
                        elif isinstance(pic_data, str):
                            picture_url = pic_data
                        if picture_url:
                            break

            # Other listing parameters
            listingParamOverrides = item.get('listingParamOverrides', {}) or {}
            link = f"https://www.airbnb.com/rooms/{_id}"

            export_results.append({
                "id": _id,
                "ListingObjType": listing.get('listingObjType', 'REGULAR'),
                "roomTypeCategory": listing.get('roomTypeCategory', 'unavailable'),
                "title": title,
                "name": title,
                "picture": picture_url,
                "checkin": listingParamOverrides.get('checkin'),
                "checkout": listingParamOverrides.get('checkout'),
                "price": price_info.get('price'),
                "discounted_price": price_info.get('discounted_price'),
                "original_price": price_info.get('original_price'),
                "link": link,
                "scraping_time": now_ts,
                "categoryTag": listingParamOverrides.get('categoryTag'),
                "photoId": listingParamOverrides.get('photoId'),
            })

            logger.debug(f"[StaysSearch] Successfully parsed listing: {_id} - {title or 'No title'}")

        except Exception as e:
            logger.warning(f"[StaysSearch] Error processing listing item: {e}")
            logger.debug(f"[StaysSearch] Problematic item structure: {type(item)} - {list(item.keys()) if isinstance(item, dict) else 'Not a dict'}")
            continue

    pg = _extract_pagination(results_obj) or {}
    legacy = _extract_legacy_logging(results_obj) or {}

    # nextPageCursor
    next_cursor = None
    try:
        if isinstance(pg, dict):
            for k in ("nextPageCursor", "nextCursor", "nextPageToken", "cursor", "next"):
                if k in pg and pg[k]:
                    next_cursor = pg[k]
                    break
        elif isinstance(pg, list) and pg:
            # sometimes a list of cursors
            last = pg[-1]
            if isinstance(last, dict):
                next_cursor = last.get("cursor") or last.get("nextPageCursor")
    except Exception:
        pass

    # totalPages (robust)
    pc = None
    if isinstance(pg, dict):
        pc = pg.get("pageCursors") or pg.get("cursors") or pg.get("pages")

    if isinstance(pc, list):
        total_pages = len(pc)
    elif isinstance(pc, dict):
        total_pages = int(pc.get("totalCount") or pc.get("totalPages") or 0)
    else:
        total_pages = int(pg.get("totalPages") or 0) if isinstance(pg, dict) else 0

    export = {
        "searchResults": export_results,
        "nextPageCursor": next_cursor,
        "totalPages": total_pages,
        "federatedSearchId": legacy.get("federatedSearchId"),
        "federatedSearchSessionId": legacy.get("federatedSearchSessionId"),
    }

    logger.info(f"[StaysSearch] Extracted {len(export_results)} results, {export['totalPages']} total pages")

    if export["totalPages"] and not export_results:
        logger.warning("[StaysSearch] Pages present but no items found - this indicates a parsing issue")
        # Save debug info
        try:
            debug_file = f"debug_parsing_issue_{int(time.time())}.json"
            with open(debug_file, "w") as f:
                json.dump({
                    "boundary": boundary,
                    "raw_response": json_data,
                    "candidates_found": len(raw_items),
                    "results_obj_keys": list(results_obj.keys()) if isinstance(results_obj, dict) else "Not a dict"
                }, f, indent=2)
            logger.warning(f"[StaysSearch] Debug info saved to: {debug_file}")
        except Exception:
            pass

    return export


def scrape_single_result(context: BrowserContext, item_search_token: str, listing_info: dict,
                         logger: logging.Logger, api_key: str, client_version, client_request_id,
                         federated_search_id: str, currency: str, locale: str,
                         base_headers: dict | None = None):
    _id = _normalize_listing_id(listing_info['id'])
    if not _id:
        raise ValueError(f"listing_info.id is not numeric: {listing_info['id']!r}")

    logger.info(f"=> Downloading the listing {listing_info.get('title')} | {listing_info.get('link')}")

    url = f"https://www.airbnb.com/api/v3/StaysPdpSections/{item_search_token}"

    item_id = base64.b64encode(f"StayListing:{_id}".encode('utf-8')).decode('utf-8')

    # IMPORTANT: add useContextualUser (required Boolean!)
    variables = {
        'id': item_id,
        'useContextualUser': False,
        'pdpSectionsRequest': {
            'adults': '1',
            'amenityFilters': None,
            'bypassTargetings': False,
            'categoryTag': listing_info.get('categoryTag'),
            'causeId': None,
            'children': '0',
            'disasterId': None,
            'discountedGuestFeeVersion': None,
            'displayExtensions': None,
            'federatedSearchId': federated_search_id,
            'forceBoostPriorityMessageType': None,
            'hostPreview': False,
            'infants': '0',
            'interactionType': None,
            'layouts': ['SIDEBAR', 'SINGLE_COLUMN'],
            'pets': 0,
            'pdpTypeOverride': None,
            'photoId': listing_info.get('photoId'),
            'preview': False,
            'previousStateCheckIn': None,
            'previousStateCheckOut': None,
            'priceDropSource': None,
            'privateBooking': False,
            'promotionUuid': None,
            'relaxedAmenityIds': None,
            'searchId': None,
            'selectedCancellationPolicyId': None,
            'selectedRatePlanId': None,
            'splitStays': None,
            'staysBookingMigrationEnabled': False,
            'translateUgc': False,
            'useNewSectionWrapperApi': False,
            'sectionIds': None,
            'checkIn': listing_info.get('checkin'),
            'checkOut': listing_info.get('checkout'),
            'p3ImpressionId': f'p3_{int(time.time())}_P3lbdkkYZMTFJexg'
        }
    }
    extensions = {'persistedQuery': {'version': 1, 'sha256Hash': item_search_token}}
    querystring = {
        "operationName": "StaysPdpSections",
        "locale": locale,
        "currency": currency,
        "variables": json.dumps(variables),
        "extensions": json.dumps(extensions)
    }

    # Build headers from intercepted ones (fixes invalid_key); keep referer
    if base_headers:
        ignore = {':authority', ':method', ':path', ':scheme', 'content-length'}
        headers = {k: v for k, v in base_headers.items()
                   if k.lower() not in ignore and not k.startswith(':')}
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
    else:
        headers = {
            "x-airbnb-supports-airlock-v2": "true",
            "x-airbnb-api-key": api_key,
            "x-csrf-without-token": "1",
            "x-airbnb-graphql-platform": "web",
            "x-airbnb-graphql-platform-client": "minimalist-niobe",
            "x-niobe-short-circuited": "true",
            "x-client-version": client_version,
            "x-client-request-id": client_request_id,
            "origin": "https://www.airbnb.com",
            "referer": listing_info.get("link", "https://www.airbnb.com/"),
            "accept-language": "en-US,en;q=0.9",
            "connection": "keep-alive",
            "priority": "u=4",
        }

    if headers.get("x-airbnb-api-key"):
        k = headers["x-airbnb-api-key"]
        logger.info(f"[PDP] Using API key: {k[:6]}…{k[-4:]}")

    response = context.request.get(
        url=url, headers=headers, params=querystring, timeout=30000
    )

    txt = response.text()
    if response.status != 200:
        logger.error(f"[PDP] HTTP {response.status} {response.status_text}\n{txt[:600]}")
        return {'skip': True}

    json_data = json.loads(txt)

    # If GraphQL reports any error, skip gracefully (avoid NoneType crashes)
    if json_data.get("errors"):
        try:
            logger.error(f"[PDP] GraphQL errors: {json.dumps(json_data['errors'], indent=2)[:1000]}")
        except Exception:
            logger.error("[PDP] GraphQL errors present")
        return {'skip': True}

    # Safe navigation to the sections payload
    data_root = (((json_data.get('data') or {}).get('presentation') or {})
                 .get('stayProductDetailPage') or {})
    if not data_root:
        logger.info("[PDP] No data payload present; skipping.")
        return {'skip': True}

    main_sections = data_root.get('sections') or {}
    # sbui Data (optional)
    try:
        sbuiData = (((main_sections.get('sbuiData') or {})
                     .get('sectionConfiguration') or {})
                    .get('root') or {}).get('sections') or []
    except Exception:
        sbuiData = []

    export = {
        'airbnbLuxe': False,
        'location': None,
        'maxGuestCapacity': 0,
        'isGuestFavorite': False,
        'host': None,
        'isSuperhost': False,
        'isVerified': False,
        'ratingCount': 0,
        'userId': None,
        'hostrAtingAverage': 0.0,
        'reviewsCount': 0,
        'averageRating': 0.0,
        'years': 0,
        'months': 0,
        'lat': None,
        'lng': None
    }

    for section in sbuiData:
        if section.get('sectionId') == 'LUXE_BANNER':
            export['airbnbLuxe'] = True

    # data sections can be a list or inside another dict key
    data_sections = []
    if isinstance(main_sections, dict):
        data_sections = main_sections.get('sections') or []
    elif isinstance(main_sections, list):
        data_sections = main_sections

    if not isinstance(data_sections, list):
        logger.info("[PDP] Unexpected sections structure; skipping.")
        return {'skip': True}

    for section in data_sections:
        try:
            sectionId = section.get('sectionId')
            section_data = section.get('section', {}) or {}
            if sectionId == 'AVAILABILITY_CALENDAR_DEFAULT':
                export['location'] = section_data.get('localizedLocation') or None
                export["maxGuestCapacity"] = section_data.get('maxGuestCapacity', 0)
            elif sectionId == 'REVIEWS_DEFAULT':
                export['isGuestFavorite'] = bool(section_data.get('isGuestFavorite', False))
                export['reviewsCount'] = section_data.get('overallCount', export['reviewsCount'])
                export['averageRating'] = section_data.get('overallRating', export['averageRating'])
            elif sectionId == 'LOCATION_DEFAULT':
                export['lat'] = section_data.get('lat', None)
                export['lng'] = section_data.get('lng', None)
            elif sectionId == 'MEET_YOUR_HOST':
                cardData = section_data.get('cardData', {}) or {}
                export['host'] = cardData.get('name', export['host'])
                export['isSuperhost'] = cardData.get('isSuperhost', export['isSuperhost'])
                export['isVerified'] = cardData.get('isVerified', export['isVerified'])
                export['ratingCount'] = cardData.get('ratingCount', export['ratingCount'])
                userId = cardData.get('userId')
                if userId:
                    try:
                        decoded = base64.b64decode(userId.encode('utf-8')).decode('utf-8')
                        export['userId'] = decoded.split(':')[-1]
                    except Exception:
                        export['userId'] = str(userId)
                timeAsHost = cardData.get('timeAsHost', {}) or {}
                export['years'] = timeAsHost.get('years', 0)
                export['months'] = timeAsHost.get('months', 0)
                export['hostrAtingAverage'] = cardData.get('ratingAverage', export['hostrAtingAverage'])
        except Exception as e:
            logger.debug(f"[PDP] Section parse error: {e}")

    return export


def wait_for_network_idle_2(page: Page, timeout=30000, max_connections=0):
    try:
        page.wait_for_load_state('networkidle', timeout=timeout)
        return
    except TimeoutError:
        pass

    page.evaluate(
        """
        ({ maxConnections, timeoutMs }) => {
          return new Promise((resolve) => {
            let resolved = false;

            const countInflight = () => {
              const entries = performance.getEntriesByType('resource');
              let inflight = 0;
              for (const r of entries) {
                if (r.responseEnd === 0) inflight++;
              }
              return inflight;
            };

            const maybeResolve = () => {
              if (resolved) return;
              const inflight = countInflight();
              if (inflight <= maxConnections) {
                resolved = true;
                try { observer.disconnect(); } catch {}
                resolve();
              }
            };

            let observer;
            try {
              observer = new PerformanceObserver(() => { maybeResolve(); });
              observer.observe({ type: 'resource', buffered: true });
            } catch (e) {
              resolved = true;
              resolve();
              return;
            }

            maybeResolve();

            setTimeout(() => {
              if (resolved) return;
              resolved = true;
              try { observer.disconnect(); } catch {}
              resolve();
            }, timeoutMs);
          });
        }
        """,
        {"maxConnections": int(max_connections), "timeoutMs": int(timeout)},
    )