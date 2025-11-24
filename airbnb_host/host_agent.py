import re
import os
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
    # HostConfig must define CONFIG_PROXY (or None), HOST_MAX_LISTINGS_PER_RUN, HOST_DETAIL_SCRAPE_LIMIT
from . import host_utils as Utils
from . import host_SQL as SQL
from . import HostScrapingUtils
from .HumanMouseMovement import HumanMouseMovement


def _safe_profile_payload(base: dict, ab: dict) -> dict:
    """Only include about/bio if we actually scraped them."""
    out = base.copy()
    if ab.get("about_text"):
        out["about_text"] = ab["about_text"]
    if ab.get("bio_text"):
        out["bio_text"] = ab["bio_text"]
    return out


def _classify_listing(dd: dict) -> str:
    """
    Decide ListingObjType using whatever fields we already collect.
    Falls back to REGULAR if nothing matches.
    """
    # Only trust typed PDP fields for Luxe
    ptype = (dd.get("productType") or "").upper()
    pdp   = (dd.get("pdpType") or "").upper()
    tname = (dd.get("__typename") or "").upper()

    if ptype == "LUXE" or pdp == "LUXE" or "LUXE" in tname:
        return "LUXE"

    # Experiences via URL heuristics
    url = (dd.get("ListingUrl") or dd.get("link") or "").lower()
    if "/experiences/" in url or "/s/experiences" in url:
        return "EXPERIENCE"

    # Hotel types (common signals from PDP payloads)
    rtc = (dd.get("roomTypeCategory") or "").lower()  # e.g., "hotel_room"
    if rtc == "hotel_room":
        return "HOTEL_ROOM"

    # Some feeds expose boolean flags like isHotel / isBoutiqueHotel
    if str(dd.get("isHotel")).lower() in {"1", "true", "yes"}:
        return "HOTEL_ROOM"
    if str(dd.get("isBoutiqueHotel")).lower() in {"1", "true", "yes"}:
        return "BOUTIQUE_HOTEL"

    # Soft heuristics (optional): title hints
    title = (dd.get("title") or "").lower()
    if "hotel" in title:
        return "HOTEL_ROOM"

    # Default
    return "REGULAR"


def _clean_review_text(raw: str, reviewer_location: Optional[str] = None) -> str:
    if not raw:
        return ""
    t = raw.replace("\u00b7", " ").replace("·", " ").replace("\u00A0", " ").replace("\u202F", " ")
    # Remove boilerplate/translation badges
    t = re.sub(r"\bTranslated from [A-Za-z]+\b", "", t, flags=re.I)
    t = re.sub(r"\bShow original\b", "", t, flags=re.I)
    t = re.sub(r"\bAfficher l[’']original\b", "", t, flags=re.I)
    t = re.sub(r"\bVoir (?:la )?version originale\b", "", t, flags=re.I)
    # Drop “Rating X out of 5”
    t = re.sub(r"Rating\s*\d+(?:\.\d+)?\s*out\s*of\s*5", "", t, flags=re.I)
    # If body starts with the location line, drop it
    if reviewer_location:
        t = re.sub(rf"^\s*{re.escape(reviewer_location)}\s*,?\s*", "", t, flags=re.I)
    # Collapse bullets/whitespace
    t = re.sub(r"[•·]+", " ", t)
    t = re.sub(r"\s{2,}", " ", t)
    return t.strip()


def _get_attr_quick(loc: Locator, name: str, timeout: int = 300) -> Optional[str]:
    """Return element attribute fast or None; never blocks for long."""
    try:
        if loc.count():
            return loc.first.get_attribute(name, timeout=timeout)
    except Exception:
        return None
    return None


def _inner_text_quick(loc: Locator, timeout: int = 300) -> Optional[str]:
    """Return inner_text fast or None; never blocks for long."""
    try:
        if loc.count():
            return (loc.first.inner_text(timeout=timeout) or "").strip()
    except Exception:
        return None
    return None


def _wait_profile_ready(page: Page, logger: logging.Logger, timeout_ms: int = 15000) -> None:
    try:
        page.wait_for_load_state("domcontentloaded", timeout=timeout_ms)
    except Exception:
        pass
    anchors = [
        '[data-testid="user-profile"]',
        'main:has(h1), main:has(h2)',
        '[data-testid*="user-profile-header"]',
    ]
    for sel in anchors:
        try:
            if page.locator(sel).first.wait_for(state="visible", timeout=3500):
                logger.info(f"[host] Profile ready via {sel}")
                return
        except Exception:
            continue
    try:
        page.mouse.wheel(0, 1200)
        page.wait_for_timeout(400)
        page.mouse.wheel(0, -1200)
    except Exception:
        pass


def _extract_first_date(text_block: str) -> Optional[str]:
    """
    Pull a clean date/relative time from a header blob, without 'Rating ...'
    """
    if not text_block:
        return None
    # common: "★★★★★ · 1 week ago", "July 2025", "October 2024", etc.
    # try explicit 'ago' first
    m = re.search(r"\b(\d+\s+(?:day|week|month|year)s?\s+ago)\b", text_block, re.IGNORECASE)
    if m:
        return m.group(1)
    # try Month YYYY
    m = re.search(r"\b(Jan(?:uary)?|Feb(?:ruary)?|Mar(?:ch)?|Apr(?:il)?|May|Jun(?:e)?|"
                  r"Jul(?:y)?|Aug(?:ust)?|Sep(?:t(?:ember)?)?|Oct(?:ober)?|Nov(?:ember)?|"
                  r"Dec(?:ember)?)\s+\d{4}\b", text_block, re.IGNORECASE)
    if m:
        return m.group(0)
    return None


def _extract_text_wide(page_or_locator) -> str:
    """Safely get a generous amount of text from a Page or Locator."""
    try:
        if isinstance(page_or_locator, Page):
            return page_or_locator.inner_text("body")
        return page_or_locator.inner_text()
    except Exception:
        return ""


# -------------------------- Misc scraping helpers -----------------------------

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


def _click_if_exists(search_root, selectors, logger, label):
    for sel in selectors:
        try:
            el = search_root.locator(sel).first
            if el.count():
                try:
                    el.wait_for(state="visible", timeout=1200)
                except Exception:
                    continue
                logger.info(f'[host] Clicking "{label}"')
                el.click(timeout=4000, force=True)
                try:
                    (search_root.page if isinstance(search_root, Locator) else search_root).wait_for_timeout(600)
                except Exception:
                    pass
                return True
        except Exception:
            continue
    return False


def _ensure_pdp_token_via_link(context: BrowserContext, logger: logging.Logger, link: str) -> bool:
    page = context.new_page()
    try:
        logger.info(f"[pdp-capture] Opening PDP link to capture token: {link}")
        page.goto(link, wait_until="domcontentloaded", timeout=60000)
        HostScrapingUtils._dismiss_any_popups_enhanced(page, logger, max_attempts=3)
        try:
            page.wait_for_load_state("networkidle", timeout=20000)
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


def _open_all_listings_and_expand(page: Page, logger: logging.Logger) -> None:
    _click_if_exists(
        page,
        ['a:has-text("View all listings")', 'button:has-text("View all listings")', r'text=/View all \d+ listings/i'],
        logger,
        "View all listings",
    )
    try:
        page.wait_for_selector('a[href*="/rooms/"]', timeout=10000)
    except Exception:
        pass
    max_clicks, stagnant_after_clicks = 12, 0

    def anchor_count() -> int:
        try:
            return page.locator('a[href*="/rooms/"]').count()
        except Exception:
            return 0

    last = anchor_count()
    show_more_selectors = [
        'button[aria-label="Show more results"]',
        'a[aria-label="Show more results"]',
        'button[data-testid="pagination-button-next"]',
        'nav[aria-label="Pagination"] button:has-text("Show more")',
    ]
    for _ in range(max_clicks):
        if not _click_if_exists(page, show_more_selectors, logger, "Show more results"):
            break
        try:
            page.wait_for_load_state("networkidle", timeout=8000)
        except Exception:
            pass
        for __ in range(3):
            try:
                page.mouse.wheel(0, random.randint(800, 1200))
            except Exception:
                break
            time.sleep(random.uniform(0.25, 0.4))
        curr = anchor_count()
        if curr <= last:
            stagnant_after_clicks += 1
        else:
            stagnant_after_clicks, last = 0, curr
        if stagnant_after_clicks >= 2:
            logger.info('[host] No new results after two "Show more" clicks — stopping.')
            break


def _collect_room_links_from_dom(page: Page, logger: logging.Logger, max_scrolls: int = 60) -> List[str]:
    all_links, last_len = [], 0
    for i in range(max_scrolls):
        try:
            anchors = page.locator('a[href*="/rooms/"]')
            count = anchors.count()
            for idx in range(min(count, 800)):
                href = anchors.nth(idx).get_attribute("href") or ""
                if "/rooms/" in href:
                    if href.startswith("/"):
                        href = "https://www.airbnb.com" + href
                    all_links.append(href.split("?")[0])
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

def _expand_about_block(about_root: Locator, logger: logging.Logger) -> bool:
    """
    Click every 'Show more/Show all' near the About block, tolerating re-renders.
    Returns True if the section clearly expanded at least once.
    """
    CANDS = [
        # EN
        'button:has-text("Show more")','button:has-text("Show all")',
        'a:has-text("Show more")','a:has-text("Show all")',
        'span[role="button"]:has-text("Show more")','span[role="button"]:has-text("Show all")',
        'button:text-matches("^\\s*Show\\s+(all|more)\\b", "i")',
        'a:text-matches("^\\s*Show\\s+(all|more)\\b", "i")',
        'span[role="button"]:text-matches("^\\s*Show\\s+(all|more)\\b", "i")',
        # FR/ES/DE/IT/PT/NL/AR/TR
        'button:has-text("Afficher plus")','button:has-text("Afficher tout")',
        'button:has-text("Ver más")','button:has-text("Mehr anzeigen")',
        'button:has-text("Mostra di più")','button:has-text("Mostrar mais")',
        'button:has-text("Alles weergeven")',
        'button:has-text("عرض المزيد")','button:has-text("إظهار المزيد")',
        'button:has-text("Daha fazla göster")',
        # Generic role
        '[role="button"]:has-text("Show more")',
        '[role="button"]:text-matches("^\\s*Show\\s+(all|more)\\b", "i")',
    ]

    def _find_btn_near(root: Locator) -> Optional[Locator]:
        # Scoped first
        for sel in CANDS:
            try:
                b = root.locator(sel).first
                if b.count() and b.is_visible():
                    return b
            except Exception:
                pass
        # Page-wide nearest to About heading y
        try:
            page = root.page
            about_y = (root.bounding_box() or {}).get("y", 0) or 0
            all_btns = page.locator("|".join(CANDS))
            n = min(all_btns.count(), 25)
            best, best_dy = None, 1e9
            for i in range(n):
                el = all_btns.nth(i)
                try:
                    if not el.is_visible():
                        continue
                    box = el.bounding_box() or {}
                    dy = abs((box.get("y") or 0) - about_y)
                    if dy < best_dy:
                        best_dy, best = dy, el
                except Exception:
                    continue
            return best
        except Exception:
            return None

    def _height(loc: Locator) -> float:
        try:
            return float((loc.bounding_box() or {}).get("height") or 0.0)
        except Exception:
            return 0.0

    expanded_once = False
    last_h = _height(about_root)

    # Try multiple times—some profiles have 2–3 separate expanders
    for _ in range(6):
        btn = _find_btn_near(about_root)
        if not btn:
            break

        try:
            btn.scroll_into_view_if_needed(timeout=1500)
        except Exception:
            pass

        # Several strategies
        clicked = False
        for how, act in (
            ("click",     lambda: btn.click(timeout=1500, force=True)),
            ("js-click",  lambda: btn.evaluate("el => el.click()")),
            ("enter",     lambda: (btn.focus(), btn.press("Enter", timeout=800))),
        ):
            try:
                act()
                logger.info(f"[about] expander triggered via {how}")
                clicked = True
                break
            except Exception:
                continue

        if not clicked:
            continue

        # Allow the UI to reflow
        try:
            about_root.page.wait_for_timeout(250)
        except Exception:
            pass

        new_h = _height(about_root)
        if new_h > last_h + 12:
            expanded_once = True
            last_h = new_h

        # If another Show more is still around, loop again
        more = _find_btn_near(about_root)
        if not more:
            break

    return expanded_once
def _extract_about_and_bio(page: Page, logger: logging.Logger, host_name: Optional[str] = None) -> Dict[str, Optional[str]]:
    """
    Extract:
      • about_text: bullet/kv pairs under “About …”
      • bio_text: free-form paragraph(s) nearby
    Works even when there is NO 'Show more' button.
    """
    out: Dict[str, Optional[str]] = {"about_text": None, "bio_text": None}

    def _clean(s: str) -> str:
        s = (s or "").replace("\u00A0", " ").replace("\u202F", " ")
        return re.sub(r"\s{2,}", " ", s.strip())

    # ---- 1) Locate the “About …” heading ----
    about_h = page.get_by_role("heading", name=re.compile(r"^\s*About\b", re.I)).first
    if not about_h.count():
        about_h = page.locator(':is(h1,h2,h3):text-matches("^\\s*About\\b", "i")').first
    if not about_h.count():
        logger.info("[about/bio] No 'About' heading found; skipping.")
        return out

    # ---- 2) Build a robust search root for the bullets panel ----
    # Priority: section containing the heading -> nearest following sibling section/div (some layouts split cards)
    search_roots: List[Locator] = []
    try:
        sec = about_h.locator("xpath=ancestor::section[1]").first
        if sec.count():
            search_roots.append(sec)
    except Exception:
        pass
    try:
        par = about_h.locator("xpath=ancestor::*[self::div or self::main][1]").first
        if par.count():
            search_roots.append(par)
    except Exception:
        pass
    try:
        sib = about_h.locator("xpath=following::*[self::section or self::div][1]").first
        if sib.count():
            search_roots.append(sib)
    except Exception:
        pass

    # De-dup while keeping order
    seen_ids = set()
    roots: List[Locator] = []
    for r in search_roots:
        try:
            b = r.bounding_box() or {}
            sig = (round(b.get("x", 0), 1), round(b.get("y", 0), 1), round(b.get("width", 0), 1))
        except Exception:
            sig = id(r)
        if sig not in seen_ids:
            roots.append(r)
            seen_ids.add(sig)

    # If for some reason we didn't get anything, at least use the heading itself
    if not roots:
        roots = [about_h]

    # Encourage lazy content to mount
    try:
        roots[0].scroll_into_view_if_needed(timeout=1000)
        page.wait_for_timeout(150)
    except Exception:
        pass

    # ---- 3) Try to expand (OK if nothing expands) ----
    expanded = False
    try:
        expanded = _expand_about_block(roots[0], logger)
    except Exception:
        pass
    if not expanded:
        logger.info("[about/bio] no expander or no growth; proceeding with visible content")

    # Re-resolve heading/root after possible re-render
    try:
        about_h = page.get_by_role("heading", name=re.compile(r"^\s*About\b", re.I)).first or about_h
        sec = about_h.locator("xpath=ancestor::section[1]").first
        roots = [sec if sec.count() else about_h.locator("xpath=ancestor::*[self::div or self::main][1]").first]
    except Exception:
        pass

    # x-gate to avoid the left profile card
    try:
        heading_box = about_h.bounding_box() or {}
        x_gate = (heading_box.get("x", 0) - 16)
    except Exception:
        x_gate = 0

    UI_NOISE = re.compile(r"(?:^|\b)(identity verified|superhost|joined|reviews?|rating|response rate|response time|show all)\b", re.I)
    CARD_NOISE = re.compile(r"(?:^|\b)(host|month hosting|months? hosting|%\{count\}|about\s+\w+)$", re.I)
    NUM_ONLY  = re.compile(r"^[0-9]+(?:\.[0-9]+)?$")
    ALLOWED_UNLABELED = re.compile(r"^(speaks|lives|i['’]m|i am|i spend|i[’']m obsessed|i’m obsessed|i am obsessed)\b", re.I)

    def iter_all(loc: Locator):
        try:
            n = loc.count()
        except Exception:
            n = 0
        for i in range(n):
            yield loc.nth(i)

    # ---- 4) Parse bio candidates (paragraphs) and bullets from any of the roots ----
    bio_y = 1e9
    bio_candidates: List[tuple] = []
    about_pairs: List[str] = []
    dedupe = set()

    for about_root in roots:
        # BIO
        for el in iter_all(about_root.locator("p, div[lang], blockquote, span")):
            try:
                t = _clean(el.inner_text(timeout=700))
                box = el.bounding_box() or {}
            except Exception:
                continue
            if not t or len(t) < 40:
                continue
            if re.match(r"^[^:\n]{1,35}:\s", t):
                continue
            if UI_NOISE.search(t) or CARD_NOISE.search(t):
                continue
            score = 0
            if re.search(r"\b(I\s+(am|’m|'m|live|work|travel|enjoy|love|go|like|prefer)|\bmy\s+|As\s+I\b)", t, re.I):
                score += 10
            score += min(len(t), 2000) / 120.0
            bio_candidates.append((score, box.get("y", 1e9), t))

        # BULLETS (icon rows, list items, plain divs)
        bullet_sel = (
            ":is([data-testid*='about'], ul li, li, div):has(svg), "
            "ul li, li, div"
        )
        for el in iter_all(about_root.locator(bullet_sel)):
            try:
                txt = _clean(el.inner_text(timeout=700))
                box = el.bounding_box() or {}
            except Exception:
                continue
            if not txt:
                continue

            # Spatial gates: right column and above bio
            if box.get("x", x_gate) < x_gate:
                continue
            if box.get("y", 0) >= bio_y:
                continue

            # Text gates
            if UI_NOISE.search(txt) or CARD_NOISE.search(txt) or NUM_ONLY.match(txt):
                continue
            if host_name and re.search(rf"^{re.escape(host_name)}\b", txt, re.I):
                continue
            if re.match(r"^About\b", txt, re.I):
                continue

            item = None
            if ":" in txt:
                k, v = txt.split(":", 1)
                k, v = _clean(k), _clean(v)
                if 1 <= len(k) <= 50 and len(v) >= 1:
                    item = f"{k}: {v}"
            else:
                if ALLOWED_UNLABELED.match(txt) and len(txt) <= 200:
                    item = txt

            if item:
                key = item.lower()
                if key not in dedupe:
                    dedupe.add(key)
                    about_pairs.append(item)

    if bio_candidates:
        bio_candidates.sort(key=lambda x: (-x[0], x[1]))
        out["bio_text"] = bio_candidates[0][2]
        bio_y = bio_candidates[0][1]

    if about_pairs:
        out["about_text"] = " ; ".join(about_pairs)

    # ---- 5) LAST-RESORT FALLBACK (no expander, strict gates missed) ----
    if not (out["about_text"] or out["bio_text"]):
        logger.info("[about/bio] strict parse empty; soft fallback over nearest sibling block")
        try:
            # The right column after the card (very common on your screenshot)
            right_block = about_h.locator("xpath=ancestor::section[1]/following-sibling::*[1]").first
            blob = (right_block.inner_text(timeout=1200) or "").strip()
        except Exception:
            blob = ""
        lines = [re.sub(r"\s{2,}", " ", l.strip()) for l in (blob.splitlines() if blob else [])]
        kv = []
        for l in lines:
            if ":" in l and len(l) <= 200 and not re.search(r"(reviews?|rating|joined|superhost|identity verified)", l, re.I):
                k, v = l.split(":", 1)
                if 1 <= len(k.strip()) <= 50 and len(v.strip()) >= 1:
                    kv.append(f"{k.strip()}: {v.strip()}")
        if kv:
            out["about_text"] = " ; ".join(dict.fromkeys(kv))
        paras = [p for p in re.split(r"\n\s*\n|•", blob) if len(p.strip()) >= 40]
        if paras and not out["bio_text"]:
            out["bio_text"] = max(paras, key=len).strip()

    logger.info(f"[about/bio] about_len={len(out['about_text'] or '')} bio_len={len(out['bio_text'] or '')}")
    return out


def _extract_host_reviews_tab_or_modal(page: Page, logger: logging.Logger, max_keep: int = 200) -> List[Dict[str, Any]]:
    out: List[Dict[str, Any]] = []

    def _open_reviews_ui() -> None:
        if not _click_if_exists(
            page,
            [
                '[role="tablist"] :text-matches("^Reviews$", "i")',
                '[data-testid*="tab"]:text-matches("^Reviews$", "i")',
                'button:has-text("Reviews")',
                'a:has-text("Reviews")'
            ],
            logger, "Reviews tab"
        ):
            _click_if_exists(
                page,
                [
                    'button:text-matches("show.*reviews", "i")',
                    'a:text-matches("show.*reviews", "i")'
                ],
                logger, "Show all reviews"
            )

    _open_reviews_ui()

    root = page.locator(
        ':is('
        '[role="tabpanel"] [data-testid*="reviews"], '
        '[data-testid="user-reviews-list"], '
        '[data-testid="reviews-list"], '
        'div[role="dialog"]:has(h2:text-matches("reviews", "i")) section > div, '
        'div[role="dialog"], '
        'section:has(h2:text-matches("reviews", "i"))'
        ')'
    ).first
    if not root.count():
        logger.info("[host] No reviews container found")
        return out

    # scroll the modal/body list to load items + handle pagination
    scroll_root = root
    dlg = page.locator('div[role="dialog"]').first
    if dlg.count():
        sr = dlg.locator('section > div').first
        if sr.count():
            scroll_root = sr

    logger.info("[host] Parsing reviews (with scrolling + pagination)…")

    def _scroll_to_bottom():
        try:
            scroll_root.evaluate("el => { el.scrollTop = el.scrollHeight; }")
        except Exception:
            try:
                page.mouse.wheel(0, 1600)
            except Exception:
                pass
        page.wait_for_timeout(350)

    # Deep scroll initial view
    for _ in range(24):
        before = None
        try:
            before = scroll_root.evaluate("el => el.scrollTop")
        except Exception:
            pass
        _scroll_to_bottom()
        after = None
        try:
            after = scroll_root.evaluate("el => el.scrollTop")
        except Exception:
            pass
        if before is not None and after is not None and after == before:
            break

    pagination_selectors = [
        'button[aria-label*="Show more reviews"]',
        'button:has-text("Show more reviews")',
        'button:has-text("Show more")',
        'a[aria-label*="Show more reviews"]',
        'a:has-text("Show more reviews")',
        'nav[aria-label="Pagination"] button',
        'button[data-testid="pagination-button-next"]',
    ]

    def _count_cards():
        try:
            return root.locator(':is([data-review-id], [data-testid="user-profile-review"], [data-testid="review-card"], div.cwt93ug)').count()
        except Exception:
            return 0

    last_count = _count_cards()
    stalls = 0
    for _ in range(50):  # plenty of pages
        clicked = False
        for sel in pagination_selectors:
            el = root.locator(sel).first
            if el.count() and el.is_visible():
                try:
                    el.click(timeout=2500, force=True)
                    clicked = True
                    break
                except Exception:
                    continue
        if not clicked:
            # Try more scrolling; if no growth, stop.
            for __ in range(6):
                _scroll_to_bottom()
            new_count = _count_cards()
            if new_count <= last_count:
                break
            last_count = new_count
            continue

        # After click, wait + scroll to load new batch
        try:
            page.wait_for_load_state("networkidle", timeout=6000)
        except Exception:
            pass
        for __ in range(10):
            _scroll_to_bottom()

        new_count = _count_cards()
        if new_count <= last_count:
            stalls += 1
        else:
            stalls = 0
            last_count = new_count
        if stalls >= 2:
            break

    # cards (both guests/hosts). We'll filter out "Response from ..." later.
    cards = root.locator(':is([data-review-id], [data-testid="user-profile-review"], [data-testid="review-card"], div.cwt93ug)')
    total_cards = cards.count()
    total = total_cards if (max_keep is None) else min(total_cards, max_keep)
    logger.info(f"[host] Found {total_cards} review cards; extracting (keeping {total})…")

    seen = set()
    for i in range(total):
        b = cards.nth(i)

        # Skip host replies like "Response from …"
        if b.locator(':text-matches("^\\s*Response from\\b", "i")').count():
            continue

        def grab_txt(sel: str, tmo: int = 300) -> str:
            return _inner_text_quick(b.locator(sel), timeout=tmo) or ""

        # Name
        name = ""
        for ns in ["h3", '[data-testid*="reviewer"]', '[itemprop="author"]', 'div.t126ex63', 'header :text-matches(".+", "i")']:
            t = grab_txt(ns)
            if t:
                name = _clean_review_text(t) or ""
                break

        # Location (typical small gray line under name; e.g., "Paris, France")
        location = ""
        for ls in ['div.s17vloqa span', '[data-testid="reviewer-location"]', 'header div:has-text(",")']:
            t = grab_txt(ls)
            if t and "," in t and len(t) <= 80:
                location = _clean_review_text(t) or ""
                break

        # Rating (several strategies)
        rating = None
        aria = _get_attr_quick(
            b.locator(':is([aria-label*="out of 5"], [aria-label*="sur 5"])'),
            "aria-label",
            timeout=1200
        )
        if aria:
            m = re.search(r'(\d+(?:\.\d+)?)\s*(?:out|sur)\s*of?\s*5', aria, re.I)
            if m:
                try:
                    rating = float(m.group(1))
                except Exception:
                    pass

        if rating is None:
            numtxt = grab_txt('span.a8jt5op')
            if numtxt and re.match(r"^\d+(?:\.\d+)?$", numtxt):
                try:
                    rating = float(numtxt)
                except Exception:
                    rating = None

        if rating is None:
            try:
                stars = b.locator(':is([data-testid*="rating"] svg, svg[data-testid*="star"])')
                cnt = stars.count()
                if cnt:
                    rating = float(min(cnt, 5))
            except Exception:
                pass

        # literal ★/⭐ in text
        if rating is None:
            blob = (grab_txt(':scope') or '').replace('·', ' ')
            m = re.search(r'([★⭐]{1,5})', blob)
            if m:
                rating = float(len(m.group(1)))

        # FINAL textual fallback
        if rating is None:
            full = grab_txt(':scope') or ''
            m = re.search(r'Rating\s*(\d+(?:\.\d+)?)\s*out\s*of\s*5', full, re.I)
            if m:
                try:
                    rating = float(m.group(1))
                except Exception:
                    pass

        # Date (strip rating noise)
        header_blob = " ".join(filter(None, [
            grab_txt('time'),
            grab_txt('[data-testid="review-date"]'),
            grab_txt('span:text-matches("(ago|week|month|year|20\\d{2})", "i")'),
            grab_txt('div.sv3k0pp')
        ]))
        date_text = _extract_first_date(header_blob) or (_extract_first_date(grab_txt(':scope')) or None)

        # Expand "Show more" inside a card
        for sel in [
            'button:has-text("Show more")',
            'button:has-text("See more")',
            'button:has-text("Read more")',
            'a:has-text("Show more")',
            'span[role="button"]:has-text("Show more")',
            # French variants
            'button:has-text("Afficher plus")',
            'button:has-text("Voir plus")',
            'span[role="button"]:has-text("Afficher plus")',
            'span[role="button"]:has-text("Voir plus")',
        ]:
            btns = b.locator(sel)
            for k in range(min(btns.count(), 3)):
                try:
                    btns.nth(k).click(timeout=1200, force=True)
                    page.wait_for_timeout(150)
                except Exception:
                    pass

        body = b.locator(':is([data-testid="review-text"], span[data-testid="review-text-main-span"])').first
        if not body.count():
            # Fallback to the classic container used on older layouts
            body = b.locator('div[id^="review-"] > div').first

        # Stitch together paragraphs if needed
        text_parts: List[str] = []
        if body.count():
            main = _inner_text_quick(body, timeout=600) or ""
            if main:
                text_parts.append(main)
        else:
            candidates = b.locator(':is([data-testid="review-text"], blockquote, q, p, div[lang] span)').all()
            for node in candidates[:8]:
                try:
                    t = (node.inner_text(timeout=400) or "").strip()
                except Exception:
                    t = ""
                if not t:
                    continue
                # Skip obvious noise
                if re.search(r"\b(Translated from|Show original|Response from)\b", t, re.I):
                    continue
                if re.search(r"Rating\s*\d+(?:\.\d+)?\s*out\s*of\s*5", t, re.I):
                    continue
                text_parts.append(t)

        text_raw = " ".join([p for p in text_parts if p]).strip()
        text = _clean_review_text(text_raw, location)

        # de-dup & finalize
        if (name or text):
            sig = (name, date_text or "", text[:80])
            if sig in seen:
                continue
            seen.add(sig)
            out.append({
                "reviewer_name": name or None,
                "reviewer_location": location or None,
                "rating": rating,
                "date_text": date_text or None,
                "text": text or None
            })

    logger.info(f"[host] Extracted {len(out)} host reviews")
    return out


def _extract_host_reviews_modal(page: Page, logger: logging.Logger, max_keep: int = 150) -> List[Dict[str, Any]]:
    """
    Scrape the “<Name>’s reviews” modal (or inline panel) on the profile.
    Returns list of { reviewer_name, rating, date_text, text}
    """
    out: List[Dict[str, Any]] = []

    # Open the modal/panel
    _click_if_exists(
        page,
        [
            'button:text-matches("^Show all \\d+ reviews$", "i")',
            'button:has-text("Show all reviews")',
            'a:has-text("Show all")'
        ],
        logger, "Show all reviews"
    )

    # Modal root or inline section
    root = page.locator('div[role="dialog"]:has(h2:text-matches("reviews", "i"))').first
    if not root.count():
        root = page.locator('section:has(h2:text-matches("reviews", "i"))').first
    if not root.count():
        logger.info("[host] No reviews container found")
        return out

    # Scroll to load a bunch of cards
    logger.info("[host] Parsing reviews (with scrolling)…")
    for _ in range(18):
        try:
            root.evaluate("el => el.scrollTop = el.scrollHeight")
            page.wait_for_timeout(350)
        except Exception:
            break

    # Cards: require an h3 (reviewer) and a stars aria-label to avoid false matches
    cards = root.locator('div:has(h3):has([aria-label*="out of 5"])')
    total = min(cards.count(), max_keep)
    logger.info(f"[host] Found {total if total else 0} visible review cards")  # fixed f-string

    seen = set()
    for i in range(total):
        b = cards.nth(i)
        try:
            name = (b.locator("h3").first.inner_text() or "").strip()

            # rating
            rating = None
            try:
                al = b.locator('[aria-label*="out of 5"]').first.get_attribute("aria-label") or ""
                m = re.search(r"(\d+(?:\.\d+)?)\s*out\s*of\s*5", al, re.I)
                if m:
                    rating = float(m.group(1))
            except Exception:
                pass

            # date text (relative or absolute)
            date_text = ""
            try:
                date_text = b.locator(
                    'span:text-matches("(ago|week|month|year|\\b20\\d{2}\\b)", "i")'
                ).first.inner_text().strip()
            except Exception:
                pass

            # main review text
            text = ""
            for sel in [
                'span[data-testid="review-text-main-span"]',
                '[data-testid="review-text"]',
                'div[lang] span',
                'p'
            ]:
                el = b.locator(sel).first
                if el.count():
                    try:
                        text = el.inner_text().strip()
                        if text:
                            break
                    except Exception:
                        pass

            if text:
                sig = (name, date_text, text[:80])
                if sig in seen:
                    continue
                seen.add(sig)
                out.append({
                    "reviewer_name": name,
                    "rating": rating,
                    "date_text": date_text,
                    "text": text,
                })
        except Exception:
            continue

    logger.info(f"[host] Extracted {len(out)} host reviews")
    return out


def _extract_guidebooks(page: Page, logger: logging.Logger) -> List[Dict[str, str]]:
    cards, out, seen = page.locator('a[href*="/guidebooks"]'), [], set()
    try:
        for i in range(min(cards.count(), 24)):
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


MONTHS = "|".join(
    [
        "January", "February", "March", "April", "May", "June",
        "July", "August", "September", "October", "November", "December",
        "Jan", "Feb", "Mar", "Apr", "Jun", "Jul", "Aug", "Sep", "Sept", "Oct", "Nov", "Dec",
    ]
)


def _extract_travels(page: Page, logger: logging.Logger) -> List[Dict[str, Union[str, int]]]:
    sec: Optional[Locator] = None
    for sel in ['div:has(h2:has-text("Where")):has(h2:has-text("has been"))']:
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
        line1, line2 = lines[i], lines[i + 1]
        if re.match(r"^[A-Za-zÀ-ÿ'.\- ]+,\s+[A-Za-zÀ-ÿ'.\- ]+$", line1):
            if re.search(rf"({MONTHS})\s+\d{{4}}", line2, re.IGNORECASE) or re.search(
                r"\b\d+\s+trips?\b", line2, re.IGNORECASE
            ):
                city, country, trips = line1.split(",")[0].strip(), line1.split(",")[1].strip(), 0
                m = re.search(r"\b(\d+)\s+trips?\b", line2, re.IGNORECASE)
                if m:
                    trips = int(m.group(1))
                results.append({"place": city, "country": country, "trips": trips, "when": line2})
    if results:
        logger.info(f"✅ Parsed {len(results)} visited places")
    return results


# Optional: Add this to host_agent.py if you want to extract property reviews
def _extract_property_reviews(page: Page, logger: logging.Logger, max_reviews: int = 200) -> List[Dict[str, Any]]:
    """
    Scrape the host's Reviews tab (guest->host/property feedback).
    Saves generic fields; 
    """
    def _click_any(selectors: List[str]) -> bool:
        for sel in selectors:
            loc = page.locator(sel).first
            if loc.count() and loc.is_visible():
                try:
                    loc.click()
                    return True
                except Exception:
                    pass
        return False

    # 1) Go to the Reviews tab (or anchor)
    clicked = _click_any([
        '[data-testid="user-profile-reviews-tab"]',
        'a[href*="#reviews"]',
        'a:has-text("Reviews")',
        'button:has-text("Reviews")'
    ])
    if not clicked:
        try:
            page.goto(page.url.split("#")[0] + "#reviews", wait_until="domcontentloaded", timeout=15000)
            clicked = True
        except Exception:
            pass

    if not clicked:
        logger.info("[host] No Reviews tab available.")
        return []

    # 2) Wait for any kind of review card to show
    try:
        page.wait_for_selector(
            ':is([data-testid="user-profile-review"], [data-review-id], [data-testid="review-card"])',
            timeout=15000
        )
    except Exception:
        logger.info("[host] Reviews tab opened but no cards appeared.")
    # 3) Lazy-load (scroll) to get more reviews
    last = -1
    for _ in range(16):
        cards = page.locator(':is([data-testid="user-profile-review"], [data-review-id], [data-testid="review-card"])')
        cnt = cards.count()
        if cnt == last:
            break
        last = cnt
        try:
            page.mouse.wheel(0, 1400)
        except Exception:
            pass
        page.wait_for_timeout(700)
        if cnt >= max_reviews:
            break

    # 4) Extract fields
    rows: List[Dict[str, Any]] = []
    cards = page.locator(':is([data-testid="user-profile-review"], [data-review-id], [data-testid="review-card"])')
    total = min(cards.count(), max_reviews)
    logger.info(f"[host] Found {total} review cards; extracting…")

    for i in range(total):
        card = cards.nth(i)

        # Expand "Show more" inside a card if present
        try:
            show_more = card.locator('button:has-text("Show more")')
            for k in range(min(show_more.count(), 3)):
                show_more.nth(k).click()
                page.wait_for_timeout(150)
        except Exception:
            pass

        def safe_text(sel: str, timeout=800) -> Optional[str]:
            try:
                loc = card.locator(sel).first
                if loc.count():
                    return loc.inner_text(timeout=timeout).strip()
            except Exception:
                return None
            return None

        # id (may be missing in some AB tests)
        review_id = card.get_attribute("data-review-id")

        # reviewer
        reviewer_name = safe_text(':is([data-testid="guest-name"], [itemprop="author"], [data-testid="reviewer-name"], h3)')
        reviewer_location = safe_text(':is([data-testid="reviewer-location"])')

        # rating text (keep it raw; SQL layer converts to float if possible)
        rating_text = None
        try:
            rating_el = card.locator(':is([aria-label*="out of 5"], [data-testid*="rating"])').first
            rating_text = rating_el.get_attribute("aria-label") or safe_text(':is([data-testid*="rating"])')
        except Exception:
            pass

        # date
        date_text = safe_text(':is(time, [data-testid="review-date"], span:has-text-matches("ago|week|month|year|20\\d{2}", "i"))')

        # text (glue multiple fragments)
        review_text = None
        try:
            blocks = card.locator(':is([data-testid="review-text"], blockquote, q, p, span:not([aria-label]))')
            parts = []
            for j in range(min(blocks.count(), 6)):
                t = blocks.nth(j).inner_text(timeout=600).strip()
                if t:
                    parts.append(t)
            review_text = " ".join(parts) if parts else None
        except Exception:
            pass

        if review_text or reviewer_name:
            rows.append({
                "reviewer_location": reviewer_location,
                "rating": rating_text,     # SQL._to_float_or_none will normalize if numeric
                "date_text": date_text,
                "text": review_text
            })

    if rows:
        logger.info(f"[host] ✅ Extracted {len(rows)} review rows.")
    else:
        logger.info("[host] No review rows extracted.")

    return rows


def _extract_some_reviews(page: Page, logger: logging.Logger, max_reviews: int = 0) -> List[Dict[str, Any]]:
    """Find and scrape host reviews (modal or on-page)."""
    try:
        reviews_section_selector = 'div:has(h2:text-matches("reviews", "i"))'
        reviews_section = page.locator(reviews_section_selector).first

        if reviews_section.count() == 0:
            logger.warning("[host] The primary reviews section container could not be found. No reviews will be scraped.")
            return []

        logger.info("[host] Scrolling to the reviews section...")
        reviews_section.scroll_into_view_if_needed()
        page.wait_for_timeout(800)

        show_reviews_selector = 'button:text-matches("Show (all|more|[0-9]+).*reviews", "i")'
        clicked_button = _click_if_exists(reviews_section, [show_reviews_selector], logger, "Show ... reviews button")
        if not clicked_button:
            logger.info("[host] No 'Show all reviews' button; scraping visible reviews.")
    except Exception as e:
        logger.error(f"[host] error locating reviews: {e}")
        return []

    # Load content
    page.wait_for_timeout(2000)

    # modal or not
    root = None
    is_modal = False
    modal_selector = 'div[role="dialog"]:has(h2:text-matches("Reviews", "i"))'
    if page.locator(modal_selector).is_visible():
        root = page.locator(modal_selector)
        is_modal = True
        logger.info("[host] Reviews in modal.")
    else:
        root = reviews_section

    # expand more
    if max_reviews == 0:
        for _ in range(50):
            try:
                more = root.locator('button:has-text("Show more reviews")').first
                if more.is_visible(timeout=1500):
                    more.click(timeout=2500)
                    page.wait_for_load_state("networkidle", timeout=4000)
                else:
                    break
            except Exception:
                break

    out: List[Dict[str, Any]] = []
    review_block_selector = 'div:has(> div h3):has(span[aria-label*="out of 5 stars"])'
    blocks = root.locator(review_block_selector)
    total = blocks.count()
    if total > 0:
        logger.info(f"[host] Found {total} review blocks. Parsing...")
        for i in range(total):
            b = blocks.nth(i)
            try:
                reviewer_name = b.locator("h3").first.inner_text().strip()
                date_text = b.locator('span:has-text-matches("ago|week|month|year|202", "i")').first.inner_text().strip()
                text_el = b.locator('span[data-testid="review-text-main-span"], .ll4r2nl').first
                text = text_el.inner_text().strip() if text_el.count() else ""
                rating = None
                try:
                    al = b.locator('span[aria-label*="out of 5 stars"]').first.get_attribute("aria-label") or ""
                    mm = re.search(r"Rated\s*(\d\.?\d*)", al)
                    if mm:
                        rating = float(mm.group(1))
                except Exception:
                    pass
                if text:
                    out.append({
                        "reviewer_name": reviewer_name,
                        "rating": rating,
                        "date_text": date_text,
                        "text": text,
                    })
            except Exception:
                continue

    if out:
        logger.info(f"✅ Successfully collected {len(out)} reviews.")
    else:
        logger.warning("[host] Review container was found, but no review data could be extracted.")

    if is_modal:
        try:
            root.locator('button[aria-label="Close"]').first.click(timeout=2000)
        except Exception:
            try:
                page.keyboard.press("Escape")
            except Exception:
                pass

    return out


# ------------------------------- Main runner ---------------------------------

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

    # Dates used for PDP hydration (also in logs)
    checkin_date = (datetime.now() + timedelta(days=90)).strftime("%Y-%m-%d")
    checkout_date = (datetime.now() + timedelta(days=95)).strftime("%Y-%m-%d")
    logger.info(f"[host] Using default search dates: {checkin_date} to {checkout_date}")
    logger.info(f"🔎 Host scrape start | userId={user_id} | url={host_url}")

    request_headers: Dict[str, str] = {}
    request_item_token: Optional[str] = None
    request_item_client_id: Optional[str] = None
    x_airbnb_api_key = os.getenv("AIRBNB_API_KEY")
    x_airbnb_api_key_captured: Optional[str] = None            # your name

    request_client_version: Optional[str] = None

    with sync_playwright() as p:
        browser: Browser = p.chromium.launch(
            headless=False,
            proxy=HostConfig.CONFIG_PROXY,
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
            extra_http_headers={"Accept-Language": "en-US,en;q=0.9,fr;q=0.7,ar;q=0.6"},
        )
        context = Tarnished.apply_stealth(context)

        def handle_request(route: Route):
            nonlocal request_item_token, request_item_client_id, request_headers, x_airbnb_api_key_captured, request_client_version
            req = route.request
            if "/api/v3/StaysPdpSections" in req.url:
                token = _extract_pdp_token_from_request(req)
                if token and not request_item_token:
                    request_item_token, request_item_client_id = token, req.headers.get("x-client-request-id")
                    logger.info(f"[route] PDP token = {request_item_token}")
                request_headers = req.headers.copy()
                api_k = req.headers.get("x-airbnb-api-key")
                if api_k:
                    x_airbnb_api_key_captured = api_k.strip()
                request_client_version = req.headers.get("x-client-version") or request_client_version
            route.continue_()

        context.route("**/api/v3/*", handle_request)

        def on_request(req: Request):
            nonlocal request_item_token, request_item_client_id, request_headers, x_airbnb_api_key_captured, request_client_version
            if "/api/v3/StaysPdpSections" in req.url:
                token = _extract_pdp_token_from_request(req)
                if token and not request_item_token:
                    request_item_token, request_item_client_id = token, req.headers.get("x-client-request-id")
                    logger.info(f"[event] PDP token captured = {request_item_token}")
                request_headers = req.headers.copy()
                api_k = req.headers.get("x-airbnb-api-key")
                if api_k:
                    x_airbnb_api_key_captured = api_k.strip()
                request_client_version = req.headers.get("x-client-version") or request_client_version

        context.on("request", on_request)

        page = context.new_page()
        page.set_default_timeout(60000)
        logger.info(f"[host] Visiting host page…")
        page.goto(host_url, wait_until="domcontentloaded", timeout=60000)
        HostScrapingUtils._dismiss_any_popups_enhanced(page, logger, max_attempts=4)
        _wait_profile_ready(page, logger)

        # --- PROFILE HEADER FIELDS (avatar + name + badges) ---
        host_name: Optional[str] = None
        profile_photo_url: Optional[str] = None

        # 1) Prefer the name inside the avatar/profile header card
        for sel in [
            '[data-testid*="user-profile-header"] [data-testid*="name"]',
            '[data-testid*="user-profile-header"] h1',
            '[data-testid*="user-profile-header"] h2',
            'div.h1oqg76h h1',                 # common container for the top card
            'div.h1oqg76h h2',
            'div.h1oqg76h [data-testid*="name"]',
        ]:
            try:
                loc = page.locator(sel).first
                if loc.count():
                    t = (loc.inner_text(timeout=1500) or "").strip()
                    # reject obvious non-names like “Identity verified”, “Host”, or headings
                    if t and len(t) <= 60 and not re.search(r'\b(Identity verified|Host)\b', t, re.I):
                        host_name = t
                        break
            except Exception:
                pass

        # 2) Fallback to the page H1 if needed
        if not host_name:
            for sel in ['main h1', '[data-testid*="user-profile"] h1', 'h1:visible']:
                try:
                    loc = page.locator(sel).first
                    if loc.count():
                        t = (loc.inner_text(timeout=1500) or "").strip()
                        if t:
                            host_name = t
                            break
                except Exception:
                    pass

        # 3) Normalize: strip “About …” in multiple locales and tidy whitespace
        if host_name:
            m = re.match(
                r'^(?:About|À propos de|À propos d’|À propos d\'|Acerca de|Sobre|Über|Informazioni su)\s+(.+)$',
                host_name, flags=re.IGNORECASE
            )
            if m:
                host_name = m.group(1).strip()
            host_name = re.sub(r'\s{2,}', ' ', host_name).strip()

        # Avatar (several layouts)
        for sel in [
            '[data-testid="user-profile-avatar"] img',
            'img[src*="/user/"]',
            'img[alt*="profile"]',
            'img[alt*="avatar"]'
        ]:
            try:
                loc = page.locator(sel).first
                if loc.count() and loc.is_visible():
                    profile_photo_url = loc.get_attribute("src")
                    if profile_photo_url:
                        break
            except Exception:
                pass

        is_super = 1 if page.locator(':text("Superhost")').count() else 0
        is_ver = 1 if page.locator(':text("Identity verified"), :text("verified")').count() else 0

        ab = _extract_about_and_bio(page, logger, host_name)
        guidebooks = _extract_guidebooks(page, logger)

        if guidebooks:
            SQL.replace_host_guidebooks(db, user_id, guidebooks)
        travels = _extract_travels(page, logger)
        if travels:
            SQL.replace_host_travels(db, user_id, travels)
        reviews = _extract_host_reviews_tab_or_modal(page, logger, max_keep=10000)
        if not reviews:
            reviews = _extract_host_reviews_modal(page, logger)

        if reviews:
            SQL.upsert_host_reviews(db, user_id, reviews)
            SQL.backfill_host_child_names(db, user_id)

        # 1. CALL THE UTILS FUNCTION TO GET THE DATA
        dom_stats = Utils.extract_profile_from_dom(page, logger)

        base_profile = {
            "userId": user_id,
            "userUrl": host_url,
            "name": host_name,
            "isSuperhost": is_super,
            "isVerified": is_ver,
            "ratingAverage": dom_stats.get("ratingAverage"),
            "ratingCount": dom_stats.get("ratingCount"),
            "years": dom_stats.get("years"),
            "months": dom_stats.get("months"),
            "total_listings": None,
            "profile_url": host_url,
            "scraping_time": int(time.time()),
            "profile_photo_url": profile_photo_url,
        }
        SQL.upsert_host_profile(db, _safe_profile_payload(base_profile, ab))

        human = HumanMouseMovement(page)
        vp = page.viewport_size or {"width": 1400, "height": 900}
        human.move_to(int(vp["width"] * 0.45), int(vp["height"] * 0.45))

        _open_all_listings_and_expand(page, logger)
        listing_links = _collect_room_links_from_dom(page, logger, max_scrolls=60)
        if not listing_links:
            logger.warning("[host] No /rooms/ links found on host page.")

        listing_items = []
        for link in listing_links:
            m = re.search(r"/rooms/(\d+)", link)
            if m:
                listing_items.append({"listingId": str(m.group(1)), "listingUrl": f"https://www.airbnb.com/rooms/{m.group(1)}"})

        try:
            SQL.replace_host_listings(db, user_id, listing_items)
            logger.info(f"[host] Saved {len(listing_items)} listing rows for userId={user_id}")
        except Exception as e:
            logger.warning(f"[host] Failed saving host_listings: {e}")

        base_profile2 = {
            "userId": user_id,
            "userUrl": host_url,
            "name": host_name,
            "total_listings": len(listing_items),
            "profile_url": host_url,
            "scraping_time": int(time.time()),
            "profile_photo_url": profile_photo_url,
        }
        SQL.upsert_host_profile(db, _safe_profile_payload(base_profile2, ab))

        if listing_items and not request_item_token:
            _ensure_pdp_token_via_link(context, logger, listing_items[0]["listingUrl"])

        processed, detailed = 0, 0
        HOST_MAX = min(HOST_MAX_LISTINGS, len(listing_items))

        for item in listing_items[:HOST_MAX]:
            _id = item["listingId"]
            processed += 1

            # PDP hydration + details
            if request_item_token and detailed < HOST_DETAIL_SCRAPE_LIMIT:
                try:
                    info = {
                        "id": _id,
                        "link": item["listingUrl"],
                        "checkin": checkin_date,
                        "checkout": checkout_date,
                    }

                    # ---- Build session-authenticated headers ----
                    base_h = (request_headers or {}).copy()
                    base_h.pop("content-length", None)

                    # Mirror the live browser UA
                    try:
                        ua = page.evaluate("() => navigator.userAgent") or None
                        if ua:
                            base_h["user-agent"] = ua
                    except Exception:
                        pass

                    # Add browser cookies for session binding
                    try:
                        cookies = context.cookies()
                        cookie_header = "; ".join(
                            f"{c['name']}={c['value']}" for c in cookies if "airbnb.com" in (c.get("domain") or "")
                        )
                        if cookie_header:
                            base_h["cookie"] = cookie_header
                    except Exception:
                        cookies = []

                    # Inject CSRF if present
                    try:
                        csrf = next(
                            (c["value"] for c in cookies if c.get("name") in ("csrf_token", "airbed_csrf_token")),
                            None,
                        )
                        if csrf and "x-csrf-token" not in {k.lower(): v for k, v in base_h.items()}:
                            base_h["x-csrf-token"] = csrf
                    except Exception:
                        pass

                    # GraphQL context headers
                    base_h.setdefault("x-airbnb-graphql-platform", "web")
                    base_h.setdefault("x-airbnb-graphql-platform-client", "web")
                    base_h.setdefault("origin", "https://www.airbnb.com")
                    base_h.setdefault("referer", item["listingUrl"])

                    # Captured key and client info
                    if x_airbnb_api_key_captured:
                        base_h["x-airbnb-api-key"] = x_airbnb_api_key_captured
                    if request_client_version:
                        base_h["x-client-version"] = request_client_version
                    if request_item_client_id:
                        base_h["x-client-request-id"] = request_item_client_id

                    # ---- Call the scraper with merged headers ----
                    dd = HostScrapingUtils.scrape_single_result(
                        context=context,
                        item_search_token=request_item_token,
                        listing_info=info,
                        logger=logger,
                        api_key=x_airbnb_api_key_captured,
                        client_version=request_client_version or "",
                        client_request_id=request_item_client_id or "",
                        federated_search_id="",
                        currency="MAD",
                        locale="en",
                        base_headers=base_h,  # use the real browser headers
                    )

                    if not dd.get("skip", False):
                        dd["checkin"] = checkin_date
                        dd["checkout"] = checkout_date
                        dd["ListingUrl"] = item["listingUrl"]
                        # NEW: If the profile About/Bio were empty, but PDP exposed a host about text, use it.
                        if not (ab.get("about_text") or ab.get("bio_text")) and dd.get("hostAboutText"):
                            ab["about_text"] = dd["hostAboutText"]
                        # Insert as a full listing
                        dd["ListingId"] = _id
                        dd["ListingObjType"] = _classify_listing(dd)
                        dd["link"] = item["listingUrl"]
                        # Ensure dd carries userUrl so it can be saved into listing_tracking
                        if not dd.get("userUrl") and dd.get("userId"):
                            dd["userUrl"] = f"https://www.airbnb.com/users/profile/{dd['userId']}"

                        SQL.insert_new_listing(db, dd)

                        host_name = dd.get("host") or host_name
                        if host_name:
                            SQL.update_host_listing_name(db, user_id, _id, host_name)

                        pics = dd.get("allPictures") or []
                        if "ListingId" not in dd:
                            logger.error(f"[host] Missing ListingId for listing {_id} — skipping DB insert.")
                            continue

                        if pics:
                            try:
                                from . import host_SQL as SQL_new
                                SQL_new.upsert_listing_pictures_horizontal(db, _id, pics)
                                logger.info(f"[host] ✅ Stored {len(pics)} pictures for {_id}")
                            except Exception as e:
                                logger.warning(f"[host] saving pictures failed for {_id}: {e}")

                        detailed += 1
                        logger.info(f"[host] ✅ hydrated {_id} | host={host_name or '—'} | photos={len(pics)}")

                        # Normalize IDs as strings for comparison
                        _to_str = lambda v: str(v).strip() if v is not None else None
                        if _to_str(dd.get("userId")) == _to_str(user_id):
                            base_profile3 = {
                                "userId": user_id,
                                "userUrl": dd.get("userUrl") or host_url,
                                "name": host_name,
                                "isSuperhost": int(bool(dd.get("isSuperhost"))),
                                "isVerified": int(bool(dd.get("isVerified"))),
                                "ratingAverage": dd.get("ratingAverage") or dd.get("hostRatingAverage") or dd.get("hostrAtingAverage"),
                                "ratingCount": dd.get("ratingCount"),
                                "years": dd.get("years"),
                                "months": dd.get("months"),
                                "total_listings": len(listing_items),
                                "profile_url": host_url,
                                "scraping_time": int(time.time()),
                                "profile_photo_url": profile_photo_url,
                            }
                            # Do not overwrite About/Bio here; we already set ab above if needed
                            SQL.upsert_host_profile(db, _safe_profile_payload(base_profile3, {}))

                        SQL.backfill_host_child_names(db, user_id)
                        try:
                            if host_name:
                                SQL.set_host_name_for_listings(db, user_id, host_name)
                        except Exception as e:
                            logger.warning(f"[host] set_host_name_for_listings failed: {e}")

                except Exception as e:
                    logger.info(f"[host] ❌ PDP hydrate failed for {_id}: {e}")

            else:

                print("ksfljhmsqùmjqogjodsjgojgodjojodjdojgodjgdogjdojgodd")
                # Only insert basic listing if PDP not scraped
                if not SQL.check_if_listing_exists(db, _id):
                    try:
                        SQL.insert_basic_listing(
                            db,
                            {
                                "ListingId": _id,
                                "ListingUrl": item["listingUrl"],
                                "link": item["listingUrl"],
                                "ListingObjType": _classify_listing({"ListingUrl": item["listingUrl"]}),
                            },
                        )
                    except Exception as e:
                        logger.warning(f"[host] Could not insert basic listing {_id}: {e}")

        logger.info(f"🎉 [host] COMPLETE | processed listings: {processed} | hydrated: {detailed}")
        SQL.backfill_host_listing_names_from_tracking(db, user_id)

        try:
            page.close()
        except Exception:
            pass
        try:
            context.close()
        except Exception:
            pass
        try:
            browser.close()
        except Exception:
            pass

    db.close()


if __name__ == "__main__":
    import sys
    if len(sys.argv) < 2:
        print("Usage: python -m airbnb_host.host_agent <host_profile_url>")
        sys.exit(1)
    scrape_host(sys.argv[1])
# --- END OF FILE host_agent.py ---
