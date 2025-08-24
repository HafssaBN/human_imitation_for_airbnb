# host_utils.py
import json
import logging
import sqlite3
import urllib.parse
from typing import Any, Dict, List, Optional

from playwright.sync_api import BrowserContext, Request, Response

import Config


# -------------------------------
# Small helpers (headers, logger, db)
# -------------------------------

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


# -------------------------------
# Network capture for a host page
# -------------------------------

def capture_host_graphql(
    context: BrowserContext,
    host_url: str,
    logger: logging.Logger,
    dismiss_fn=None,
) -> Dict[str, Any]:
    """
    Open a host profile URL, dismiss popups (if provided), and capture:
      - all /api/v3 requests (operationName, variables, extensions.persistedQuery.sha256Hash)
      - all /api/v3 JSON responses (for profile parsing)
    Returns:
      {
        "profile_jsons": [ ... ],
        "listing_req": {
            "url": ".../api/v3/<Operation>/<sha>",
            "method": "GET" or "POST",
            "headers": {...},      # cleaned & usable
            "operationName": str,
            "variables": {...},
            "extensions": {...}
        } or None
      }
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

        # Try to extract operationName/variables/extentions from query params (GET)
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

    def on_res(res: Response):
        try:
            if "/api/v3/" in res.url and "application/json" in (res.headers.get("content-type") or ""):
                captured_responses.append(res.json())
        except Exception:
            # Non-JSON or transient fetch error
            pass

    context.on("request", on_req)
    context.on("response", on_res)

    try:
        logger.info(f"[HOST] Opening profile: {host_url}")
        page.goto(host_url, wait_until="domcontentloaded", timeout=60000)
        if dismiss_fn:
            try:
                dismiss_fn(page, logger, max_attempts=3)
            except Exception:
                pass
        page.wait_for_timeout(2500)  # give GraphQL calls time to fire
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

    return {
        "profile_jsons": captured_responses,
        "listing_req": listing_req,
    }


# -------------------------------
# JSON deep scan utilities
# -------------------------------

def _deep_items(o: Any):
    """Yield every dict/list node in a nested structure."""
    if isinstance(o, dict):
        yield o
        for v in o.values():
            yield from _deep_items(v)
    elif isinstance(o, list):
        for v in o:
            yield from _deep_items(v)


def parse_host_profile_from_jsons(json_blobs: List[Dict[str, Any]], logger: logging.Logger) -> Dict[str, Any]:
    """
    Best-effort deep scan for common host profile fields from the captured JSONs.
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
    }

    for blob in json_blobs:
        for node in _deep_items(blob):
            if not isinstance(node, dict):
                continue

            if profile["name"] is None and isinstance(node.get("name"), str):
                profile["name"] = node["name"]

            if "isSuperhost" in node:
                profile["isSuperhost"] = int(bool(node["isSuperhost"]))

            if "isVerified" in node:
                profile["isVerified"] = int(bool(node["isVerified"]))

            if "isIdentityVerified" in node:
                profile["identityVerified"] = int(bool(node["isIdentityVerified"]))

            if "languages" in node and isinstance(node["languages"], list):
                profile["languages"] = [str(x) for x in node["languages"] if isinstance(x, (str, int, float))]

            if "location" in node and isinstance(node["location"], str):
                profile["location"] = node["location"]

            if "about" in node and isinstance(node["about"], str):
                profile["about"] = node["about"]

            if "memberSince" in node and isinstance(node["memberSince"], str) and not profile["memberSince"]:
                profile["memberSince"] = node["memberSince"]

            if "ratingAverage" in node and profile["ratingAverage"] is None:
                try:
                    profile["ratingAverage"] = float(node["ratingAverage"])
                except Exception:
                    pass

            if "ratingCount" in node and isinstance(node["ratingCount"], (int, float)):
                profile["ratingCount"] = max(profile["ratingCount"], int(node["ratingCount"]))

            if "responseRate" in node and isinstance(node["responseRate"], str):
                profile["responseRate"] = node["responseRate"]

            if "responseTime" in node and isinstance(node["responseTime"], str):
                profile["responseTime"] = node["responseTime"]

            if "profilePicture" in node and isinstance(node["profilePicture"], str):
                profile["profilePhoto"] = node["profilePicture"]

    return profile


# -------------------------------
# Replay captured "host listings" request (pagination)
# -------------------------------

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
