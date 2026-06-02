# =============================================================================
# meta_shared.py
# CONCRETE HORIZONS — MULTI-TENANT FOUNDATION LAYER
# =============================================================================
# This is the root module. Every other engine imports from here.
#
# MULTI-TENANT UPGRADE:
#   Call configure_tenant(cfg) at the top of any script BEFORE using any
#   function in this module. cfg is a dict loaded from config_loader.py.
#   If configure_tenant() is never called, all values fall back to the
#   original environment variable defaults — the viral page still works
#   exactly as before with zero changes.
#
# TENANT-AWARE FIELDS:
#   FB_PAGE_ID        — which Facebook page to post to
#   FB_PAGE_TOKEN     — that page's access token
#   ASSETS_FILE       — tenant-isolated asset registry
#   METRICS_CACHE_FILE— tenant-isolated metrics cache
# =============================================================================

import json
import os
from copy import deepcopy
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Tuple

import requests

# ============================================================
# MODULE-LEVEL GLOBALS — overridden by configure_tenant()
# ============================================================
GRAPH_VERSION        = os.getenv("GRAPH_VERSION", "v22.0")
FB_PAGE_ID           = os.getenv("FB_PAGE_ID", "")
FB_PAGE_TOKEN        = os.getenv("FB_PAGE_TOKEN", "")
ASSETS_FILE          = os.getenv("ASSETS_FILE", "assets.json")
METRICS_CACHE_FILE   = os.getenv("METRICS_CACHE_FILE", "metrics_cache.json")

# ============================================================
# TENANT CONFIGURATION INJECTOR
# ============================================================
def configure_tenant(cfg: dict) -> None:
    """
    Call this once at the top of any script with the loaded tenant config dict.
    Injects the correct FB credentials and file paths for the active tenant.

    Example (in scraper.py):
        from config_loader import load_config
        import meta_shared
        cfg = load_config()
        meta_shared.configure_tenant(cfg)

    The cfg dict must contain at minimum:
        fb_page_id_env   : str  — name of the env var holding the page ID
        fb_page_token_env: str  — name of the env var holding the token
        pipeline.assets_file        : str  — path to this tenant's assets.json
        pipeline.metrics_cache_file : str  — path to this tenant's metrics_cache.json
    """
    global FB_PAGE_ID, FB_PAGE_TOKEN, ASSETS_FILE, METRICS_CACHE_FILE

    # Resolve credentials from the env var names stored in the config
    page_id_env    = cfg.get("fb_page_id_env", "FB_PAGE_ID")
    page_token_env = cfg.get("fb_page_token_env", "FB_PAGE_TOKEN")

    FB_PAGE_ID    = os.getenv(page_id_env, "") or os.getenv("FB_PAGE_ID", "")
    FB_PAGE_TOKEN = os.getenv(page_token_env, "") or os.getenv("FB_PAGE_TOKEN", "")

    # Resolve tenant-specific state files
    pipeline_cfg        = cfg.get("pipeline", {})
    ASSETS_FILE         = pipeline_cfg.get("assets_file", ASSETS_FILE)
    METRICS_CACHE_FILE  = pipeline_cfg.get("metrics_cache_file", METRICS_CACHE_FILE)

    print(f"[meta_shared] Tenant configured → Page ID suffix: ...{FB_PAGE_ID[-6:] if FB_PAGE_ID else 'NOT SET'}")
    print(f"[meta_shared] Assets file  : {ASSETS_FILE}")
    print(f"[meta_shared] Metrics file : {METRICS_CACHE_FILE}")


# ============================================================
# JSON FILE HELPERS
# ============================================================
def _utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()

def _atomic_write_json(path: str, data: Any) -> None:
    directory = os.path.dirname(path) or "."
    os.makedirs(directory, exist_ok=True)
    tmp_path = f"{path}.tmp"
    with open(tmp_path, "w", encoding="utf-8") as f:
        json.dump(data, f, indent=2, ensure_ascii=False)
    os.replace(tmp_path, path)

def load_json_file(path: str, default: Any) -> Any:
    if not os.path.exists(path):
        return default
    try:
        with open(path, "r", encoding="utf-8") as f:
            return json.load(f)
    except Exception:
        return default

def save_json_file(path: str, data: Any) -> None:
    _atomic_write_json(path, data)

def ensure_data_files() -> None:
    if not os.path.exists(ASSETS_FILE):
        save_json_file(ASSETS_FILE, _default_assets())
    if not os.path.exists(METRICS_CACHE_FILE):
        save_json_file(METRICS_CACHE_FILE, {})


# ============================================================
# TEXT LIST HELPERS
# ============================================================
def load_lines(path: str) -> List[str]:
    if not os.path.exists(path):
        return []
    try:
        with open(path, "r", encoding="utf-8") as f:
            return [line.strip() for line in f if line.strip()]
    except Exception:
        return []

def append_unique_line(path: str, value: str) -> bool:
    value = str(value).strip()
    if not value:
        return False
    existing = set(load_lines(path))
    if value in existing:
        return False
    with open(path, "a", encoding="utf-8") as f:
        f.write(f"{value}\n")
    return True


# ============================================================
# DICT MERGE HELPERS
# ============================================================
def _deep_merge_dict(base: Dict[str, Any], patch: Dict[str, Any]) -> Dict[str, Any]:
    result = deepcopy(base)
    for key, value in patch.items():
        if key in result and isinstance(result[key], dict) and isinstance(value, dict):
            result[key] = _deep_merge_dict(result[key], value)
        else:
            result[key] = deepcopy(value)
    return result


# ============================================================
# ASSET REGISTRY
# ============================================================
def _default_assets() -> Dict[str, List[Dict[str, Any]]]:
    return {"reels": [], "news": []}

def load_assets() -> Dict[str, List[Dict[str, Any]]]:
    assets = load_json_file(ASSETS_FILE, _default_assets())
    if not isinstance(assets, dict):
        return _default_assets()
    assets.setdefault("reels", [])
    assets.setdefault("news", [])
    if not isinstance(assets["reels"], list):
        assets["reels"] = []
    if not isinstance(assets["news"], list):
        assets["news"] = []
    return assets

def save_assets(assets: Dict[str, List[Dict[str, Any]]]) -> None:
    normalized = {
        "reels": assets.get("reels", []),
        "news":  assets.get("news", []),
    }
    save_json_file(ASSETS_FILE, normalized)

def register_asset(
    asset_type: str,
    asset_id: str,
    *,
    source: str = "",
    title: str = "",
    url: str = "",
    extra: Optional[Dict[str, Any]] = None,
) -> bool:
    asset_type = asset_type.strip().lower()
    if asset_type not in {"reels", "news"}:
        raise ValueError("asset_type must be 'reels' or 'news'")

    asset_id = str(asset_id).strip()
    if not asset_id:
        return False

    assets = load_assets()

    # Remove from both buckets to avoid duplicate registration
    for bucket in ("reels", "news"):
        assets[bucket] = [
            item for item in assets[bucket]
            if str(item.get("id", "")).strip() != asset_id
        ]

    record: Dict[str, Any] = {
        "id":         asset_id,
        "type":       asset_type,
        "source":     source,
        "title":      title,
        "url":        url,
        "created_at": _utc_now_iso(),
    }
    if extra:
        record["extra"] = extra

    assets[asset_type].append(record)
    save_assets(assets)
    return True

def get_asset_index() -> Dict[str, Dict[str, Any]]:
    assets = load_assets()
    index: Dict[str, Dict[str, Any]] = {}
    for bucket in ("reels", "news"):
        for item in assets.get(bucket, []):
            item_id = str(item.get("id", "")).strip()
            if item_id:
                index[item_id] = item
    return index


# ============================================================
# METRICS CACHE
# ============================================================
def load_metrics_cache() -> Dict[str, Any]:
    cache = load_json_file(METRICS_CACHE_FILE, {})
    return cache if isinstance(cache, dict) else {}

def save_metrics_cache(cache: Dict[str, Any]) -> None:
    save_json_file(METRICS_CACHE_FILE, cache)

def get_cached_metrics(
    asset_id: str,
    ttl_seconds: Optional[int] = None,
) -> Optional[Dict[str, Any]]:
    asset_id = str(asset_id).strip()
    if not asset_id:
        return None

    cache = load_metrics_cache()
    entry = cache.get(asset_id)
    if not isinstance(entry, dict):
        return None

    if ttl_seconds is not None:
        cached_at = entry.get("cached_at")
        if not cached_at:
            return None
        try:
            cached_dt = datetime.fromisoformat(cached_at.replace("Z", "+00:00"))
            age = (datetime.now(timezone.utc) - cached_dt).total_seconds()
            if age > ttl_seconds:
                return None
        except Exception:
            return None

    return entry

def set_cached_metrics(
    asset_id: str,
    *,
    kind: str,
    metrics: Dict[str, Any],
    metadata: Optional[Dict[str, Any]] = None,
) -> None:
    asset_id = str(asset_id).strip()
    if not asset_id:
        return

    cache = load_metrics_cache()
    current = cache.get(asset_id, {})
    if not isinstance(current, dict):
        current = {}

    merged = dict(current)
    merged["id"]       = asset_id
    merged["kind"]     = kind
    merged["metrics"]  = deepcopy(metrics)
    merged["metadata"] = deepcopy(metadata or {})
    merged["cached_at"] = _utc_now_iso()

    cache[asset_id] = merged
    save_metrics_cache(cache)

def update_cached_metrics(asset_id: str, patch: Dict[str, Any]) -> None:
    asset_id = str(asset_id).strip()
    if not asset_id:
        return

    cache = load_metrics_cache()
    entry = cache.get(asset_id, {})
    if not isinstance(entry, dict):
        entry = {}

    merged = dict(entry)
    patch = dict(patch or {})

    for key, value in patch.items():
        if key in merged and isinstance(merged[key], dict) and isinstance(value, dict):
            merged[key] = _deep_merge_dict(merged[key], value)
        else:
            merged[key] = deepcopy(value)

    merged.setdefault("id", asset_id)
    merged["cached_at"] = _utc_now_iso()

    cache[asset_id] = merged
    save_metrics_cache(cache)


# ============================================================
# GRAPH API HELPERS
# ============================================================
def _auth_error() -> Dict[str, Dict[str, str]]:
    return {"error": {"message": "FB_PAGE_TOKEN is not set"}}

def graph_get(
    path: str,
    params: Optional[Dict[str, Any]] = None,
) -> Tuple[Optional[Dict[str, Any]], Optional[Dict[str, Any]]]:
    if not FB_PAGE_TOKEN:
        return None, _auth_error()

    params = dict(params or {})
    params["access_token"] = FB_PAGE_TOKEN

    url = f"https://graph.facebook.com/{GRAPH_VERSION}/{path.lstrip('/')}"
    try:
        response = requests.get(url, params=params, timeout=20)
        data = response.json()
        if response.status_code >= 400:
            return None, data if isinstance(data, dict) else {"error": {"message": "Request failed"}}
        if isinstance(data, dict) and data.get("error"):
            return None, data
        return data if isinstance(data, dict) else None, None
    except Exception as e:
        return None, {"error": {"message": str(e)}}

def graph_post(
    path: str,
    data: Optional[Dict[str, Any]] = None,
    timeout: int = 20,
) -> Tuple[Optional[Dict[str, Any]], Optional[Dict[str, Any]]]:
    if not FB_PAGE_TOKEN:
        return None, _auth_error()

    payload = dict(data or {})
    payload["access_token"] = FB_PAGE_TOKEN

    url = f"https://graph.facebook.com/{GRAPH_VERSION}/{path.lstrip('/')}"
    try:
        response = requests.post(url, data=payload, timeout=timeout)
        resp_data = response.json()
        if response.status_code >= 400:
            return None, resp_data if isinstance(resp_data, dict) else {"error": {"message": "Request failed"}}
        if isinstance(resp_data, dict) and resp_data.get("error"):
            return None, resp_data
        return resp_data if isinstance(resp_data, dict) else None, None
    except Exception as e:
        return None, {"error": {"message": str(e)}}

def graph_batch_get(
    requests_list: List[Dict[str, str]],
) -> Tuple[Optional[List[Dict[str, Any]]], Optional[Dict[str, Any]]]:
    if not FB_PAGE_TOKEN:
        return None, _auth_error()
    if not requests_list:
        return [], None

    batch_payload = {
        "access_token": FB_PAGE_TOKEN,
        "batch": json.dumps(requests_list),
    }

    try:
        url = f"https://graph.facebook.com/{GRAPH_VERSION}/"
        response = requests.post(url, data=batch_payload, timeout=30)
        data = response.json()
        if response.status_code >= 400:
            return None, data if isinstance(data, dict) else {"error": {"message": "Batch failed"}}
        return data if isinstance(data, list) else None, None
    except Exception as e:
        return None, {"error": {"message": str(e)}}

def paginate(
    path: str,
    params: Optional[Dict[str, Any]] = None,
    max_pages: int = 50,
) -> List[Dict[str, Any]]:
    params = dict(params or {})
    data, err = graph_get(path, params=params)
    if err or not data:
        return []

    items    = data.get("data", []) if isinstance(data, dict) else []
    next_url = data.get("paging", {}).get("next") if isinstance(data, dict) else None
    pages    = 1

    while next_url and pages < max_pages:
        try:
            response = requests.get(next_url, timeout=20)
            page_data = response.json()
            items.extend(page_data.get("data", []))
            next_url = page_data.get("paging", {}).get("next")
            pages += 1
        except Exception as e:
            print(f"[-] Pagination interrupted: {e}")
            break

    return items


# ============================================================
# COUNT HELPERS
# ============================================================
def safe_int(value: Any, default: int = 0) -> int:
    try:
        return default if value is None else int(value)
    except Exception:
        return default

def extract_count_from_summary(field_value: Any) -> int:
    if isinstance(field_value, dict):
        if "summary" in field_value and isinstance(field_value["summary"], dict):
            return safe_int(field_value["summary"].get("total_count", 0))
        for key in ("count", "total_count", "value"):
            if key in field_value:
                return safe_int(field_value[key])
    if isinstance(field_value, (int, float)):
        return int(field_value)
    return 0

def extract_shares(field_value: Any) -> int:
    if isinstance(field_value, dict):
        for key in ("count", "share_count"):
            if key in field_value:
                return safe_int(field_value[key])
    if isinstance(field_value, (int, float)):
        return int(field_value)
    return 0


# ============================================================
# BOOT — ensure tenant files exist when module loads
# ============================================================
ensure_data_files()


Configloader

# =============================================================================
# config_loader.py
# CONCRETE HORIZONS — TENANT CONFIG LOADER
# =============================================================================
# Reads a YAML config file and returns a clean dict every engine can use.
# Called at the top of scraper.py, pipeline.py, news_engine.py,
# cache_updater.py, analytics.py, and import_existing_assets.py.
#
# USAGE IN ANY SCRIPT:
#   import argparse
#   from config_loader import load_config
#   import meta_shared
#
#   parser = argparse.ArgumentParser()
#   parser.add_argument("--config", default="config/viral.yml")
#   args = parser.parse_args()
#
#   cfg = load_config(args.config)
#   meta_shared.configure_tenant(cfg)
#
# CONFIG FILES:
#   config/viral.yml    — Viral page identity, handles, RSS feeds, file paths
#   config/producer.yml — Producer page identity, handles, RSS feeds, file paths
# =============================================================================

import os
import sys

try:
    import yaml
except ImportError:
    print("[FATAL] PyYAML not installed. Run: pip install pyyaml")
    sys.exit(1)


# ============================================================
# LOADER
# ============================================================
def load_config(config_path: str = "config/viral.yml") -> dict:
    """
    Loads and validates a tenant YAML config file.
    Returns a flat/nested dict the engines read from.
    Exits with a clear error if the file is missing or malformed.
    """
    if not os.path.exists(config_path):
        print(f"[FATAL] Config file not found: {config_path}")
        print("        Expected: config/viral.yml or config/producer.yml")
        sys.exit(1)

    try:
        with open(config_path, "r", encoding="utf-8") as f:
            cfg = yaml.safe_load(f)
    except Exception as e:
        print(f"[FATAL] Failed to parse config file {config_path}: {e}")
        sys.exit(1)

    if not isinstance(cfg, dict):
        print(f"[FATAL] Config file {config_path} is empty or not a valid YAML map.")
        sys.exit(1)

    _validate_config(cfg, config_path)

    tenant = cfg.get("tenant", "unknown")
    print(f"[config_loader] Loaded tenant: {tenant} from {config_path}")
    return cfg


# ============================================================
# VALIDATION
# ============================================================
REQUIRED_KEYS = [
    "tenant",
    "page_name",
    "fb_page_id_env",
    "fb_page_token_env",
]

def _validate_config(cfg: dict, path: str) -> None:
    for key in REQUIRED_KEYS:
        if key not in cfg:
            print(f"[FATAL] Config {path} is missing required key: '{key}'")
            sys.exit(1)


# ============================================================
# SECTION HELPERS — safe accessors with defaults
# ============================================================
def get_scraper_config(cfg: dict) -> dict:
    return cfg.get("scraper", {})

def get_pipeline_config(cfg: dict) -> dict:
    return cfg.get("pipeline", {})

def get_news_config(cfg: dict) -> dict:
    return cfg.get("news", {})

def get_analytics_config(cfg: dict) -> dict:
    return cfg.get("analytics", {})


# ============================================================
# BUILT-IN CONFIG GENERATOR
# Writes default viral.yml and producer.yml if they don't exist.
# Run: python config_loader.py --init
# ============================================================
VIRAL_YML = """\
# =============================================================================
# config/viral.yml
# CONCRETE HORIZONS — VIRAL PAGE TENANT CONFIG
# =============================================================================

tenant: viral
page_name: "Concrete Horizons"

# GitHub Secrets env var names for this page's credentials
fb_page_id_env:    "FB_PAGE_ID"
fb_page_token_env: "FB_PAGE_TOKEN"

# ---------------------------------------------------------------------------
scraper:
  output_dir:          "downloaded_videos"
  queue_file:          "queue.txt"
  history_file:        "scraper_history.txt"
  daily_limit:         1
  niche_handles:
    sports:
      - HumansNoContext
      - NextSkills
      - GymMotivation
      - TheBest_Viral
      - YepViralVideos
      - ViralHog
    satisfying:
      - SatisfyingVids
      - NatureIsLit
      - OddlySatisfying
      - CraftyThings
      - RestorationClips
    funny:
      - Fun_Viral_Vids
      - FunnyByteVids
      - aww
      - Pubity
      - FailArmy
      - Memezar
      - CrazyClips
      - ViralHog2

# ---------------------------------------------------------------------------
pipeline:
  queue_file:          "queue.txt"
  history_file:        "history.txt"
  watermark_text:      "CONCRETE HORIZONS"
  assets_file:         "assets.json"
  metrics_cache_file:  "metrics_cache.json"
  page_name:           "Concrete Horizons"
  caption_style:       "viral"

# ---------------------------------------------------------------------------
news:
  history_file:        "news_history.txt"
  assets_file:         "assets.json"
  metrics_cache_file:  "metrics_cache.json"
  page_name:           "Concrete Horizons"
  caption_style:       "global news, objective, informative"
  max_posts_per_run:   1
  feeds:
    - "http://feeds.bbci.co.uk/news/world/rss.xml"
    - "https://rss.dw.com/xml/rss-dw-top-stories"
    - "https://www.cbc.ca/cmlink/rss-world"
    - "http://feeds.abcnews.com/abcnews/usheadlines"

# ---------------------------------------------------------------------------
analytics:
  history_file:        "analytics_history.csv"
  assets_file:         "assets.json"
  metrics_cache_file:  "metrics_cache.json"
  report_title:        "CONCRETE HORIZONS ANALYTICS"
"""

PRODUCER_YML = """\
# =============================================================================
# config/producer.yml
# CONCRETE HORIZONS — PRODUCER PAGE TENANT CONFIG
# =============================================================================

tenant: producer
page_name: "Concrete Horizons Producers"

# GitHub Secrets env var names for this page's credentials
# Add these secrets to your GitHub repo:
#   FB_PRODUCER_PAGE_ID    — your producer Facebook page ID
#   FB_PRODUCER_PAGE_TOKEN — your producer page access token
fb_page_id_env:    "FB_PRODUCER_PAGE_ID"
fb_page_token_env: "FB_PRODUCER_PAGE_TOKEN"

# ---------------------------------------------------------------------------
scraper:
  output_dir:          "downloaded_videos_producer"
  queue_file:          "queue_producer.txt"
  history_file:        "scraper_history_producer.txt"
  daily_limit:         1
  niche_handles:
    production:
      - ILLENIUM
      - deadmau5
      - BleepsMachine
      - FL_Studio
      - AbletonLive
      - SpliceHQ
    beats:
      - BeatsByDre
      - OfficialTrackLib
      - BeatStarsOfficial
      - ProducerHustle
      - TheProducerMindset
    audio_engineering:
      - iZotopeInc
      - FabFilterBV
      - Plugin_Boutique
      - KVRaudio
      - MusicRadarNews

# ---------------------------------------------------------------------------
pipeline:
  queue_file:          "queue_producer.txt"
  history_file:        "history_producer.txt"
  watermark_text:      "CONCRETE HORIZONS"
  assets_file:         "assets_producer.json"
  metrics_cache_file:  "metrics_cache_producer.json"
  page_name:           "Concrete Horizons Producers"
  caption_style:       "producer"

# ---------------------------------------------------------------------------
news:
  history_file:        "news_history_producer.txt"
  assets_file:         "assets_producer.json"
  metrics_cache_file:  "metrics_cache_producer.json"
  page_name:           "Concrete Horizons Producers"
  caption_style:       "music production, beats, audio engineering, industry news"
  max_posts_per_run:   1
  feeds:
    - "https://www.musicradar.com/feeds/all"
    - "https://cdm.link/feed/"
    - "https://www.kvraudio.com/news.php?mode=rss"
    - "https://www.hypebot.com/feed"
    - "https://www.musicbusinessworldwide.com/feed/"

# ---------------------------------------------------------------------------
analytics:
  history_file:        "analytics_history_producer.csv"
  assets_file:         "assets_producer.json"
  metrics_cache_file:  "metrics_cache_producer.json"
  report_title:        "CONCRETE HORIZONS PRODUCERS ANALYTICS"
"""

def init_configs() -> None:
    """
    Writes default config files if they do not already exist.
    Run once: python config_loader.py --init
    """
    os.makedirs("config", exist_ok=True)

    viral_path    = "config/viral.yml"
    producer_path = "config/producer.yml"

    if not os.path.exists(viral_path):
        with open(viral_path, "w", encoding="utf-8") as f:
            f.write(VIRAL_YML)
        print(f"[✓] Created {viral_path}")
    else:
        print(f"[~] Already exists: {viral_path}")

    if not os.path.exists(producer_path):
        with open(producer_path, "w", encoding="utf-8") as f:
            f.write(PRODUCER_YML)
        print(f"[✓] Created {producer_path}")
    else:
        print(f"[~] Already exists: {producer_path}")

    print("\n[DONE] Config files ready. Edit them before deploying.")


# ============================================================
# STANDALONE INIT
# ============================================================
if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser(description="Concrete Horizons Config Loader")
    parser.add_argument(
        "--init",
        action="store_true",
        help="Write default viral.yml and producer.yml into the config/ directory",
    )
    args = parser.parse_args()

    if args.init:
        init_configs()
    else:
        parser.print_help()
