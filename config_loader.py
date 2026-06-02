
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
