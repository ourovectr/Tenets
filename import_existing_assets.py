

Import_existing_assets.py 

# =============================================================================
# import_existing_assets.py
# CONCRETE HORIZONS — MULTI-TENANT HISTORICAL ASSET IMPORTER
# =============================================================================
# One-shot script that scans the Facebook page for all existing reels and
# news posts, registers them in assets.json, and seeds metrics_cache.json
# with zero-baseline metrics so the cache updater can fill them in.
#
# Run this once when onboarding a new tenant, or to re-sync a page that
# has posts that predate the engine.
#
# USAGE:
#   python import_existing_assets.py --config config/viral.yml
#   python import_existing_assets.py --config config/producer.yml
#
# GITHUB ACTIONS:
#   Triggered manually via workflow_dispatch only.
#   Pass --config config/viral.yml    in import_existing_assets.yml
#   Pass --config config/producer.yml in producer_import_existing_assets.yml
#
# SECRETS REQUIRED (viral):
#   FB_PAGE_ID, FB_PAGE_TOKEN
#
# SECRETS REQUIRED (producer):
#   FB_PRODUCER_PAGE_ID, FB_PRODUCER_PAGE_TOKEN
# =============================================================================

import argparse
from typing import Any, Dict

from config_loader import load_config
import meta_shared
from meta_shared import (
    get_asset_index,
    load_metrics_cache,
    paginate,
    register_asset,
    set_cached_metrics,
    update_cached_metrics,
)

# ============================================================
# BOOT — load tenant config before anything else
# ============================================================
parser = argparse.ArgumentParser(description="Concrete Horizons Asset Importer")
parser.add_argument(
    "--config",
    default="config/viral.yml",
    help="Path to tenant YAML config (default: config/viral.yml)",
)
args = parser.parse_args()

cfg = load_config(args.config)
meta_shared.configure_tenant(cfg)

# ============================================================
# MODULE-LEVEL TENANT CONSTANTS
# ============================================================
FB_PAGE_ID    = meta_shared.FB_PAGE_ID
FB_PAGE_TOKEN = meta_shared.FB_PAGE_TOKEN
TENANT        = cfg.get("tenant", "viral")
PAGE_NAME     = cfg.get("page_name", "Concrete Horizons")

print(f"[import] Tenant    : {TENANT}")
print(f"[import] Page name : {PAGE_NAME}")
print(f"[import] Page ID   : ...{FB_PAGE_ID[-6:] if FB_PAGE_ID else 'NOT SET'}")

# ============================================================
# TOKEN GUARD
# ============================================================
if not FB_PAGE_TOKEN:
    print(f"[FATAL] FB page token is missing for tenant '{TENANT}'.")
    print(f"        Set the correct GitHub secret and rerun.")
    raise SystemExit(1)

# =====================================================================
# HELPERS
# =====================================================================
def ensure_asset_and_metrics(
    asset_bucket: str,
    asset_id: str,
    title: str,
    url: str,
    kind: str,
) -> None:
    """
    Keeps assets.json and metrics_cache.json aligned without overwriting
    existing real metrics.
    """
    asset_bucket = asset_bucket.strip().lower()
    kind         = kind.strip().lower()

    existing_assets = get_asset_index()
    current_asset   = existing_assets.get(asset_id)

    # Register only when missing or placed in the wrong bucket.
    if not current_asset or current_asset.get("type") != asset_bucket:
        register_asset(
            asset_bucket,
            asset_id,
            source="import",
            title=title,
            url=url,
        )

    cache = load_metrics_cache()
    entry = cache.get(asset_id)

    if not isinstance(entry, dict):
        set_cached_metrics(
            asset_id,
            kind=kind,
            metrics={
                "reactions":   0,
                "comments":    0,
                "shares":      0,
                "views":       None,
                "engagements": 0,
            },
            metadata={
                "source": "import",
                "title":  title,
                "url":    url,
                "tenant": TENANT,
            },
        )
        return

    patch: Dict[str, Any] = {}
    if entry.get("kind") != kind:
        patch["kind"] = kind

    metadata = entry.get("metadata", {})
    if not isinstance(metadata, dict):
        metadata = {}
    metadata.update({
        "source": "import",
        "title":  title,
        "url":    url,
        "tenant": TENANT,
    })
    patch["metadata"] = metadata

    metrics = entry.get("metrics", {})
    if not isinstance(metrics, dict) or not metrics:
        patch["metrics"] = {
            "reactions":   0,
            "comments":    0,
            "shares":      0,
            "views":       None,
            "engagements": 0,
        }

    if patch:
        update_cached_metrics(asset_id, patch)


# =====================================================================
# IMPORTERS
# =====================================================================
def import_reels():
    print(f"[IMPORT] Scanning existing reels [{TENANT}]...")

    reels = paginate(
        f"{FB_PAGE_ID}/video_reels",
        params={"fields": "id,created_time,description,permalink_url", "limit": 100},
    )

    count = 0
    for item in reels:
        item_id = item.get("id")
        if not item_id:
            continue

        title = (item.get("description") or "Imported Reel")[:80]
        url   = item.get("permalink_url", f"https://facebook.com/{item_id}")

        ensure_asset_and_metrics(
            asset_bucket="reels",
            asset_id=item_id,
            title=title,
            url=url,
            kind="reel",
        )
        count += 1

    print(f"[IMPORT] Registered {count} reel(s) into assets.json [{TENANT}]")


def import_news_posts():
    print(f"[IMPORT] Scanning existing news posts [{TENANT}]...")

    posts = paginate(
        f"{FB_PAGE_ID}/posts",
        params={"fields": "id,created_time,message,permalink_url", "limit": 100},
    )

    count = 0
    for item in posts:
        item_id = item.get("id")
        if not item_id:
            continue

        title = (item.get("message") or "Imported Post")[:80]
        url   = item.get("permalink_url", f"https://facebook.com/{item_id}")

        ensure_asset_and_metrics(
            asset_bucket="news",
            asset_id=item_id,
            title=title,
            url=url,
            kind="news",
        )
        count += 1

    print(f"[IMPORT] Registered {count} post(s) into assets.json [{TENANT}]")


# =====================================================================
# MAIN
# =====================================================================
if __name__ == "__main__":
    print("=" * 55)
    print(f"  {PAGE_NAME.upper()} — ASSET IMPORTER")
    print("=" * 55)

    import_reels()
    import_news_posts()

    print(
        f"\n[DONE] Import complete [{TENANT}]. "
        f"assets.json and metrics_cache.json are now populated."
    )


