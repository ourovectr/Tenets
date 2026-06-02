# =============================================================================
# cache_updater.py
# CONCRETE HORIZONS — MULTI-TENANT BATCH CACHE UPDATER
# =============================================================================
# Discovers live content from Facebook, then refreshes metrics for every
# registered asset. Tenant identity, FB credentials, and file paths are all
# loaded from the YAML config — nothing is hardcoded.
#
# USAGE:
#   python cache_updater.py --config config/viral.yml
#   python cache_updater.py --config config/producer.yml
#
# GITHUB ACTIONS:
#   Pass --config config/viral.yml    in the viral refresh workflow
#   Pass --config config/producer.yml in the producer refresh workflow
#
# SECRETS REQUIRED (viral):
#   FB_PAGE_ID, FB_PAGE_TOKEN
#
# SECRETS REQUIRED (producer):
#   FB_PRODUCER_PAGE_ID, FB_PRODUCER_PAGE_TOKEN
# =============================================================================

import argparse
import time
from typing import Any, Dict

from config_loader import load_config
import meta_shared
from meta_shared import (
    extract_count_from_summary,
    extract_shares,
    get_asset_index,
    graph_get,
    load_metrics_cache,
    paginate,
    register_asset,
    set_cached_metrics,
)

# ============================================================
# BOOT — load tenant config before anything else
# ============================================================
parser = argparse.ArgumentParser(description="Concrete Horizons Cache Updater")
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
FB_PAGE_ID = meta_shared.FB_PAGE_ID
TENANT     = cfg.get("tenant", "viral")

print(f"[cache_updater] Tenant    : {TENANT}")
print(f"[cache_updater] Page ID   : ...{FB_PAGE_ID[-6:] if FB_PAGE_ID else 'NOT SET'}")

# =====================================================================
# HELPERS
# =====================================================================
def safe_int(value: Any, default: int = 0) -> int:
    try:
        if value is None:
            return default
        return int(value)
    except Exception:
        return default


def normalise_kind(raw_type: str) -> str:
    mapping = {
        "reels": "reel",
        "news":  "news",
    }
    return mapping.get(str(raw_type).strip().lower(), str(raw_type).strip().lower())


def extract_possible_views(data_dict):
    if not isinstance(data_dict, dict):
        return None

    for key in ("video_views", "view_count", "views", "plays", "total_video_views"):
        if key in data_dict:
            val = data_dict[key]
            if isinstance(val, dict):
                return val.get("value", val.get("count", 0))
            if isinstance(val, (int, float)):
                return int(val)
    return None


def parse_action_breakdown(action_value: Any) -> Dict[str, int]:
    result = {"reactions": 0, "comments": 0, "shares": 0}
    if not isinstance(action_value, dict):
        return result

    comments = action_value.get("comment", action_value.get("comments", 0))
    shares   = action_value.get("share",   action_value.get("shares",   0))

    reactions = 0
    for key, value in action_value.items():
        if key in {"comment", "comments", "share", "shares"}:
            continue
        reactions += safe_int(value, 0)

    result["comments"]  = safe_int(comments,  0)
    result["shares"]    = safe_int(shares,    0)
    result["reactions"] = reactions
    return result


def get_existing_metrics(asset_id: str) -> Dict[str, Any]:
    cache = load_metrics_cache()
    entry = cache.get(asset_id, {})
    if not isinstance(entry, dict):
        return {}
    metrics = entry.get("metrics", {})
    return metrics if isinstance(metrics, dict) else {}


def merge_and_store_metrics(
    asset_id: str,
    *,
    kind: str,
    new_metrics: Dict[str, Any],
    metadata: Dict[str, Any],
) -> None:
    """
    Merge new metrics into the existing dict so we do not wipe out
    older values when Facebook returns partial data.
    """
    cache = load_metrics_cache()
    existing_entry = cache.get(asset_id, {})
    if not isinstance(existing_entry, dict):
        existing_entry = {}

    existing_metrics = existing_entry.get("metrics", {})
    if not isinstance(existing_metrics, dict):
        existing_metrics = {}

    existing_metadata = existing_entry.get("metadata", {})
    if not isinstance(existing_metadata, dict):
        existing_metadata = {}

    merged_metrics = dict(existing_metrics)

    # Preserve existing good values if the refresh returns None or zero fallback.
    for key, value in new_metrics.items():
        if value is None:
            continue

        if key in {"views", "unique_views", "reactions", "comments", "shares"}:
            previous = merged_metrics.get(key)
            if value == 0 and previous not in (None, 0, 0.0):
                continue

        merged_metrics[key] = value

    # Always recalculate engagements from the merged counters
    merged_metrics["engagements"] = (
        safe_int(merged_metrics.get("reactions", 0), 0)
        + safe_int(merged_metrics.get("comments",  0), 0)
        + safe_int(merged_metrics.get("shares",    0), 0)
    )

    merged_metadata = dict(existing_metadata)
    merged_metadata.update(metadata)

    set_cached_metrics(
        asset_id,
        kind=kind,
        metrics=merged_metrics,
        metadata=merged_metadata,
    )


# =====================================================================
# LIVE CONTENT DISCOVERY
# =====================================================================
def sync_live_content():
    print(f"[SYNC] Discovering live content from Facebook API [{TENANT}]...")

    discovered_reels = paginate(
        f"{FB_PAGE_ID}/video_reels",
        params={"fields": "id,created_time,permalink_url,description", "limit": 100},
    )

    discovered_posts = paginate(
        f"{FB_PAGE_ID}/posts",
        params={"fields": "id,created_time,permalink_url,message", "limit": 100},
    )

    existing_index = get_asset_index()
    seen_ids       = set(existing_index.keys())
    new_items      = 0

    for item in discovered_reels:
        item_id = item.get("id")
        if not item_id or item_id in seen_ids:
            continue

        register_asset(
            "reels",
            item_id,
            source="live_discovery",
            title=(item.get("description") or "Discovered Reel")[:80],
            url=item.get("permalink_url", f"https://facebook.com/{item_id}"),
        )
        seen_ids.add(item_id)
        new_items += 1

    for item in discovered_posts:
        item_id = item.get("id")
        if not item_id or item_id in seen_ids:
            continue

        register_asset(
            "news",
            item_id,
            source="live_discovery",
            title=(item.get("message") or "Discovered Post")[:80],
            url=item.get("permalink_url", f"https://facebook.com/{item_id}"),
        )
        seen_ids.add(item_id)
        new_items += 1

    print(f"[SYNC] {new_items} new item(s) added to registry [{TENANT}].")


# =====================================================================
# METRICS REFRESH
# =====================================================================
def refresh_reel_metrics(asset_id: str, asset_info: Dict[str, Any]) -> bool:
    """
    Reel metrics are pulled from insights first.
    The fallback request intentionally avoids unsupported fields like
    'shares' on Reel objects, which causes Graph API #100 errors.
    """
    existing_metrics = get_existing_metrics(asset_id)

    insights_data, err = graph_get(
        f"{asset_id}/video_insights",
        params={
            "metric": (
                "total_video_views,"
                "total_video_views_unique,"
                "total_video_stories_by_action_type"
            ),
        },
    )

    if err:
        print(f"  [!] Reel insights failed for {asset_id}: {err}")
        insights_data = {}

    video_views      = None
    unique_views     = None
    action_breakdown = {"reactions": 0, "comments": 0, "shares": 0}

    if insights_data and isinstance(insights_data, dict):
        action_value = None

        for item in insights_data.get("data", []) or []:
            if not isinstance(item, dict):
                continue

            metric_name  = item.get("name")
            values       = item.get("values", []) or []
            first        = values[0] if values else None
            metric_value = first.get("value") if isinstance(first, dict) else first

            if metric_name == "total_video_views":
                video_views = metric_value
            elif metric_name == "total_video_views_unique":
                unique_views = metric_value
            elif metric_name == "total_video_stories_by_action_type":
                action_value = metric_value

        action_breakdown = parse_action_breakdown(action_value)

    fallback_data, fallback_err = graph_get(
        asset_id,
        params={
            "fields": (
                "id,created_time,permalink_url,description,"
                "comments.summary(true)"
            )
        },
    )

    if fallback_err:
        print(f"  [!] Reel fallback fetch failed for {asset_id}: {fallback_err}")

    fallback_data = fallback_data or {}
    if not isinstance(fallback_data, dict):
        fallback_data = {}

    comments = extract_count_from_summary(fallback_data.get("comments"))

    # Do not query 'shares' on reels — it causes Graph API errors.
    # Preserve existing values if insights do not return them.
    shares = action_breakdown["shares"]
    if not shares:
        shares = safe_int(existing_metrics.get("shares", 0), 0)

    reactions = action_breakdown["reactions"]
    if not reactions:
        reactions = safe_int(existing_metrics.get("reactions", 0), 0)

    if not comments:
        comments = safe_int(existing_metrics.get("comments", 0), 0)

    views = video_views if video_views is not None else extract_possible_views(fallback_data)
    if views is None:
        views = existing_metrics.get("views", None)

    if unique_views is None:
        unique_views = existing_metrics.get("unique_views", None)

    merge_and_store_metrics(
        asset_id,
        kind="reel",
        new_metrics={
            "views":        views,
            "unique_views": unique_views,
            "reactions":    reactions,
            "comments":     comments,
            "shares":       shares,
        },
        metadata={
            "source": asset_info.get("source", ""),
            "title":  asset_info.get("title",  ""),
            "url":    asset_info.get("url",    ""),
            "tenant": TENANT,
        },
    )
    return True


def refresh_news_metrics(asset_id: str, asset_info: Dict[str, Any]) -> bool:
    existing_metrics = get_existing_metrics(asset_id)

    data, err = graph_get(
        asset_id,
        params={
            "fields": (
                "id,created_time,permalink_url,message,"
                "comments.summary(true),reactions.summary(true),shares"
            )
        },
    )

    if err or not data:
        print(f"  [!] News asset failed: {asset_id} -> {err}")
        return False

    if not isinstance(data, dict):
        return False

    reactions = extract_count_from_summary(data.get("reactions"))
    comments  = extract_count_from_summary(data.get("comments"))
    shares    = extract_shares(data.get("shares"))

    if not reactions:
        reactions = safe_int(existing_metrics.get("reactions", 0), 0)
    if not comments:
        comments  = safe_int(existing_metrics.get("comments",  0), 0)
    if not shares:
        shares    = safe_int(existing_metrics.get("shares",    0), 0)

    merge_and_store_metrics(
        asset_id,
        kind="news",
        new_metrics={
            "views":     None,
            "reactions": reactions,
            "comments":  comments,
            "shares":    shares,
        },
        metadata={
            "source": asset_info.get("source", ""),
            "title":  asset_info.get("title",  ""),
            "url":    asset_info.get("url",    ""),
            "tenant": TENANT,
        },
    )
    return True


def refresh_all_metrics():
    print("=" * 55)
    print(f"  {cfg.get('page_name', 'CONCRETE HORIZONS').upper()} — BATCH CACHE UPDATER")
    print("=" * 55)

    sync_live_content()

    asset_index = get_asset_index()
    all_ids     = list(asset_index.keys())

    if not all_ids:
        print(f"[-] No assets in registry [{TENANT}]. Exiting.")
        return

    print(f"[METRICS] Fetching data for {len(all_ids)} asset(s) [{TENANT}]...")
    updated_count = 0

    for i, asset_id in enumerate(all_ids, 1):
        asset_info = asset_index.get(asset_id, {})
        asset_kind = normalise_kind(asset_info.get("type", "news"))

        print(f"  [→] {i}/{len(all_ids)} {asset_kind}: {asset_id}")

        try:
            if asset_kind == "reel":
                ok = refresh_reel_metrics(asset_id, asset_info)
            else:
                ok = refresh_news_metrics(asset_id, asset_info)

            if ok:
                updated_count += 1
        except Exception as e:
            print(f"  [-] Failed to process {asset_id}: {e}")

        time.sleep(1)

    print(f"\n[✓] Cache refresh complete [{TENANT}]. Updated {updated_count} item(s).")


# =====================================================================
# MAIN
# =====================================================================
if __name__ == "__main__":
    refresh_all_metrics()































Analytics.py 
# =============================================================================
# analytics.py
# CONCRETE HORIZONS — MULTI-TENANT ANALYTICS ENGINE
# =============================================================================
# Compiles a full performance report from the metrics cache, saves a CSV
# snapshot, and sends the formatted report to Telegram.
# Report title, history file, and page identity are all loaded from the
# tenant YAML config — nothing is hardcoded.
#
# USAGE:
#   python analytics.py --config config/viral.yml
#   python analytics.py --config config/producer.yml
#
# GITHUB ACTIONS:
#   Pass --config config/viral.yml    in the viral analytics workflow
#   Pass --config config/producer.yml in the producer analytics workflow
#
# SECRETS REQUIRED (viral):
#   FB_PAGE_ID, FB_PAGE_TOKEN, TG_BOT_TOKEN, TG_CHAT_ID
#
# SECRETS REQUIRED (producer):
#   FB_PRODUCER_PAGE_ID, FB_PRODUCER_PAGE_TOKEN, TG_BOT_TOKEN, TG_CHAT_ID
# =============================================================================

import argparse
import csv
import html
import os
import requests
from datetime import datetime, timezone

from config_loader import load_config, get_analytics_config
import meta_shared
from meta_shared import load_assets, load_metrics_cache

# ============================================================
# BOOT — load tenant config before anything else
# ============================================================
parser = argparse.ArgumentParser(description="Concrete Horizons Analytics")
parser.add_argument(
    "--config",
    default="config/viral.yml",
    help="Path to tenant YAML config (default: config/viral.yml)",
)
args = parser.parse_args()

cfg           = load_config(args.config)
meta_shared.configure_tenant(cfg)
analytics_cfg = get_analytics_config(cfg)

# ============================================================
# MODULE-LEVEL TENANT CONSTANTS
# ============================================================
TENANT                 = cfg.get("tenant", "viral")
PAGE_NAME              = cfg.get("page_name", "Concrete Horizons")
ANALYTICS_HISTORY_FILE = analytics_cfg.get("history_file",  "analytics_history.csv")
REPORT_TITLE           = analytics_cfg.get("report_title",  "CONCRETE HORIZONS ANALYTICS")

TG_BOT_TOKEN = os.getenv("TG_BOT_TOKEN", "")
TG_CHAT_ID   = os.getenv("TG_CHAT_ID",   "")

print(f"[analytics] Tenant       : {TENANT}")
print(f"[analytics] Page name    : {PAGE_NAME}")
print(f"[analytics] History file : {ANALYTICS_HISTORY_FILE}")
print(f"[analytics] Report title : {REPORT_TITLE}")

# =====================================================================
# TELEGRAM
# =====================================================================
def send_telegram_update(message):
    if not TG_BOT_TOKEN or not TG_CHAT_ID:
        print("[!] Telegram credentials not set — skipping notification.")
        return

    url     = f"https://api.telegram.org/bot{TG_BOT_TOKEN}/sendMessage"
    payload = {"chat_id": TG_CHAT_ID, "text": message, "parse_mode": "HTML"}

    try:
        response = requests.post(url, json=payload, timeout=15)
        if response.status_code >= 400:
            print(f"[-] Telegram API error {response.status_code}: {response.text}")
    except Exception as e:
        print(f"[-] Telegram send failed: {e}")


# =====================================================================
# HELPERS
# =====================================================================
def safe_int(value, default=0):
    try:
        if value is None:
            return default
        return int(value)
    except Exception:
        return default


def normalize_kind(kind_value: str) -> str:
    kind = str(kind_value or "").strip().lower()
    if kind in {"reels", "reel"}:
        return "reel"
    if kind in {"news"}:
        return "news"
    if kind.endswith("s"):
        return kind[:-1]
    return kind


def build_asset_registry_index():
    """
    Source of truth for classification.
    assets.json decides whether an ID is a reel or news.
    """
    assets = load_assets()
    index  = {}

    for bucket in ("reels", "news"):
        items = assets.get(bucket, [])
        if not isinstance(items, list):
            continue

        for item in items:
            if not isinstance(item, dict):
                continue

            item_id = str(item.get("id", "")).strip()
            if item_id:
                index[item_id] = bucket

    return index


def extract_reel_views(metrics: dict):
    """
    Only treat views as reel views when they belong to a reel row.
    """
    for key in ("views", "video_views", "view_count", "plays", "total_video_views"):
        value = metrics.get(key)
        if value is not None:
            return safe_int(value, 0)
    return None


def pick_kind(asset_id: str, cache_kind: str, registry_index: dict) -> str:
    """
    Prefer assets.json classification.
    Fall back to cache kind if the asset is missing from registry.
    """
    registry_kind = registry_index.get(asset_id)
    if registry_kind:
        return normalize_kind(registry_kind)
    return normalize_kind(cache_kind)


# =====================================================================
# REPORT BUILDER
# =====================================================================
def build_report():
    print(f"[ANALYTICS] Compiling report from metrics cache [{TENANT}]...")
    cache          = load_metrics_cache()
    registry_index = build_asset_registry_index()

    reel_rows = []
    news_rows = []

    for asset_id, data in cache.items():
        if not isinstance(data, dict):
            continue

        metrics = data.get("metrics", {})
        if not isinstance(metrics, dict):
            metrics = {}

        kind = pick_kind(asset_id, data.get("kind", ""), registry_index)

        item = {
            "id":          asset_id,
            "reactions":   safe_int(metrics.get("reactions",   0), 0),
            "comments":    safe_int(metrics.get("comments",    0), 0),
            "shares":      safe_int(metrics.get("shares",      0), 0),
            "engagements": safe_int(metrics.get("engagements", 0), 0),
            "views":       None,
        }

        if kind == "reel":
            item["views"] = extract_reel_views(metrics)
            reel_rows.append(item)
        elif kind == "news":
            news_rows.append(item)

    # ── Reel aggregates ──────────────────────────────────────────────
    total_reels          = len(reel_rows)
    total_reel_reactions = sum(r["reactions"] for r in reel_rows)
    total_reel_comments  = sum(r["comments"]  for r in reel_rows)
    total_reel_shares    = sum(r["shares"]    for r in reel_rows)

    reel_view_items       = [r["views"] for r in reel_rows if r["views"] is not None]
    total_reel_views      = sum(reel_view_items) if reel_view_items else None
    reel_views_available  = len(reel_view_items)
    avg_views_per_reel    = (
        round(total_reel_views / reel_views_available, 2)
        if reel_views_available > 0 and total_reel_views is not None
        else None
    )

    total_reel_engagements = (
        total_reel_reactions + total_reel_comments + total_reel_shares
    )

    # ── News aggregates ──────────────────────────────────────────────
    total_news            = len(news_rows)
    total_news_reactions  = sum(r["reactions"] for r in news_rows)
    total_news_comments   = sum(r["comments"]  for r in news_rows)
    total_news_shares     = sum(r["shares"]    for r in news_rows)
    total_news_engagements = (
        total_news_reactions + total_news_comments + total_news_shares
    )

    # ── Page totals ──────────────────────────────────────────────────
    total_page_engagements  = total_reel_engagements + total_news_engagements
    total_page_views        = total_reel_views if total_reel_views is not None else 0
    total_page_interactions = total_page_engagements + total_page_views

    best_reel = max(reel_rows, key=lambda x: x["engagements"]) if reel_rows else None
    best_news = max(news_rows, key=lambda x: x["engagements"]) if news_rows else None

    return {
        "generated_at":            datetime.now(timezone.utc).isoformat(),
        "tenant":                  TENANT,
        "page_name":               PAGE_NAME,
        "total_reels":             total_reels,
        "total_news":              total_news,
        "total_reel_reactions":    total_reel_reactions,
        "total_reel_comments":     total_reel_comments,
        "total_reel_shares":       total_reel_shares,
        "total_reel_engagements":  total_reel_engagements,
        "total_reel_views":        total_reel_views,
        "reel_views_available":    reel_views_available,
        "avg_views_per_reel":      avg_views_per_reel,
        "total_news_reactions":    total_news_reactions,
        "total_news_comments":     total_news_comments,
        "total_news_shares":       total_news_shares,
        "total_news_engagements":  total_news_engagements,
        "total_page_engagements":  total_page_engagements,
        "total_page_views":        total_page_views,
        "total_page_interactions": total_page_interactions,
        "best_reel":               best_reel,
        "best_news":               best_news,
    }


# =====================================================================
# CSV SNAPSHOT
# =====================================================================
def save_snapshot(report):
    file_exists = os.path.exists(ANALYTICS_HISTORY_FILE)

    fieldnames = [
        "generated_at",
        "tenant",
        "page_name",
        "total_reels",
        "total_news",
        "total_reel_reactions",
        "total_reel_comments",
        "total_reel_shares",
        "total_reel_engagements",
        "total_reel_views",
        "reel_views_available",
        "avg_views_per_reel",
        "total_news_reactions",
        "total_news_comments",
        "total_news_shares",
        "total_news_engagements",
        "total_page_engagements",
        "total_page_views",
        "total_page_interactions",
        "best_reel_id",
        "best_reel_engagements",
        "best_news_id",
        "best_news_engagements",
    ]

    with open(ANALYTICS_HISTORY_FILE, "a", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        if not file_exists:
            writer.writeheader()

        writer.writerow({
            "generated_at":            report["generated_at"],
            "tenant":                  report["tenant"],
            "page_name":               report["page_name"],
            "total_reels":             report["total_reels"],
            "total_news":              report["total_news"],
            "total_reel_reactions":    report["total_reel_reactions"],
            "total_reel_comments":     report["total_reel_comments"],
            "total_reel_shares":       report["total_reel_shares"],
            "total_reel_engagements":  report["total_reel_engagements"],
            "total_reel_views":        report["total_reel_views"] if report["total_reel_views"] is not None else "",
            "reel_views_available":    report["reel_views_available"],
            "avg_views_per_reel":      report["avg_views_per_reel"] if report["avg_views_per_reel"] is not None else "",
            "total_news_reactions":    report["total_news_reactions"],
            "total_news_comments":     report["total_news_comments"],
            "total_news_shares":       report["total_news_shares"],
            "total_news_engagements":  report["total_news_engagements"],
            "total_page_engagements":  report["total_page_engagements"],
            "total_page_views":        report["total_page_views"],
            "total_page_interactions": report["total_page_interactions"],
            "best_reel_id":            report["best_reel"]["id"] if report["best_reel"] else "",
            "best_reel_engagements":   report["best_reel"]["engagements"] if report["best_reel"] else "",
            "best_news_id":            report["best_news"]["id"] if report["best_news"] else "",
            "best_news_engagements":   report["best_news"]["engagements"] if report["best_news"] else "",
        })

    print(f"[✓] Snapshot saved to {ANALYTICS_HISTORY_FILE}")


# =====================================================================
# TELEGRAM REPORT FORMATTER
# =====================================================================
def format_report(report):
    lines = [
        f"📊 <b>{html.escape(REPORT_TITLE)}</b>",
        f"<i>Tenant: {html.escape(TENANT.upper())} — {html.escape(PAGE_NAME)}</i>",
        "",
        "<b>🎬 REELS</b>",
        "──────────────────────",
        f"Total Reels: {report['total_reels']}",
        f"Total Reactions: {report['total_reel_reactions']}",
        f"Total Comments: {report['total_reel_comments']}",
        f"Total Shares: {report['total_reel_shares']}",
    ]

    if report["total_reel_views"] is not None:
        lines.append(f"Total Views: {report['total_reel_views']}")
        lines.append(f"Average Views/Reel: {report['avg_views_per_reel']}")
    else:
        lines.append("Total Views: Pending data...")
        lines.append("Average Views/Reel: Pending data...")

    if report["best_reel"]:
        lines.append(f"Top Reel: {html.escape(report['best_reel']['id'])}")
        lines.append(f"Top Reel Engagements: {report['best_reel']['engagements']}")

    lines += [
        "",
        "<b>📰 NEWS POSTS</b>",
        "──────────────────────",
        f"Total Posts: {report['total_news']}",
        f"Total Reactions: {report['total_news_reactions']}",
        f"Total Comments: {report['total_news_comments']}",
        f"Total Shares: {report['total_news_shares']}",
    ]

    if report["best_news"]:
        lines.append(f"Top News Post: {html.escape(report['best_news']['id'])}")
        lines.append(f"Top News Engagements: {report['best_news']['engagements']}")

    lines += [
        "",
        "<b>📈 PAGE TOTALS</b>",
        "──────────────────────",
        f"Total Engagements: {report['total_page_engagements']}",
    ]

    if report["total_page_views"]:
        lines.append(f"Total Views: {report['total_page_views']}")
    else:
        lines.append("Total Views: Pending data...")

    lines += [
        f"Total Interactions: {report['total_page_interactions']}",
        "",
        f"🕐 Generated: {report['generated_at'][:19].replace('T', ' ')} UTC",
    ]

    return "\n".join(lines)


# =====================================================================
# MAIN
# =====================================================================
if __name__ == "__main__":
    report  = build_report()
    save_snapshot(report)
    message = format_report(report)
    print(message)
    send_telegram_update(message)

