# =============================================================================
# scraper.py
# CONCRETE HORIZONS — MULTI-TENANT VIDEO SCRAPER
# =============================================================================
# Scrapes video content from X (Twitter) for the active tenant page.
# Tenant identity, target handles, output paths, and queue file are all
# loaded from the YAML config. Nothing is hardcoded.
#
# USAGE:
#   python scraper.py --config config/viral.yml
#   python scraper.py --config config/producer.yml
#
# GITHUB ACTIONS:
#   Pass --config config/viral.yml    in the viral reel workflow
#   Pass --config config/producer.yml in the producer reel workflow
#
# SECRETS REQUIRED:
#   Viral:    X_AUTH_TOKEN, X_CT0
#   Producer: X_AUTH_TOKEN, X_CT0  (same X account — same cookies)
# =============================================================================

import argparse
import os
import re
import json
import requests

from config_loader import load_config, get_scraper_config
import meta_shared
from meta_shared import load_lines, append_unique_line

# ============================================================
# BOOT — load tenant config before anything else
# ============================================================
parser = argparse.ArgumentParser(description="Concrete Horizons Scraper")
parser.add_argument(
    "--config",
    default="config/viral.yml",
    help="Path to tenant YAML config (default: config/viral.yml)",
)
args = parser.parse_args()

cfg         = load_config(args.config)
meta_shared.configure_tenant(cfg)
scraper_cfg = get_scraper_config(cfg)


# ============================================================
# 1. CONFIGURATION — loaded from tenant config + env vars
# ============================================================
X_COOKIES = {
    "auth_token": os.getenv("X_AUTH_TOKEN", ""),
    "ct0":        os.getenv("X_CT0", ""),
}

NICHE_HANDLES  = scraper_cfg.get("niche_handles", {})
TARGET_ACCOUNTS = [
    handle
    for niche in NICHE_HANDLES.values()
    for handle in niche
]
ACCOUNT_TO_NICHE = {
    handle: niche
    for niche, handles in NICHE_HANDLES.items()
    for handle in handles
}

OUTPUT_DIR           = scraper_cfg.get("output_dir",   "downloaded_videos")
DAILY_LIMIT          = scraper_cfg.get("daily_limit",  2)   # 2 videos per run
QUEUE_FILE           = scraper_cfg.get("queue_file",   "queue.txt")
SCRAPER_HISTORY_FILE = scraper_cfg.get("history_file", "scraper_history.txt")
TENANT               = cfg.get("tenant", "viral")

os.makedirs(OUTPUT_DIR, exist_ok=True)

print(f"[scraper] Tenant       : {TENANT}")
print(f"[scraper] Output dir   : {OUTPUT_DIR}")
print(f"[scraper] Queue file   : {QUEUE_FILE}")
print(f"[scraper] History file : {SCRAPER_HISTORY_FILE}")
print(f"[scraper] Targets      : {len(TARGET_ACCOUNTS)} accounts")
print(f"[scraper] Videos/run   : {DAILY_LIMIT}")


# ============================================================
# NEGATIVE KEYWORD SHIELD — blocks unsafe / low-quality content
# ============================================================
NEGATIVE_KEYWORDS = [
    "kill", "shot", "shoot", "dead", "death", "blood", "murder",
    "fight", "accident", "crash", "gore", "nsfw", "suicide", "stab",
    "fatal", "tragedy", "gun", "weapon",
    "nude", "naked", "porn", "onlyfans", "strip", "thirst",
    "bikini", "lingerie", "sexy", "boobs", "ass", "topless",
]


# ============================================================
# 2. COOKIE GUARD
# ============================================================
def validate_cookies():
    if not X_COOKIES["auth_token"] or not X_COOKIES["ct0"]:
        print("[FATAL] X cookies not set. Add X_AUTH_TOKEN and X_CT0 to GitHub secrets.")
        return False
    return True


# ============================================================
# 3. TEXT CLEANING UTILITIES
# ============================================================
def clean_tweet_text(text):
    if not text:
        return ""
    text = re.sub(r"^RT @\w+:\s*", "", text.strip())
    text = re.sub(r"^@\w+\s*", "", text.strip())
    text = re.sub(r"https?://\S+", "", text)
    text = re.sub(r"#\w+", "", text)
    text = re.sub(r"\s+", " ", text).strip()
    return text


# ============================================================
# 4. CORE PIPELINE OPERATIONS
# ============================================================
def fetch_timeline(username, cookies):
    url = (
        f"https://syndication.twitter.com/srv/timeline-profile/screen-name/{username}"
        f"?dnt=true&embedId=twitter-widget-0&features=%3D%3D"
    )
    headers = {
        "User-Agent": (
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
            "(KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
        ),
        "Accept":          "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        "Accept-Language": "en-US,en;q=0.5",
        "Referer":         "https://platform.twitter.com/",
    }
    try:
        print(f"  [~] Scanning @{username}...")
        response = requests.get(url, headers=headers, cookies=cookies, timeout=15)

        if response.status_code == 401:
            print("  [FATAL] X cookies are expired or invalid. Status 401.")
            return []

        if response.status_code != 200:
            print(f"  [-] @{username} rejected. Status: {response.status_code}")
            return []

        match = re.search(
            r'<script id="__NEXT_DATA__" type="application/json">(.*?)</script>',
            response.text,
        )
        if not match:
            return []

        data = json.loads(match.group(1))
        entries = (
            data.get("props", {})
            .get("pageProps", {})
            .get("timeline", {})
            .get("entries", [])
        )
        return entries

    except Exception as e:
        print(f"  [-] Connection error for @{username}: {e}")
        return []


def _safe_text(value):
    return re.sub(r"\s+", " ", str(value or "")).strip()

def _get_tweet_data(entry):
    content    = entry.get("content", {}) if isinstance(entry, dict) else {}
    tweet_data = content.get("tweet", {})
    if not isinstance(tweet_data, dict):
        return {}
    return tweet_data

def _get_nested_dict(source, *path):
    current = source
    for key in path:
        if not isinstance(current, dict):
            return {}
        current = current.get(key, {})
    return current if isinstance(current, dict) else {}


def extract_video_metadata(entry):
    try:
        tweet_data = _get_tweet_data(entry)
        if not tweet_data:
            return None

        legacy = tweet_data.get("legacy", {}) if isinstance(tweet_data.get("legacy"), dict) else {}

        tweet_id = (
            tweet_data.get("id_str")
            or tweet_data.get("rest_id")
            or legacy.get("id_str")
            or ""
        )

        text = (
            tweet_data.get("text")
            or tweet_data.get("full_text")
            or legacy.get("full_text")
            or legacy.get("text")
            or ""
        )
        text = _safe_text(text)

        if not tweet_id:
            return None

        lowered = text.lower()
        if any(bad_word in lowered for bad_word in NEGATIVE_KEYWORDS):
            print(f"  [SHIELD] Blocked unsafe content in tweet {tweet_id}")
            return None

        media_list = (
            tweet_data.get("mediaDetails")
            or legacy.get("extended_entities", {}).get("media")
            or legacy.get("entities", {}).get("media")
            or tweet_data.get("extended_entities", {}).get("media")
            or tweet_data.get("entities", {}).get("media")
            or []
        )

        video_url       = None
        highest_bitrate = -1

        for media in media_list:
            if not isinstance(media, dict):
                continue
            media_type = media.get("type", "")
            if media_type in ("video", "animated_gif"):
                variants     = _get_nested_dict(media, "video_info").get("variants", [])
                mp4_variants = [
                    v for v in variants
                    if isinstance(v, dict) and v.get("content_type") == "video/mp4"
                ]
                for variant in mp4_variants:
                    bitrate = variant.get("bitrate", 0)
                    if bitrate > highest_bitrate:
                        highest_bitrate = bitrate
                        video_url = variant.get("url")

        if not video_url:
            return None

        metrics_source = (
            tweet_data.get("public_metrics")
            or legacy.get("public_metrics")
            or {}
        )
        if not isinstance(metrics_source, dict):
            metrics_source = {}

        view_count = (
            tweet_data.get("views")
            or tweet_data.get("view_count")
            or tweet_data.get("video_view_count")
            or legacy.get("views")
            or legacy.get("view_count")
            or legacy.get("video_view_count")
        )

        if view_count is None:
            for key in ("ext_views", "impression_count", "play_count", "video_views"):
                if key in tweet_data:
                    view_count = tweet_data.get(key)
                    break
                if key in legacy:
                    view_count = legacy.get(key)
                    break

        likes     = metrics_source.get("like_count",     0)
        retweets  = metrics_source.get("retweet_count",  0)
        replies   = metrics_source.get("reply_count",    0)
        quotes    = metrics_source.get("quote_count",    0)
        bookmarks = metrics_source.get("bookmark_count", 0)

        account = (
            tweet_data.get("core", {})
            .get("user_results", {})
            .get("result", {})
            .get("legacy", {})
            .get("screen_name")
            or ""
        )

        niche = ACCOUNT_TO_NICHE.get(account, "general")

        return {
            "id":        tweet_id,
            "text":      text,
            "video_url": video_url,
            "account":   account,
            "niche":     niche,
            "metrics": {
                "views":     int(view_count) if str(view_count).isdigit() else 0,
                "likes":     int(likes     or 0),
                "retweets":  int(retweets  or 0),
                "replies":   int(replies   or 0),
                "quotes":    int(quotes    or 0),
                "bookmarks": int(bookmarks or 0),
            },
        }

    except Exception as e:
        print(f"  [-] Metadata extraction error: {e}")
        return None


def score_video_candidate(meta):
    metrics   = meta.get("metrics", {}) if isinstance(meta, dict) else {}
    views     = int(metrics.get("views",     0) or 0)
    likes     = int(metrics.get("likes",     0) or 0)
    retweets  = int(metrics.get("retweets",  0) or 0)
    replies   = int(metrics.get("replies",   0) or 0)
    quotes    = int(metrics.get("quotes",    0) or 0)
    bookmarks = int(metrics.get("bookmarks", 0) or 0)
    text      = (meta.get("text") or "").lower()

    engagement_score = (
        likes     * 3
        + retweets  * 5
        + replies   * 3
        + quotes    * 3
        + bookmarks * 2
    )
    view_score = views * 10

    penalty = 0
    if any(word in text for word in [
        "nude", "naked", "porn", "onlyfans", "bikini",
        "lingerie", "sexy", "topless", "thirst",
    ]):
        penalty += 250000

    if any(word in text for word in [
        "fight", "blood", "gun", "weapon", "crash", "death", "kill",
    ]):
        penalty += 100000

    niche       = meta.get("niche", "general")
    niche_boost = 0

    if TENANT == "viral":
        if niche == "sports":           niche_boost = 1500
        elif niche == "satisfying":     niche_boost = 1200
        elif niche == "funny":          niche_boost = 1300
    elif TENANT == "producer":
        if niche == "production":           niche_boost = 1500
        elif niche == "beats":              niche_boost = 1400
        elif niche == "audio_engineering":  niche_boost = 1300

    return view_score + engagement_score + niche_boost - penalty


def download_pipeline_assets(meta, username, history_set):
    if meta["id"] in history_set:
        return False

    video_filename = os.path.join(OUTPUT_DIR, f"{username}_{meta['id']}.mp4")
    print(f"\n  [+] Downloading {meta['id']} from @{username}...")

    try:
        with requests.get(meta["video_url"], stream=True, timeout=30) as r:
            r.raise_for_status()
            with open(video_filename, "wb") as f:
                for chunk in r.iter_content(chunk_size=8192):
                    if chunk:
                        f.write(chunk)

        print(f"  [✓] Saved: {video_filename}")

        clean_title = clean_tweet_text(meta["text"])
        if not clean_title:
            clean_title = f"{cfg.get('page_name', 'Viral Video')} — Watch Now"

        print(f"  [✓] Queue title cleaned: {clean_title}")

        x_link = f"https://x.com/{username}/status/{meta['id']}"

        with open(QUEUE_FILE, "a", encoding="utf-8") as q:
            q.write(f"{x_link}||{clean_title}||{os.path.basename(video_filename)}\n")

        append_unique_line(SCRAPER_HISTORY_FILE, meta["id"])
        return True

    except Exception as e:
        print(f"  [-] Download failed for {meta['id']}: {e}")
        return False


# ============================================================
# 5. MAIN RUNNER
# ============================================================
def run_pipeline():
    print("=" * 55)
    print(f"  {cfg.get('page_name', 'CONCRETE HORIZONS').upper()} — VIDEO SCRAPER")
    print(f"  Target: {DAILY_LIMIT} video(s) this run")
    print("=" * 55)

    if not validate_cookies():
        return

    history_set    = set(load_lines(SCRAPER_HISTORY_FILE))
    all_candidates = []

    for account in TARGET_ACCOUNTS:
        niche = ACCOUNT_TO_NICHE.get(account, "general")
        print(f"\n🔍 @{account} [{niche}]...")
        entries = fetch_timeline(account, X_COOKIES)

        for entry in entries:
            video_meta = extract_video_metadata(entry)
            if video_meta and video_meta["id"] not in history_set:
                all_candidates.append(video_meta)

    if all_candidates:
        all_candidates.sort(key=score_video_candidate, reverse=True)

        total_downloaded = 0
        for video_meta in all_candidates:
            if total_downloaded >= DAILY_LIMIT:
                break
            if download_pipeline_assets(
                video_meta,
                video_meta.get("account", "unknown"),
                history_set,
            ):
                total_downloaded += 1
                history_set.add(video_meta["id"])

        print(f"\n[✓] Downloaded {total_downloaded}/{DAILY_LIMIT} video(s) this cycle.")
    else:
        print("\n[-] No new eligible videos found across all target accounts.")


if __name__ == "__main__":
    run_pipeline()
