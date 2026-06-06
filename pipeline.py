# =============================================================================
# pipeline.py
# CONCRETE HORIZONS — MULTI-TENANT REEL PROCESSOR & UPLOADER
# =============================================================================
# Reads the next queued video, runs FFmpeg wash, generates an AI caption,
# uploads to the correct Facebook page, and posts an engagement-bait comment.
# All page credentials, file paths, watermark text, and caption style are
# loaded from the tenant YAML config.
#
# USAGE:
#   python pipeline.py --config config/viral.yml
#   python pipeline.py --config config/producer.yml
#
# GITHUB ACTIONS:
#   Pass --config config/viral.yml    in the viral reel workflow
#   Pass --config config/producer.yml in the producer reel workflow
#
# SECRETS REQUIRED (viral):
#   FB_PAGE_ID, FB_PAGE_TOKEN, TG_BOT_TOKEN, TG_CHAT_ID, OPENROUTER_API_KEY
#
# SECRETS REQUIRED (producer):
#   FB_PRODUCER_PAGE_ID, FB_PRODUCER_PAGE_TOKEN,
#   TG_BOT_TOKEN, TG_CHAT_ID, OPENROUTER_API_KEY
# =============================================================================

import argparse
import glob
import json
import os
import re
import random
import requests
import subprocess
import time

from config_loader import load_config, get_pipeline_config
import meta_shared
from meta_shared import register_asset, set_cached_metrics

# ============================================================
# BOOT — load tenant config before anything else
# ============================================================
parser = argparse.ArgumentParser(description="Concrete Horizons Pipeline")
parser.add_argument(
    "--config",
    default="config/viral.yml",
    help="Path to tenant YAML config (default: config/viral.yml)",
)
args = parser.parse_args()

cfg          = load_config(args.config)
meta_shared.configure_tenant(cfg)
pipeline_cfg = get_pipeline_config(cfg)

# ============================================================
# 1. CONFIGURATION — loaded from tenant config + env vars
# ============================================================
FB_PAGE_TOKEN      = os.getenv(cfg.get("fb_page_token_env", "FB_PAGE_TOKEN"), "")
TG_BOT_TOKEN       = os.getenv("TG_BOT_TOKEN",       "")
TG_CHAT_ID         = os.getenv("TG_CHAT_ID",         "")
OPENROUTER_API_KEY = os.getenv("OPENROUTER_API_KEY", "")

FB_PAGE_ID     = meta_shared.FB_PAGE_ID

QUEUE_FILE      = pipeline_cfg.get("queue_file",      "queue.txt")
HISTORY_FILE    = pipeline_cfg.get("history_file",    "history.txt")
WATERMARK_TEXT  = pipeline_cfg.get("watermark_text",  "CONCRETE HORIZONS")
PAGE_NAME       = pipeline_cfg.get("page_name",       cfg.get("page_name", "Concrete Horizons"))
CAPTION_STYLE   = pipeline_cfg.get("caption_style",   "viral")
TENANT          = cfg.get("tenant", "viral")

print(f"[pipeline] Tenant        : {TENANT}")
print(f"[pipeline] Page name     : {PAGE_NAME}")
print(f"[pipeline] Queue file    : {QUEUE_FILE}")
print(f"[pipeline] History file  : {HISTORY_FILE}")
print(f"[pipeline] Watermark     : {WATERMARK_TEXT}")
print(f"[pipeline] Caption style : {CAPTION_STYLE}")


# ============================================================
# 2. OPENROUTER AI — drop-in Gemini replacement
# ============================================================
def generate_text(prompt, max_tokens=300):
    """
    Calls OpenRouter with a free model.
    Returns the text response or empty string on failure.
    """
    if not OPENROUTER_API_KEY:
        print("[!] OPENROUTER_API_KEY not set.")
        return ""
    try:
        response = requests.post(
            "https://openrouter.ai/api/v1/chat/completions",
            headers={
                "Authorization":  f"Bearer {OPENROUTER_API_KEY}",
                "Content-Type":   "application/json",
                "HTTP-Referer":   "https://github.com/ourovectr/Tenets",
                "X-Title":        "Concrete Horizons",
            },
            json={
                "model":    "meta-llama/llama-3.1-8b-instruct:free",
                "messages": [{"role": "user", "content": prompt}],
                "max_tokens": max_tokens,
            },
            timeout=30,
        )
        data = response.json()
        if "choices" in data:
            return data["choices"][0]["message"]["content"].strip()
        print(f"[!] OpenRouter unexpected response: {data}")
        return ""
    except Exception as e:
        print(f"[-] OpenRouter error: {e}")
        return ""


# ============================================================
# 3. TELEGRAM
# ============================================================
def send_telegram_update(message):
    if not TG_BOT_TOKEN or not TG_CHAT_ID:
        print("[!] Telegram credentials not set — skipping notification.")
        return
    url     = f"https://api.telegram.org/bot{TG_BOT_TOKEN}/sendMessage"
    payload = {"chat_id": TG_CHAT_ID, "text": message, "parse_mode": "HTML"}
    try:
        response = requests.post(url, json=payload, timeout=10)
        if response.status_code >= 400:
            print(f"[-] Telegram API error {response.status_code}: {response.text}")
    except Exception as e:
        print(f"[-] Telegram transmission anomaly: {e}")


# ============================================================
# 4. RATE LIMIT SAFETY CHECK
# ============================================================
def verify_api_rate_clearance():
    try:
        test_url = (
            f"https://graph.facebook.com/v22.0/{FB_PAGE_ID}"
            f"?fields=name&access_token={FB_PAGE_TOKEN}"
        )
        response = requests.get(test_url, timeout=10)

        if response.status_code != 200:
            err = response.json()
            print(f"[FATAL] Facebook API Error: {err}")
            send_telegram_update(
                f"🛑 <b>Circuit Breaker [{TENANT}]:</b> Auth Token expired or invalid. Halting."
            )
            return False

        usage_header = (
            response.headers.get("x-page-usage")
            or response.headers.get("x-app-usage")
        )
        if usage_header:
            usage_json     = json.loads(usage_header)
            call_count_pct = usage_json.get("call_count", 0)
            total_pct = max(
                call_count_pct,
                usage_json.get("total_cputime", 0),
                usage_json.get("total_time",    0),
            )
            print(f"[MONITOR] Meta Engine Load [{TENANT}]: {total_pct}%")
            if total_pct > 85:
                send_telegram_update(
                    f"⚠️ <b>Rate Limit Warning [{TENANT}]:</b> Engine load at {total_pct}%. Pausing."
                )
                return False

        return True
    except Exception as e:
        print(f"[!] System health check bypassed: {e}")
        return True


# ============================================================
# 5. HASHTAG ENGINE — tenant-aware niche sets
# ============================================================

# ── Viral hashtag pools ──────────────────────────────────────
VIRAL_HASHTAGS = {
    "sports": [
        "#sports", "#sportsclips", "#athlete", "#gamehighlights",
        "#winning", "#competition", "#viral", "#trending", "#reels", "#explorepage",
    ],
    "satisfying": [
        "#satisfying", "#oddlysatisfying", "#asmr", "#restoration",
        "#cleaning", "#craftsmanship", "#viral", "#trending",
    ],
    "funny": [
        "#funny", "#humor", "#fails", "#viral", "#trending",
        "#reels", "#explorepage", "#laugh", "#watchthis",
    ],
    "general": [
        "#viral", "#trending", "#reels", "#explorepage",
        "#watchthis", "#content", "#mustwatch", "#fyp",
    ],
}

VIRAL_HASHTAG_COUNTS = {
    "sports": 10, "satisfying": 8, "funny": 9, "general": 8,
}

VIRAL_KEYWORDS = {
    "sports": [
        "sports", "game", "goal", "match", "team", "win", "winning",
        "football", "soccer", "basketball", "baseball", "boxing",
        "ufc", "f1", "race", "racing", "athlete", "championship",
        "highlights", "training",
    ],
    "satisfying": [
        "satisfying", "oddly", "asmr", "cleaning", "restore", "restoration",
        "craft", "crafts", "carve", "cut", "repair", "build", "painting",
        "organize", "organising", "pressure wash", "wash", "polish",
    ],
    "funny": [
        "funny", "meme", "lol", "haha", "fails", "fail", "prank",
        "comedy", "humor", "humour", "aww", "cute", "animals",
        "cat", "dog", "viral", "laugh",
    ],
}

# ── Producer hashtag pools ───────────────────────────────────
PRODUCER_HASHTAGS = {
    "production": [
        "#musicproducer", "#beatmaker", "#flstudio", "#ableton",
        "#producerlife", "#hiphopbeats", "#trapbeats", "#studiolife",
        "#newmusic", "#beatsforsale",
    ],
    "beats": [
        "#beats", "#beatsforsale", "#hiphop", "#trap", "#lofi",
        "#instrumental", "#beatmaker", "#producer", "#newbeat", "#reels",
    ],
    "audio_engineering": [
        "#mixingengineer", "#masteringaudio", "#homestudio", "#daw",
        "#plugins", "#sounddesign", "#audioengineering", "#music", "#reels",
    ],
    "general": [
        "#musicproducer", "#beatmaker", "#producerlife", "#studiolife",
        "#newmusic", "#reels", "#trending", "#mustwatch",
    ],
}

PRODUCER_HASHTAG_COUNTS = {
    "production": 10, "beats": 10, "audio_engineering": 9, "general": 8,
}

PRODUCER_KEYWORDS = {
    "production": [
        "fl studio", "ableton", "logic pro", "daw", "plugin", "vst",
        "sample pack", "midi", "synthesizer", "kontakt", "serum",
    ],
    "beats": [
        "beat", "instrumental", "trap", "drill", "lofi", "hiphop",
        "hip hop", "type beat", "freestyle", "bpm",
    ],
    "audio_engineering": [
        "mixing", "mastering", "eq", "compression", "reverb", "delay",
        "stem", "audio engineer", "mix", "sound design",
    ],
}


def infer_niche(source_text):
    text    = (source_text or "").lower()
    keywords = PRODUCER_KEYWORDS if TENANT == "producer" else VIRAL_KEYWORDS
    for niche, kws in keywords.items():
        if any(kw in text for kw in kws):
            return niche
    return "general"

def build_reel_hashtags(source_text, max_tags=None):
    niche      = infer_niche(source_text)
    tag_pool   = PRODUCER_HASHTAGS if TENANT == "producer" else VIRAL_HASHTAGS
    count_pool = PRODUCER_HASHTAG_COUNTS if TENANT == "producer" else VIRAL_HASHTAG_COUNTS
    tags         = list(tag_pool.get(niche, tag_pool["general"]))
    target_count = max_tags if max_tags is not None else count_pool.get(niche, 8)
    return " ".join(tags[:target_count])

def build_fallback_caption(raw_source_text):
    clean_text = (raw_source_text or "Exclusive update").replace("\n", " ").strip()
    return (
        f"{clean_text}\n\n"
        f"Stay tuned for more.\n\n"
        f"{build_reel_hashtags(clean_text)}"
    )


# ============================================================
# 6. ENGAGEMENT-BAIT COMMENT BOT
# ============================================================
def deploy_engagement_bait_comment(post_id, post_caption):
    print("[ENGAGEMENT SYSTEM] Preparing automated first comment...")
    time.sleep(10)

    try:
        if TENANT == "producer":
            prompt = (
                f"Based on this music producer Facebook Reel caption, write a single short "
                f"follow-up question for the comments that sparks debate among producers, "
                f"beatmakers, or audio engineers. Under 15 words. No hashtags. No emojis. "
                f"Write like a page owner who knows music production.\n\n"
                f"Caption: {post_caption}"
            )
        else:
            prompt = (
                f"Based on this social media post caption, write a single interactive short "
                f"follow-up question to drop in the comment section that forces viewers to "
                f"argue, comment, or share their opinion. Under 15 words. No hashtags or "
                f"emojis. Write like a human page owner.\n\n"
                f"Caption: {post_caption}"
            )

        bait_question = generate_text(prompt, max_tokens=50).replace('"', '')

        if not bait_question:
            print("[!] Engagement comment generation returned empty. Skipping.")
            return False

        comment_url = f"https://graph.facebook.com/v22.0/{post_id}/comments"
        payload     = {"message": bait_question, "access_token": FB_PAGE_TOKEN}
        res         = requests.post(comment_url, data=payload, timeout=15).json()

        if "id" in res:
            print(f'[✓] First Comment Deployed: "{bait_question}"')
            return True
        else:
            print(f"[-] Engagement comment failed: {res}")
            return False

    except Exception as e:
        print(f"[-] Engagement bot error: {e}")
        return False


# ============================================================
# 7. FFMPEG WATERMARK WASH
# ============================================================
def get_fontfile():
    candidates = [
        "/usr/share/fonts/truetype/dejavu/DejaVuSans.ttf",
        "/usr/share/fonts/truetype/liberation2/LiberationSans-Regular.ttf",
        "/usr/share/fonts/truetype/freefont/FreeSans.ttf",
    ]
    for path in candidates:
        if os.path.exists(path):
            return path
    return None

def execute_laundry_wash(input_path, output_path):
    print(f"[LAUNDRY] Processing: {input_path}")

    speed_factor   = round(random.uniform(0.97, 1.03), 2)
    audio_pts      = round(1.0 / speed_factor, 2)
    watermark_text = WATERMARK_TEXT
    fontfile       = get_fontfile()

    drawtext_base = (
        f"drawtext=text='{watermark_text}':fontcolor=white@0.4:fontsize=42:"
        f"x=(w-text_w)/2:y=h-250"
    )
    if fontfile:
        drawtext_base += f":fontfile={fontfile}"

    video_filter = (
        f"scale=1080:1920:force_original_aspect_ratio=increase,crop=1080:1920,"
        f"setpts={audio_pts}*PTS,"
        f"{drawtext_base}"
    )

    ffmpeg_cmd = [
        "ffmpeg", "-y",
        "-i", input_path,
        "-vf", video_filter,
        "-filter:a", f"atempo={speed_factor}",
        "-c:v", "libx264", "-profile:v", "main", "-level:v", "4.0",
        "-pix_fmt", "yuv420p",
        "-c:a", "aac", "-b:a", "128k",
        "-movflags", "+faststart",
        "-map_metadata", "-1",
        output_path,
    ]

    try:
        subprocess.run(
            ffmpeg_cmd,
            stdout=subprocess.PIPE, stderr=subprocess.PIPE,
            text=True, check=True,
        )
        print("[✓] FFmpeg complete. Watermark burned, metadata stripped.")
        return True
    except subprocess.CalledProcessError as e:
        print(f"[-] FFmpeg failure: {e.stderr}")
        return False


# ============================================================
# 8. QUEUE UTILITIES
# ============================================================
def get_next_queued_video():
    if not os.path.exists(QUEUE_FILE):
        return None

    try:
        with open(QUEUE_FILE, "r", encoding="utf-8") as f:
            lines = [line.strip() for line in f if line.strip()]

        if not lines:
            return None

        first_line    = lines[0]
        parts         = first_line.split("||")
        url           = parts[0].strip() if len(parts) > 0 else "UNKNOWN_URL"
        title         = parts[1].strip() if len(parts) > 1 else "Exclusive Update"
        filename_hint = parts[2].strip() if len(parts) > 2 else ""

        output_dir = pipeline_cfg.get(
            "output_dir",
            "downloaded_videos_producer" if TENANT == "producer" else "downloaded_videos",
        )

        if filename_hint:
            direct_candidates = [
                filename_hint,
                os.path.join(output_dir, filename_hint),
                os.path.join(".", filename_hint),
            ]
            for candidate in direct_candidates:
                if os.path.exists(candidate) and candidate.endswith(".mp4"):
                    return {"filepath": candidate, "title": title, "url": url}

        tweet_id        = url.rstrip("/").split("/")[-1]
        exact_match     = glob.glob(f"{output_dir}/*_{tweet_id}.mp4")
        if exact_match:
            return {"filepath": exact_match[0], "title": title, "url": url}

        root_exact_match = glob.glob(f"*_{tweet_id}.mp4")
        if root_exact_match:
            return {"filepath": root_exact_match[0], "title": title, "url": url}

        fallback = [
            f for f in glob.glob(f"{output_dir}/*.mp4")
            if "washed_factory_output" not in f
        ]
        if not fallback:
            fallback = [f for f in glob.glob("*.mp4") if "washed_factory_output" not in f]

        if not fallback:
            print(f"[-] Queue entry found (ID: {tweet_id}), but no matching .mp4 in {output_dir}/")
            return None

        print(f"[!] Exact file match not found for {tweet_id}. Using fallback: {fallback[0]}")
        return {"filepath": fallback[0], "title": title, "url": url}

    except Exception as e:
        print(f"[-] Queue read failure: {e}")
        return None

def pop_completed_queue_item(source_filepath):
    try:
        with open(QUEUE_FILE, "r", encoding="utf-8") as f:
            lines = f.readlines()

        remaining = [l for l in lines[1:] if l.strip()]

        with open(QUEUE_FILE, "w", encoding="utf-8") as f:
            f.writelines([l if l.endswith("\n") else l + "\n" for l in remaining])

        print(f"[QUEUE] Item removed. {len(remaining)} item(s) still queued.")

        if os.path.exists(source_filepath):
            os.remove(source_filepath)
            print(f"[CLEANUP] Deleted source file: {source_filepath}")

    except Exception as e:
        print(f"[-] Queue pop error: {e}")


# ============================================================
# 9. META PUBLISHING PIPELINE
# ============================================================
def broadcast_reel_to_meta(video_path, caption_text):
    print("[PIPELINE-META] Initializing upload sequence...")

    init_url     = f"https://graph.facebook.com/v22.0/{FB_PAGE_ID}/video_reels"
    init_payload = {"upload_phase": "START", "access_token": FB_PAGE_TOKEN}

    try:
        init_res   = requests.post(init_url, data=init_payload, timeout=15).json()
        video_id   = init_res.get("video_id")
        upload_url = init_res.get("upload_url")

        if not video_id or not upload_url:
            print(f"[-] Phase 1 Failed: {init_res}")
            err_msg = init_res.get("error", {}).get("message", "Unknown Error")
            send_telegram_update(
                f"🛑 <b>Upload Failed [{TENANT}] (Phase 1):</b> Meta rejected START.\n"
                f"<code>{err_msg}</code>"
            )
            return False

        print(f"[✓] Upload session open. Video ID: {video_id}")

        with open(video_path, "rb") as vf:
            headers = {
                "Authorization": f"OAuth {FB_PAGE_TOKEN}",
                "offset":        "0",
                "file_size":     str(os.path.getsize(video_path)),
            }
            upload_res = requests.post(upload_url, data=vf, headers=headers, timeout=60).json()

        if not upload_res.get("success", False):
            print(f"[-] Phase 2 Stream Interrupted: {upload_res}")
            send_telegram_update(
                f"🛑 <b>Upload Failed [{TENANT}] (Phase 2):</b> Stream transfer rejected."
            )
            return False

        print("[✓] Video payload accepted by Meta.")

        publish_url     = f"https://graph.facebook.com/v22.0/{FB_PAGE_ID}/video_reels"
        publish_payload = {
            "upload_phase": "FINISH",
            "video_id":     video_id,
            "video_state":  "PUBLISHED",
            "description":  caption_text,
            "access_token": FB_PAGE_TOKEN,
        }
        publish_res = requests.post(publish_url, data=publish_payload, timeout=20).json()

        if publish_res.get("success", False) or "id" in publish_res:
            saved_reel_id = publish_res.get("video_id") or publish_res.get("id") or video_id
            print(f"[SUCCESS] Reel live! Asset ID: {saved_reel_id}")
            send_telegram_update(
                f"🎬 <b>New Reel Deployed! [{TENANT.upper()}]</b>\n\n"
                f"📝 <b>Caption:</b>\n{caption_text}\n\n"
                f"⚙️ <b>State:</b> Hashed & Cleaned"
            )
            deploy_engagement_bait_comment(saved_reel_id, caption_text)
            return saved_reel_id
        else:
            print(f"[-] Phase 3 Error: {publish_res}")
            err_msg = publish_res.get("error", {}).get("message", "Unknown Error")
            send_telegram_update(
                f"🛑 <b>Upload Failed [{TENANT}] (Phase 3):</b> FINISH rejected.\n"
                f"<code>{err_msg}</code>"
            )
            return False

    except Exception as e:
        print(f"[-] Runtime failure in publishing cycle: {e}")
        send_telegram_update(
            f"🛑 <b>Pipeline Crashed [{TENANT}]:</b> Fatal exception.\n<code>{str(e)}</code>"
        )
        return False


# ============================================================
# 10. MAIN
# ============================================================
if __name__ == "__main__":
    print("=" * 55)
    print(f"  {PAGE_NAME.upper()} — REELS ENGINE v2")
    print("=" * 55)

    if not verify_api_rate_clearance():
        print("[HALT] Load threshold exceeded or auth error. Terminating safely.")
        exit()

    target_job = get_next_queued_video()

    if not target_job:
        print(f"[QUEUE] No videos found in {QUEUE_FILE}. Standby mode active.")
        exit()

    source_file      = target_job["filepath"]
    raw_source_text  = target_job.get("title", "Exclusive Update")
    raw_source_text  = re.sub(r"https?://\S+|www\.\S+", "", raw_source_text).strip()

    processed_output_file = f"washed_factory_output_{TENANT}.mp4"

    print(f"[ACTIVE JOB] Processing: {source_file}")

    laundry_success = execute_laundry_wash(source_file, processed_output_file)

    if not laundry_success:
        print("[-] Aborting — video render failed.")
        send_telegram_update(
            f"🛑 <b>Laundry Failed [{TENANT}]:</b> FFmpeg could not process: "
            f"<code>{source_file}</code>"
        )
        exit()

    print("[AI] Generating caption with OpenRouter...")
    if TENANT == "producer":
        prompt = (
            f"Rewrite this title into a high-retention engaging Facebook Reel description "
            f"for a music production page called '{PAGE_NAME}': '{raw_source_text}'. "
            f"Keep it under 2 sentences. Speak directly to producers, beatmakers, "
            f"and audio engineers. Write like an expert in the craft. "
            f"Do NOT include any hashtags or URLs."
        )
    else:
        prompt = (
            f"Rewrite this title into a high-retention, engaging Facebook Reel description: "
            f"'{raw_source_text}'. "
            f"Keep it under 2 sentences. Maximize curiosity. Write like an expert "
            f"commentator. Do NOT include any hashtags or URLs."
        )

    final_caption = generate_text(prompt, max_tokens=150)

    if final_caption:
        print("[✓] AI caption generated.")
    else:
        print("[!] AI generation failed. Using fallback caption.")
        final_caption = build_fallback_caption(raw_source_text)

    hashtag_source = f"{raw_source_text} {final_caption}"
    reel_hashtags  = build_reel_hashtags(hashtag_source)
    caption_body   = re.sub(r"(?:#\w+\s*)+$", "", final_caption.strip()).strip()
    final_caption  = f"{caption_body}\n\n{reel_hashtags}"

    if os.path.exists(processed_output_file):
        broadcast_result = broadcast_reel_to_meta(processed_output_file, final_caption)

        if broadcast_result:
            register_asset(
                "reels",
                broadcast_result,
                source="pipeline",
                title=target_job.get("title", ""),
                url=target_job.get("url", ""),
                extra={
                    "queue_source": source_file,
                    "caption":      final_caption,
                    "status":       "published",
                    "tenant":       TENANT,
                },
            )

            set_cached_metrics(
                broadcast_result,
                kind="reel",
                metrics={
                    "reactions":   0,
                    "comments":    0,
                    "shares":      0,
                    "views":       None,
                    "engagements": 0,
                },
                metadata={
                    "title":   target_job.get("title", ""),
                    "url":     target_job.get("url",   ""),
                    "source":  "pipeline",
                    "caption": final_caption,
                    "tenant":  TENANT,
                },
            )

            print("[INFO] Cleaning up queue...")
            pop_completed_queue_item(source_file)

            try:
                video_id_for_history = (
                    target_job["url"].rstrip("/").split("/")[-1]
                    if "/" in target_job["url"] else target_job["url"]
                )
                with open(HISTORY_FILE, "a", encoding="utf-8") as history:
                    history.write(f"{video_id_for_history}\n")
                print(f"[✓] History updated: {video_id_for_history}")
            except Exception as e:
                print(f"[-] History write failed: {e}")

        else:
            print("[!] Broadcast failed. Queue item retained for next run.")
            send_telegram_update(
                f"⚠️ <b>Job Uncompleted [{TENANT}]:</b> File failed to post. "
                f"Kept in queue for next cycle."
            )

        if os.path.exists(processed_output_file):
            os.remove(processed_output_file)
            print("[CLEANUP] Purged temporary washed file.")

    else:
        print("[-] Critical: Washed output file missing at broadcast time.")
        send_telegram_update(
            f"🛑 <b>Critical Output Error [{TENANT}]:</b> Washed file not found when "
            f"broadcaster tried to post."
        )

    print("\n--- Pipeline Cycle Terminated Cleanly ---")