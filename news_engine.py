# =============================================================================
# news_engine.py
# CONCRETE HORIZONS — MULTI-TENANT NEWS INGESTION ENGINE
# =============================================================================
# Pulls articles from RSS feeds, scrapes full story text, grabs HD images,
# AI-washes the copy for the correct page voice, and posts to Facebook.
# All feeds, credentials, history files, and AI prompts are loaded from
# the tenant YAML config.
#
# USAGE:
#   python news_engine.py --config config/viral.yml
#   python news_engine.py --config config/producer.yml
#
# GITHUB ACTIONS:
#   Pass --config config/viral.yml    in the viral news workflow
#   Pass --config config/producer.yml in the producer news workflow
#
# SECRETS REQUIRED (viral):
#   FB_PAGE_ID, FB_PAGE_TOKEN, TG_BOT_TOKEN, TG_CHAT_ID, GEMINI_API_KEY
#
# SECRETS REQUIRED (producer):
#   FB_PRODUCER_PAGE_ID, FB_PRODUCER_PAGE_TOKEN,
#   TG_BOT_TOKEN, TG_CHAT_ID, GEMINI_API_KEY
# =============================================================================

import argparse
import hashlib
import os
import re
import time
import urllib.request
import xml.etree.ElementTree as ET

import requests
import google.genai as genai
from bs4 import BeautifulSoup

from config_loader import load_config, get_news_config
import meta_shared
from meta_shared import register_asset, set_cached_metrics

# ============================================================
# BOOT — load tenant config before anything else
# ============================================================
parser = argparse.ArgumentParser(description="Concrete Horizons News Engine")
parser.add_argument(
    "--config",
    default="config/viral.yml",
    help="Path to tenant YAML config (default: config/viral.yml)",
)
args = parser.parse_args()

cfg       = load_config(args.config)
meta_shared.configure_tenant(cfg)
news_cfg  = get_news_config(cfg)


# ============================================================
# 1. CONFIGURATION — loaded from tenant config + env vars
# ============================================================
FB_PAGE_TOKEN  = os.getenv(cfg.get("fb_page_token_env", "FB_PAGE_TOKEN"), "")
TG_BOT_TOKEN   = os.getenv("TG_BOT_TOKEN",  "")
TG_CHAT_ID     = os.getenv("TG_CHAT_ID",    "")
GEMINI_API_KEY = os.getenv("GEMINI_API_KEY", "")
client         = genai.Client(api_key=GEMINI_API_KEY)

FB_PAGE_ID     = meta_shared.FB_PAGE_ID

NEWS_HISTORY_FILE  = news_cfg.get("history_file",       "news_history.txt")
PAGE_NAME          = news_cfg.get("page_name",           cfg.get("page_name", "Concrete Horizons"))
CAPTION_STYLE      = news_cfg.get("caption_style",       "global news, objective, informative")
MAX_POSTS_PER_RUN  = news_cfg.get("max_posts_per_run",   1)
FEEDS              = news_cfg.get("feeds",               [])
TENANT             = cfg.get("tenant", "viral")

print(f"[news_engine] Tenant        : {TENANT}")
print(f"[news_engine] Page name     : {PAGE_NAME}")
print(f"[news_engine] History file  : {NEWS_HISTORY_FILE}")
print(f"[news_engine] Caption style : {CAPTION_STYLE}")
print(f"[news_engine] Feeds loaded  : {len(FEEDS)}")


# ============================================================
# 2. TELEGRAM
# ============================================================
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
        print(f"[-] Telegram alert failed: {e}")


# ============================================================
# 3. HISTORY
# ============================================================
def load_history():
    if not os.path.exists(NEWS_HISTORY_FILE):
        return set()
    with open(NEWS_HISTORY_FILE, "r", encoding="utf-8") as f:
        return set(line.strip() for line in f if line.strip())

def save_to_history(url_hash):
    with open(NEWS_HISTORY_FILE, "a", encoding="utf-8") as f:
        f.write(f"{url_hash}\n")


# ============================================================
# 4. RSS FEED READER
# ============================================================
def fetch_global_news(feed_url):
    try:
        request_headers  = {"User-Agent": "Mozilla/5.0"}
        network_request  = urllib.request.Request(feed_url, headers=request_headers)

        with urllib.request.urlopen(network_request, timeout=20) as network_response:
            xml_payload = network_response.read()

        xml_root        = ET.fromstring(xml_payload)
        parsed_articles = []

        for article_node in xml_root.findall(".//item"):
            title = article_node.find("title")
            link  = article_node.find("link")
            desc  = article_node.find("description")

            title = title.text if title is not None else ""
            link  = link.text  if link  is not None else ""
            rss_description = desc.text if desc is not None else ""

            image_url = ""
            enclosure = article_node.find("enclosure")
            if enclosure is not None:
                image_url = enclosure.get("url", "")

            if not image_url:
                for child in article_node:
                    if "content" in child.tag or "thumbnail" in child.tag:
                        image_url = child.attrib.get("url", "")
                        if image_url:
                            break

            if title and link:
                parsed_articles.append({
                    "title":           title.strip(),
                    "url":             link.strip(),
                    "image_url":       image_url.strip(),
                    "rss_description": clean_text(rss_description),
                })

        return parsed_articles

    except Exception as network_error:
        print(f"[-] Network processing bottleneck: {network_error}")
        return []


# ============================================================
# 5. FULL STORY EXTRACTION
# ============================================================
def clean_text(text):
    return re.sub(r"\s+", " ", text or "").strip()

def _dedupe_paragraphs(paragraphs):
    seen    = set()
    ordered = []
    for para in paragraphs:
        para = clean_text(para)
        if not para or para in seen:
            continue
        seen.add(para)
        ordered.append(para)
    return ordered

def fetch_full_story_text(article_url, rss_description=""):
    print(f"  [+] Scraping {article_url} for full story text...")
    try:
        headers = {
            "User-Agent": (
                "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
            )
        }
        res = requests.get(article_url, headers=headers, timeout=20)
        if res.status_code != 200:
            print(f"  [-] Full story fetch failed: HTTP {res.status_code}")
            return clean_text(rss_description)

        soup = BeautifulSoup(res.text, "html.parser")

        for tag_name in [
            "script", "style", "noscript", "svg", "iframe",
            "form", "nav", "footer", "header", "aside",
        ]:
            for tag in soup.find_all(tag_name):
                tag.decompose()

        paragraph_candidates = []
        selectors = [
            "article p",
            "main p",
            "[role='main'] p",
            "div[itemprop='articleBody'] p",
            "section p",
            "div[class*='article'] p",
            "div[class*='story'] p",
            "div[class*='content'] p",
        ]

        for selector in selectors:
            for p in soup.select(selector):
                text = clean_text(p.get_text(" ", strip=True))
                if text and len(text) > 35:
                    paragraph_candidates.append(text)
            if len(paragraph_candidates) >= 10:
                break

        ordered_paragraphs = _dedupe_paragraphs(paragraph_candidates)
        full_story         = "\n\n".join(ordered_paragraphs[:10]).strip()

        if len(full_story) < 220:
            meta_bits = []
            for selector in [
                ("meta", {"property": "og:description"}),
                ("meta", {"name": "description"}),
                ("meta", {"name": "twitter:description"}),
            ]:
                tag = soup.find(selector[0], selector[1])
                if tag and tag.get("content"):
                    content = clean_text(tag["content"])
                    if content:
                        meta_bits.append(content)
            if meta_bits:
                full_story = "\n\n".join(_dedupe_paragraphs(meta_bits)).strip()

        if len(full_story) < 120:
            full_story = clean_text(rss_description)

        return full_story[:7000]

    except Exception as e:
        print(f"  [-] Full story extraction failed: {e}")
        return clean_text(rss_description)


# ============================================================
# 6. HIGH-RES IMAGE SCRAPER
# ============================================================
def get_high_res_image(article_url, fallback_image):
    print(f"  [+] Scraping {article_url} for HD Open Graph image...")
    try:
        headers = {
            "User-Agent": (
                "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
            )
        }
        res = requests.get(article_url, headers=headers, timeout=15)
        if res.status_code == 200:
            soup     = BeautifulSoup(res.text, "html.parser")
            og_image = soup.find("meta", property="og:image")
            if og_image and og_image.get("content"):
                print("  [✓] HD Image Found (og:image)!")
                return og_image["content"]
            tw_image = soup.find("meta", attrs={"name": "twitter:image"})
            if tw_image and tw_image.get("content"):
                print("  [✓] HD Image Found (twitter:image)!")
                return tw_image["content"]

        print("  [-] No HD metadata found. Using RSS fallback image.")
    except Exception as e:
        print(f"  [-] Image extraction failed: {e}. Using RSS fallback.")

    return fallback_image


# ============================================================
# 7. AI TEXT WASH — tenant-aware prompt
# ============================================================
def build_ai_prompt(title, story_block):
    """
    Returns a Gemini prompt tuned to the active tenant's voice and audience.
    Viral page: broad, objective, global-news tone.
    Producer page: music industry, speaks directly to producers and engineers.
    """
    if TENANT == "producer":
        return (
            f"You are writing for a Facebook page called '{PAGE_NAME}' "
            f"that serves music producers, beatmakers, audio engineers, and artists.\n"
            f"Rewrite the news item below into an engaging, knowledgeable Facebook post "
            f"that speaks directly to music creators.\n"
            f"Use production language naturally where relevant. "
            f"Write 2 to 3 paragraphs, about 120 to 220 words total.\n"
            f"Do not sound like a press release. Write with authority and personality.\n"
            f"Do not mention that you are an AI.\n"
            f"End with exactly 3 relevant hashtags on a new line.\n\n"
            f"Headline:\n{title}\n\n"
            f"Full story context:\n{story_block}"
        )
    else:
        return (
            f"You are writing for a Facebook news page called '{PAGE_NAME}'.\n"
            f"Rewrite the news item below into an engaging, objective, readable Facebook post.\n"
            f"Use the full story context when available.\n"
            f"Write 2 to 3 paragraphs, about 120 to 220 words total.\n"
            f"Do not sound sensationalist or clickbaity.\n"
            f"Do not mention that you are an AI.\n"
            f"End with exactly 3 relevant hashtags on a new line.\n\n"
            f"Headline:\n{title}\n\n"
            f"Full story context:\n{story_block}"
        )


def wash_and_post_to_meta(title, url, image_url, story_text=""):
    print(f"\n[AI PROCESSING] Washing headline: {title}")

    story_block = clean_text(story_text)
    if not story_block:
        story_block = "No article body was available. Use the headline only."

    prompt        = build_ai_prompt(title, story_block)
    washed_caption = ""

    for attempt in range(2):
        try:
            response       = client.models.generate_content(
                model="gemini-2.0-flash",
                contents=prompt,
            )
            washed_caption = response.text.strip()
            break
        except Exception as ai_error:
            if ("429" in str(ai_error) or "RESOURCE_EXHAUSTED" in str(ai_error)) and attempt == 0:
                print("[!] Gemini rate limit hit. Waiting 65 seconds before retry...")
                time.sleep(65)
                continue
            print(f"[-] AI Processing Error: {ai_error}")
            break

    if not washed_caption:
        print("[!] AI generation failed completely. Using raw title fallback.")
        tenant_tag = "#musicproducer #beatmaker #producerlife" if TENANT == "producer" \
                     else "#news #concretehorizons #worldnews"
        washed_caption = f"{title}\n\n{tenant_tag}"

    # ── Post to Facebook ─────────────────────────────────────
    try:
        if image_url:
            print("[✓] Compiling Photo Post layout.")
            post_url = f"https://graph.facebook.com/v22.0/{FB_PAGE_ID}/photos"
            payload  = {
                "caption":      f"{washed_caption}\n\nFull coverage: {url}",
                "url":          image_url,
                "access_token": FB_PAGE_TOKEN,
            }
        else:
            print("[!] No image asset found. Falling back to Link Feed Post.")
            post_url = f"https://graph.facebook.com/v22.0/{FB_PAGE_ID}/feed"
            payload  = {
                "message":      washed_caption,
                "link":         url,
                "access_token": FB_PAGE_TOKEN,
            }

        res = requests.post(post_url, data=payload, timeout=15).json()

        if "id" in res:
            post_id = res.get("post_id") or res.get("id")
            print(f"[✓] News posted to Facebook [{TENANT}]. Post ID: {post_id}")

            send_telegram_update(
                f"📰 <b>News Posted! [{TENANT.upper()}]</b>\n\n"
                f"📝 <b>Caption:</b>\n{washed_caption}\n\n"
                f"🔗 <b>Source:</b> {url}\n"
                f"🆔 <b>Saved ID:</b> {post_id}"
            )

            register_asset(
                "news",
                post_id,
                source="news_engine",
                title=title,
                url=url,
                extra={"status": "published", "tenant": TENANT},
            )

            set_cached_metrics(
                post_id,
                kind="news",
                metrics={
                    "reactions":   0,
                    "comments":    0,
                    "shares":      0,
                    "views":       None,
                    "engagements": 0,
                },
                metadata={
                    "title":   title,
                    "url":     url,
                    "source":  "news_engine",
                    "caption": washed_caption,
                    "tenant":  TENANT,
                },
            )

            return "SUCCESS"

        else:
            print(f"[-] Meta API Error: {res}")
            err_msg = res.get("error", {}).get("message", "")
            if "OAuth" in str(res) or "access token" in err_msg.lower():
                send_telegram_update(
                    f"🛑 <b>Circuit Breaker [{TENANT}]:</b> Auth Token expired in News Engine."
                )
                return "FATAL"
            return "FAILED"

    except Exception as e:
        print(f"[-] Pipeline error during posting: {e}")
        return "FAILED"


# ============================================================
# 8. DEDUPLICATION LOOP
# ============================================================
def pipeline_deduplication_sync(articles, max_posts_per_run=1):
    history_set    = load_history()
    posts_executed = 0

    for article in articles:
        if posts_executed >= max_posts_per_run:
            break

        url_signature = hashlib.md5(article["url"].encode("utf-8")).hexdigest()
        if url_signature in history_set:
            continue

        print(f"\n[NEW UNIQUE STORY] -> {article['title']}")

        story_text   = fetch_full_story_text(article["url"], article.get("rss_description", ""))
        hd_image_url = get_high_res_image(article["url"], article["image_url"])

        status = wash_and_post_to_meta(
            article["title"],
            article["url"],
            hd_image_url,
            story_text=story_text,
        )

        if status == "SUCCESS":
            save_to_history(url_signature)
            history_set.add(url_signature)
            posts_executed += 1
            time.sleep(5)
        elif status == "FATAL":
            print("[-] Critical Token/API failure. Aborting loop.")
            posts_executed = -1
            break
        else:
            print("[-] Skipping article due to upload failure.")
            continue

    return posts_executed


# ============================================================
# 9. MAIN ENGINE LOOP
# ============================================================
if __name__ == "__main__":
    print("=" * 55)
    print(f"  {PAGE_NAME.upper()} — NEWS ENGINE")
    print("=" * 55)

    if not FEEDS:
        print("[FATAL] No RSS feeds defined in config. Check your YAML.")
        exit(1)

    total_new_posts = 0

    for target_feed in FEEDS:
        if total_new_posts >= MAX_POSTS_PER_RUN:
            print(
                f"[GOVERNOR] Global cap of {MAX_POSTS_PER_RUN} post(s) reached "
                f"for this cycle. Shutting down cleanly."
            )
            break

        print(f"\nScanning target node: {target_feed}")
        discovered_stories = fetch_global_news(target_feed)

        allowed_slots = MAX_POSTS_PER_RUN - total_new_posts
        if allowed_slots > 0:
            posts_this_feed = pipeline_deduplication_sync(
                discovered_stories,
                max_posts_per_run=allowed_slots,
            )

            if posts_this_feed == -1:
                print("[ABORT] Halting execution across all remaining feed networks.")
                break

            total_new_posts += posts_this_feed

    print(f"\n--- Cycle Completed: Posted {total_new_posts} new article(s) [{TENANT}] ---")
