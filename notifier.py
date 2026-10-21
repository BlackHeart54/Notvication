import os
import sys
import logging
import asyncio
import aiohttp
import json
from datetime import datetime, timedelta, timezone
from supabase import create_client

# ------------------------------------------------------------------
# Config
# ------------------------------------------------------------------
BOT_TOKEN = os.environ.get("BOT_TOKEN")
SUPABASE_URL = os.environ.get("SUPABASE_URL")
SUPABASE_KEY = os.environ.get("SUPABASE_SERVICE_ROLE_KEY")
CHANNEL_ID_RAW = os.environ.get("CHANNEL_ID", "").strip()
MY_ID = int(os.environ.get("MY_ID", "0") or "0")
MAX_CONTENT_AGE_HOURS = float(os.environ.get("MAX_CONTENT_AGE_HOURS", "4"))

if not BOT_TOKEN:
    sys.exit("BOT_TOKEN is required")
if not SUPABASE_URL or not SUPABASE_KEY:
    sys.exit("SUPABASE_URL and SUPABASE_SERVICE_ROLE_KEY are required")

CHANNEL_IDS = [c.strip() for c in CHANNEL_ID_RAW.split(",") if c.strip()]

TELEGRAM_API = f"https://api.telegram.org/bot{BOT_TOKEN}"
CONTENT_COLUMNS = "title, url, category, type, kurdish_title, poster_url, genre, rating, runtime, story"
STATE_KEY = "last_notified_id"

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger("notifier")

sb = create_client(SUPABASE_URL, SUPABASE_KEY)

NOTIFY_GENRE_OPTIONS = [
    {"key": "action", "matchTerms": ["action"]},
    {"key": "adventure", "matchTerms": ["adventure"]},
    {"key": "animation", "matchTerms": ["animation"]},
    {"key": "comedy", "matchTerms": ["comedy"]},
    {"key": "crime", "matchTerms": ["crime"]},
    {"key": "documentary", "matchTerms": ["documentary"]},
    {"key": "drama", "matchTerms": ["drama"]},
    {"key": "family", "matchTerms": ["family"]},
    {"key": "fantasy", "matchTerms": ["fantasy"]},
    {"key": "horror", "matchTerms": ["horror"]},
    {"key": "mystery", "matchTerms": ["mystery"]},
    {"key": "romance", "matchTerms": ["romance"]},
    {"key": "scifi", "matchTerms": ["sci-fi", "science fiction", "scifi"]},
    {"key": "thriller", "matchTerms": ["thriller"]},
    {"key": "war", "matchTerms": ["war"]},
    {"key": "western", "matchTerms": ["western"]},
]

_error_sent_times = {}
_TELEGRAM_SEMAPHORE = asyncio.Semaphore(30)


# ------------------------------------------------------------------
# Small helpers
# ------------------------------------------------------------------
def esc(text):
    if text is None or text == "":
        return ""
    return (
        str(text)
        .replace("&", "&amp;")
        .replace("<", "&lt;")
        .replace(">", "&gt;")
        .replace('"', "&quot;")
        .replace("'", "&#x27;")
    )


def is_usable(value):
    if not value:
        return False
    v = str(value).strip()
    return v != "" and v.upper() != "N/A"


def content_matches_genre_prefs(content_genre, selected_keys):
    if not selected_keys:
        return True
    genre_text = str(content_genre or "").lower()
    if not genre_text:
        return False
    selected = set(selected_keys)
    for opt in NOTIFY_GENRE_OPTIONS:
        if opt["key"] not in selected:
            continue
        if any(term in genre_text for term in opt["matchTerms"]):
            return True
    return False


def send_telegram_error(message):
    if not MY_ID:
        return
    prefix = str(message)[:50]
    now = datetime.now(timezone.utc).timestamp()
    last = _error_sent_times.get(prefix)
    if last and now - last < 60:
        return
    _error_sent_times[prefix] = now
    try:
        import requests
        requests.post(
            f"{TELEGRAM_API}/sendMessage",
            data={"chat_id": str(MY_ID), "text": str(message)[:4000]},
            timeout=15,
        )
    except Exception:
        pass


def retry(fn, *args, max_retries=5, **kwargs):
    for attempt in range(max_retries):
        try:
            return fn(*args, **kwargs)
        except Exception as e:
            msg = str(e).lower()
            is_conn_issue = any(k in msg for k in ("connection", "timeout", "closed", "fetch"))
            if is_conn_issue and attempt < max_retries - 1:
                import time
                time.sleep(2)
                continue
            raise
    return None


# ------------------------------------------------------------------
# Supabase access
# ------------------------------------------------------------------
def get_setting(key, default=None):
    res = sb.table("bot_settings").select("value").eq("key", key).maybe_single().execute()
    if res is None or res.data is None:
        return default
    return res.data["value"]


def set_setting(key, value):
    sb.table("bot_settings").upsert({"key": key, "value": str(value)}, on_conflict="key").execute()


def get_global_notify_enabled():
    v = get_setting("global_notify_enabled")
    return True if v is None else v == "true"


def get_group_notify_enabled():
    v = get_setting("group_notify_enabled")
    return True if v is None else v == "true"


def get_notify_subscribers():
    res = sb.table("users").select("user_id").eq("notify_new", True).execute()
    return [row["user_id"] for row in (res.data or [])]


def get_all_genre_prefs_map():
    res = sb.table("user_genre_prefs").select("user_id, genre_key").execute()
    m = {}
    for row in res.data or []:
        m.setdefault(row["user_id"], []).append(row["genre_key"])
    return m


def get_max_content_id():
    res = sb.table("all_content").select("id").order("id", desc=True).limit(1).execute()
    return res.data[0]["id"] if res.data else 0


def get_new_content_rows(last_id, limit=50):
    res = (
        sb.table("all_content")
        .select(f"id, created_at, skip_notify, {CONTENT_COLUMNS}")
        .gt("id", last_id)
        .order("id", desc=False)
        .limit(limit)
        .execute()
    )
    return res.data or []


def claim_content_for_channel_post(content_id):
    res = (
        sb.table("all_content")
        .update({"channel_posted": True})
        .eq("id", content_id)
        .eq("channel_posted", False)
        .execute()
    )
    return bool(res.data)


def was_content_recently_channel_posted(title, category, hours=24):
    since = (datetime.now(timezone.utc) - timedelta(hours=hours)).isoformat()
    res = (
        sb.table("all_content")
        .select("id")
        .eq("channel_posted", True)
        .eq("category", category)
        .ilike("title", (title or "").strip())
        .gte("created_at", since)
        .limit(1)
        .execute()
    )
    return bool(res.data)


# ------------------------------------------------------------------
# Message building
# ------------------------------------------------------------------
def build_new_content_texts(title, category, item_type, extra):
    kurdish_title = extra.get("kurdish_title")
    poster_url = extra.get("poster_url")
    genre = extra.get("genre")
    rating = extra.get("rating")
    runtime = extra.get("runtime")
    story = extra.get("story")

    type_label = {"movie": "🎬 فیلمێکی نوێ", "series": "🍿 زنجیرەیەکی نوێ"}.get(
        item_type, "🎬🍿 بەرهەمێکی نوێ"
    )

    lines = [f"✨ <b>{esc(type_label)} زیادکرا!</b>", ""]
    lines.append(f"📌 <b>ناو:</b> {esc(title)}")
    if is_usable(kurdish_title) and str(kurdish_title).strip() != str(title).strip():
        lines.append(f"🅺 <b>ناوی کوردی:</b> {esc(kurdish_title)}")
    if is_usable(genre):
        lines.append(f"🎭 <b>ژانەر:</b> {esc(genre)}")
    if is_usable(runtime):
        lines.append(f"⏱ <b>ماوە:</b> {esc(runtime)}")
    if is_usable(rating):
        lines.append(f"⭐ <b>ڕەیتینگی IMDb:</b> {esc(rating)}")
    lines.append(f"🌐 <b>ماڵپەڕ:</b> {esc(category)}")
    base_text = "\n".join(lines)

    def build_text(story_limit):
        parts = [base_text]
        if is_usable(story):
            story_text = str(story).strip()
            if len(story_text) > story_limit:
                story_text = story_text[:story_limit].rstrip() + "…"
            rtl_lines = [f"\u200f{ln}" if ln.strip() else ln for ln in story_text.split("\n")]
            parts.append(f"\n📝 <b>کورتەی چیرۆک:</b>\n<blockquote>{esc(chr(10).join(rtl_lines))}</blockquote>")
        return "\n".join(parts)

    full_text = build_text(700)
    caption_text = build_text(700)
    if len(caption_text) > 1024:
        overflow = len(caption_text) - 1024
        caption_text = build_text(max(0, 700 - overflow - 10))

    return {
        "full_text": full_text,
        "caption_text": caption_text,
        "has_poster": is_usable(poster_url),
        "poster_url": poster_url,
    }


def inline_keyboard(url):
    return {"inline_keyboard": [[{"text": "▶️ کردنەوەی لینک", "url": url}]]}


# ------------------------------------------------------------------
# ASYNC Telegram functions
# ------------------------------------------------------------------
async def fetch_poster_bytes_async(session, poster_url):
    try:
        async with session.get(
            poster_url,
            headers={"User-Agent": "Mozilla/5.0 (compatible; KurdFilmBot/1.0)"},
            timeout=aiohttp.ClientTimeout(total=20)
        ) as response:
            if response.status == 200:
                return await response.read()
            return None
    except Exception:
        return None


async def tg_send_photo_async(session, chat_id, photo, caption, keyboard, is_file=False):
    async with _TELEGRAM_SEMAPHORE:
        chat_id_str = str(chat_id)
        if is_file:
            data = aiohttp.FormData()
            data.add_field("chat_id", chat_id_str)
            data.add_field("caption", caption)
            data.add_field("parse_mode", "HTML")
            data.add_field("reply_markup", json.dumps(keyboard))
            data.add_field("photo", photo, filename="poster.jpg", content_type="image/jpeg")
            
            async with session.post(f"{TELEGRAM_API}/sendPhoto", data=data, timeout=aiohttp.ClientTimeout(total=30)) as response:
                result = await response.json()
                if not result.get("ok"):
                    raise RuntimeError(str(result))
                return result
        else:
            payload = {
                "chat_id": chat_id_str,
                "photo": photo,
                "caption": caption,
                "parse_mode": "HTML",
                "reply_markup": json.dumps(keyboard),
            }
            async with session.post(f"{TELEGRAM_API}/sendPhoto", data=payload, timeout=aiohttp.ClientTimeout(total=30)) as response:
                result = await response.json()
                if not result.get("ok"):
                    raise RuntimeError(str(result))
                return result


async def tg_send_message_async(session, chat_id, text, keyboard):
    async with _TELEGRAM_SEMAPHORE:
        chat_id_str = str(chat_id)
        payload = {
            "chat_id": chat_id_str,
            "text": text,
            "parse_mode": "HTML",
            "reply_markup": json.dumps(keyboard),
        }
        async with session.post(f"{TELEGRAM_API}/sendMessage", data=payload, timeout=aiohttp.ClientTimeout(total=20)) as response:
            result = await response.json()
            if not result.get("ok"):
                raise RuntimeError(str(result))
            return result


async def broadcast_new_content_async(title, url, category, item_type, extra, subscribers):
    if not subscribers:
        return

    texts = build_new_content_texts(title, category, item_type, extra)
    keyboard = inline_keyboard(url)
    
    poster_bytes = None
    if texts["has_poster"]:
        async with aiohttp.ClientSession() as session:
            poster_bytes = await fetch_poster_bytes_async(session, texts["poster_url"])
    
    async with aiohttp.ClientSession() as session:
        tasks = [
            send_to_user_async(session, user_id, texts, keyboard, poster_bytes)
            for user_id in subscribers
        ]
        await asyncio.gather(*tasks, return_exceptions=True)


async def send_to_user_async(session, user_id, texts, keyboard, poster_bytes=None):
    try:
        if texts["has_poster"] and poster_bytes:
            await tg_send_photo_async(
                session, user_id, poster_bytes, texts["caption_text"], keyboard, is_file=True
            )
        else:
            await tg_send_message_async(session, user_id, texts["full_text"], keyboard)
        return True
    except Exception:
        return False


async def post_new_content_to_channel_async(content_id, title, url, category, item_type, extra):
    if not CHANNEL_IDS:
        return

    claimed = retry(claim_content_for_channel_post, content_id)
    if not claimed:
        return

    if retry(was_content_recently_channel_posted, title, category):
        return

    texts = build_new_content_texts(title, category, item_type, extra)
    keyboard = inline_keyboard(url)
    
    poster_bytes = None
    if texts["has_poster"]:
        async with aiohttp.ClientSession() as session:
            poster_bytes = await fetch_poster_bytes_async(session, texts["poster_url"])
    
    async with aiohttp.ClientSession() as session:
        for channel_id in CHANNEL_IDS:
            try:
                if texts["has_poster"] and poster_bytes:
                    await tg_send_photo_async(
                        session, channel_id, poster_bytes, texts["caption_text"], keyboard, is_file=True
                    )
                else:
                    await tg_send_message_async(session, channel_id, texts["full_text"], keyboard)
            except Exception as e:
                send_telegram_error(f"❌ هەڵە لە پۆستکردنی بەرهەمی نوێ بۆ کەناڵ {channel_id}: {e}")


# ------------------------------------------------------------------
# Main check execution (Single-run function for Cron)
# ------------------------------------------------------------------
async def check_new_content_once_async():
    last_id_raw = get_setting(STATE_KEY)
    if last_id_raw is None:
        max_id = get_max_content_id()
        set_setting(STATE_KEY, max_id)
        log.info("watcher initialized. last_id = %s", max_id)
        return

    last_id = int(last_id_raw)
    initial_last_id = last_id

    try:
        rows = get_new_content_rows(last_id, limit=50)
        if not rows:
            return

        global_notify_enabled = retry(get_global_notify_enabled)
        group_notify_enabled = retry(get_group_notify_enabled)
        subscribers = retry(get_notify_subscribers) if global_notify_enabled else []
        if not group_notify_enabled:
            subscribers = [uid for uid in subscribers if uid > 0]
        genre_prefs_map = retry(get_all_genre_prefs_map) if subscribers else {}

        max_age = timedelta(hours=MAX_CONTENT_AGE_HOURS)

        for row in rows:
            content_id = row["id"]
            title = row["title"]
            url = row["url"]
            category = row["category"]
            item_type = row["type"]
            skip_notify = row.get("skip_notify")
            created_at = row.get("created_at")

            age = timedelta(0)
            if created_at:
                created_dt = datetime.fromisoformat(created_at.replace("Z", "+00:00"))
                age = datetime.now(timezone.utc) - created_dt

            if skip_notify or age > max_age:
                last_id = content_id
                continue

            extra = {
                "kurdish_title": row.get("kurdish_title"),
                "poster_url": row.get("poster_url"),
                "genre": row.get("genre"),
                "rating": row.get("rating"),
                "runtime": row.get("runtime"),
                "story": row.get("story"),
            }

            await post_new_content_to_channel_async(content_id, title, url, category, item_type, extra)

            if global_notify_enabled and subscribers:
                if genre_prefs_map:
                    row_subscribers = [
                        uid
                        for uid in subscribers
                        if content_matches_genre_prefs(extra["genre"], genre_prefs_map.get(uid, []))
                    ]
                else:
                    row_subscribers = subscribers
                
                if row_subscribers:
                    await broadcast_new_content_async(title, url, category, item_type, extra, row_subscribers)

            last_id = content_id

    except Exception as e:
        send_telegram_error(f"❌ هەڵە لە پشکنینی بەرهەمی نوێ: {e}")
    finally:
        if last_id != initial_last_id:
            set_setting(STATE_KEY, last_id)


def run_checker():
    """Entry point for single execution (can be triggered via Webhook or Cron)"""
    asyncio.run(check_new_content_once_async())


if __name__ == "__main__":
    run_checker()
