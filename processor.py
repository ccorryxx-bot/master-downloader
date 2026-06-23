import os
import asyncio
import re
import time
import json
import requests
import boto3
from botocore.config import Config
from telethon import TelegramClient
from telethon.sessions import StringSession

# ── Env ───────────────────────────────────────────────────────────────────────
API_ID               = int(os.environ.get("API_ID", 0))
API_HASH             = os.environ.get("API_HASH", "")
STRING_SESSION       = os.environ.get("STRING_SESSION_1", "")
BOT_TOKEN            = os.environ.get("BOT_TOKEN", "")
E2_ACCESS_KEY_ID     = os.environ.get("E2_ACCESS_KEY_ID", "")
E2_SECRET_ACCESS_KEY = os.environ.get("E2_SECRET_ACCESS_KEY", "")
E2_ENDPOINT          = os.environ.get("E2_ENDPOINT", "")
E2_BUCKET_NAME       = os.environ.get("E2_BUCKET_NAME", "")
E2_REGION            = os.environ.get("E2_REGION", "ap-northeast-1")
CF_AUTH_EMAIL        = os.environ.get("CF_AUTH_EMAIL", "")
CF_AUTH_KEY          = os.environ.get("CF_AUTH_KEY", "")
CF_ACCOUNT_ID        = os.environ.get("CF_ACCOUNT_ID", "")
CF_KV_NAMESPACE_ID   = os.environ.get("CF_KV_NAMESPACE_ID", "")
GITHUB_TOKEN         = os.environ.get("GITHUB_TOKEN", "")
WORKER_URL           = os.environ.get("WORKER_URL", "").rstrip("/")
SUPABASE_URL         = "https://guotpdwaswaybjiiezax.supabase.co"
SUPABASE_KEY         = os.environ.get("SUPABASE_KEY", "")

# ── Workflow Inputs ───────────────────────────────────────────────────────────
MEDIA_LINK           = os.environ.get("MEDIA_LINK", "")
TARGET_CHAT_ID       = os.environ.get("TARGET_CHAT_ID", "")
QUALITY              = os.environ.get("QUALITY", "720")
TASK_ID              = os.environ.get("TASK_ID", "")
CHAIN_TO_CHANNEL     = os.environ.get("CHAIN_TO_CHANNEL", "false").lower() == "true"
CHANNEL_ID           = os.environ.get("CHANNEL_ID", "")
CAPTION_CONFIG_RAW   = os.environ.get("CAPTION_CONFIG", "{}")
WF_INDEX             = int(os.environ.get("WF_INDEX", "1"))
CHANNEL_MANAGER_REPO = os.environ.get("CHANNEL_MANAGER_REPO", "ccorryxx-bot/master-channel-manager")

try:
    CAPTION_CONFIG = json.loads(CAPTION_CONFIG_RAW)
except Exception:
    CAPTION_CONFIG = {}

# ── Supabase ──────────────────────────────────────────────────────────────────
def _sb_hdrs():
    return {"apikey": SUPABASE_KEY, "Authorization": f"Bearer {SUPABASE_KEY}",
            "Content-Type": "application/json", "Prefer": "return=minimal"}

def sb_update_task(status, error_msg=None):
    if not SUPABASE_KEY or not TASK_ID: return
    try:
        body = {"status": status}
        if status in ("completed", "failed"):
            body["end_time"] = time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())
        if error_msg:
            body["error_message"] = error_msg[:500]
        requests.patch(f"{SUPABASE_URL}/rest/v1/tasks?task_id=eq.{TASK_ID}",
                       headers=_sb_hdrs(), json=body, timeout=10)
    except Exception as e:
        print(f"[WARN] sb_update_task: {e}")

def sb_log(level, message):
    if not SUPABASE_KEY or not TASK_ID: return
    try:
        requests.post(f"{SUPABASE_URL}/rest/v1/task_logs", headers=_sb_hdrs(),
                      json={"task_id": TASK_ID, "level": level, "message": message[:500]},
                      timeout=10)
    except Exception as e:
        print(f"[WARN] sb_log: {e}")

# ── Progress ──────────────────────────────────────────────────────────────────
def send_progress(text):
    msg = f"[DL-WF{WF_INDEX}] {text}"
    print(msg)
    sb_log("error" if "❌" in text else "info", text[:400])
    if TARGET_CHAT_ID and WORKER_URL:
        try:
            base = WORKER_URL.rstrip("/")
            url  = base if base.endswith("/progress") else base + "/progress"
            requests.post(url,
                          json={"chat_id": TARGET_CHAT_ID, "progress_text": msg, "task_id": TASK_ID},
                          timeout=10)
        except Exception as e:
            print(f"[WARN] progress push: {e}")

# ── Storage Check ─────────────────────────────────────────────────────────────
def _make_s3():
    return boto3.client("s3", endpoint_url=E2_ENDPOINT,
                        aws_access_key_id=E2_ACCESS_KEY_ID,
                        aws_secret_access_key=E2_SECRET_ACCESS_KEY,
                        region_name=E2_REGION,
                        config=Config(signature_version="s3v4"))

def handle_storage_check():
    send_progress("🗄 IDrive e2 storage စစ်ဆေးနေသည်...")
    try:
        s3 = _make_s3()
        objects = s3.list_objects_v2(Bucket=E2_BUCKET_NAME)
        files = objects.get("Contents", [])
        total_mb = sum(f["Size"] for f in files) / 1024 / 1024
        sorted_files = sorted(files, key=lambda x: x["LastModified"], reverse=True)[:15]
        lines = [f"🗄 *IDrive e2 Storage*\n\n📦 {len(files)} files | {total_mb:.1f} MB\n"]
        for f in sorted_files:
            sz = f["Size"] / 1024 / 1024
            lines.append(f"• `{f['Key'][:50]}` ({sz:.1f} MB)")
        lines.append("\n_💡 /del filename.mp4 — ဖျက်ရန် | /delall — အားလုံးဖျက်_")
        send_progress("\n".join(lines))
        # Send structured filelist JSON so worker can save to KV and render delete buttons
        file_data = [{"key": f["Key"], "size_mb": round(f["Size"]/1024/1024, 1)}
                     for f in sorted_files]
        send_progress(f"__FILELIST__:{json.dumps({'files': file_data, 'total_mb': round(total_mb, 1)})}")
    except Exception as e:
        send_progress(f"❌ Storage error: {e}")

# ── Storage Delete ─────────────────────────────────────────────────────────────
def handle_storage_delete(file_key):
    send_progress(f"🗑 Deleting: `{file_key}`...")
    try:
        s3 = _make_s3()
        s3.delete_object(Bucket=E2_BUCKET_NAME, Key=file_key)
        send_progress(f"🎉 Deleted: `{file_key}`")
    except Exception as e:
        send_progress(f"❌ Delete failed: {e}")

def handle_storage_delete_all():
    send_progress("🗑 Deleting all files...")
    try:
        s3 = _make_s3()
        objects = s3.list_objects_v2(Bucket=E2_BUCKET_NAME)
        files = objects.get("Contents", [])
        if not files:
            send_progress("📭 Storage empty — ဖျက်ရမည့် file မရှိပါ")
            return
        for f in files:
            s3.delete_object(Bucket=E2_BUCKET_NAME, Key=f["Key"])
        send_progress(f"🎉 Deleted {len(files)} files — Storage ကို ရှင်းလင်းပြီ")
    except Exception as e:
        send_progress(f"❌ Delete all failed: {e}")

# ── Telegram Link Parser ──────────────────────────────────────────────────────
def parse_tg_link(link):
    m = re.match(r"https?://t\.me/c/(\d+)/(\d+)", link)
    if m:
        return int(f"-100{m.group(1)}"), int(m.group(2))
    m2 = re.match(r"https?://t\.me/([a-zA-Z0-9_]+)/(\d+)", link)
    if m2:
        return m2.group(1), int(m2.group(2))
    return None, None

# ── IDrive e2 Upload ──────────────────────────────────────────────────────────
def upload_to_e2(local_path, key):
    file_size = os.path.getsize(local_path)
    send_progress(f"☁️ Uploading to IDrive e2... ({file_size // 1024 // 1024}MB)")
    s3 = boto3.client("s3", endpoint_url=E2_ENDPOINT,
                      aws_access_key_id=E2_ACCESS_KEY_ID,
                      aws_secret_access_key=E2_SECRET_ACCESS_KEY,
                      region_name=E2_REGION,
                      config=Config(signature_version="s3v4"))
    uploaded = [0]
    last_pct = [0]

    def _cb(n):
        uploaded[0] += n
        pct = uploaded[0] / file_size * 100
        if pct - last_pct[0] >= 10:
            last_pct[0] = pct
            send_progress(f"☁️ Upload {pct:.0f}% ({uploaded[0]//1024//1024}MB/{file_size//1024//1024}MB)")

    s3.upload_file(local_path, E2_BUCKET_NAME, key, Callback=_cb)

    # Presigned URL valid 6 hours
    url = s3.generate_presigned_url(
        "get_object",
        Params={"Bucket": E2_BUCKET_NAME, "Key": key},
        ExpiresIn=21600
    )
    send_progress(f"✅ Upload complete — presigned URL ready (6h)")
    return url

# ── Chain to Channel Manager ──────────────────────────────────────────────────
def chain_to_channel_manager(video_url):
    send_progress("🔗 Channel Manager ကို chain လုပ်နေသည်...")
    events = ["process_video_v1", "process_video_v2", "process_video_v3",
              "process_video_v4", "process_video_v5"]
    event = events[(WF_INDEX - 1) % 5]

    payload = {
        "video_url":       video_url,
        "target_channel_id": CHANNEL_ID,
        "photo_caption":   CAPTION_CONFIG.get("photo_caption", "auto"),
        "video_caption":   CAPTION_CONFIG.get("video_caption", "auto"),
        "num_photos":      CAPTION_CONFIG.get("num_photos", "auto"),
        "post_mode":       CAPTION_CONFIG.get("post_mode", "auto"),
        "chat_id":         TARGET_CHAT_ID,
        "worker_url":      WORKER_URL,
        "task_id":         TASK_ID,
        "channel_alias":   CAPTION_CONFIG.get("channel_alias", "default"),
    }

    r = requests.post(
        f"https://api.github.com/repos/{CHANNEL_MANAGER_REPO}/dispatches",
        headers={"Authorization": f"token {GITHUB_TOKEN}",
                 "Accept": "application/vnd.github.v3+json",
                 "Content-Type": "application/json",
                 "User-Agent": "master-downloader"},
        json={"event_type": event, "client_payload": payload},
        timeout=30
    )
    if r.status_code == 204:
        send_progress(f"✅ Channel Manager ({event}) dispatched! Step 2 started.")
        return True
    else:
        send_progress(f"❌ Chain dispatch failed: HTTP {r.status_code} — {r.text[:200]}")
        sb_update_task("failed", f"chain dispatch failed: {r.status_code}")
        return False

# ── Main ──────────────────────────────────────────────────────────────────────
async def main():
    import traceback

    # Special modes: storage operations
    if MEDIA_LINK == "storage_check":
        handle_storage_check()
        return
    if MEDIA_LINK.startswith("storage_delete:"):
        file_key = MEDIA_LINK[len("storage_delete:"):]
        handle_storage_delete(file_key)
        return
    if MEDIA_LINK == "storage_delete_all":
        handle_storage_delete_all()
        return

    # Preflight
    missing = [k for k, v in {
        "API_ID": API_ID, "API_HASH": API_HASH, "STRING_SESSION_1": STRING_SESSION,
        "E2_ACCESS_KEY_ID": E2_ACCESS_KEY_ID, "E2_SECRET_ACCESS_KEY": E2_SECRET_ACCESS_KEY,
        "E2_ENDPOINT": E2_ENDPOINT, "E2_BUCKET_NAME": E2_BUCKET_NAME,
        "MEDIA_LINK": MEDIA_LINK,
    }.items() if not v or v == 0]
    if missing:
        send_progress(f"❌ Missing env vars: {', '.join(missing)}")
        sb_update_task("failed", f"Missing: {', '.join(missing)}")
        return

    chat_id, msg_id = parse_tg_link(MEDIA_LINK)
    if not chat_id or not msg_id:
        send_progress(f"❌ Invalid Telegram link: {MEDIA_LINK}")
        sb_update_task("failed", "Invalid Telegram link")
        return

    sb_update_task("running")
    send_progress(f"🚀 Download start | chat={chat_id} msg={msg_id} quality={QUALITY}")

    local_path = f"/tmp/dl_{TASK_ID}.mp4"
    e2_key     = f"dl_{TASK_ID}.mp4"

    try:
        # ── Telegram download ─────────────────────────────────────────────────
        send_progress("📡 Telegram session connecting...")
        client = TelegramClient(StringSession(STRING_SESSION), API_ID, API_HASH,
                                connection_retries=None, request_retries=5)
        await client.connect()

        if not await client.is_user_authorized():
            send_progress("❌ Session not authorized — TG_STRING_SESSION check လုပ်ပါ")
            sb_update_task("failed", "Session not authorized")
            return

        send_progress("✅ Connected — message fetch...")
        msg = await client.get_messages(chat_id, ids=msg_id)

        if not msg or not msg.media:
            send_progress(f"❌ Message {msg_id} has no downloadable media")
            sb_update_task("failed", "No media in message")
            await client.disconnect()
            return

        # File size estimate
        file_size_b = 0
        if hasattr(msg.media, "document") and msg.media.document:
            file_size_b = msg.media.document.size
        send_progress(f"📥 Downloading {file_size_b // 1024 // 1024}MB from Telegram...")

        last_pct = [0]
        def dl_progress(current, total):
            if total:
                pct = current / total * 100
                if pct - last_pct[0] >= 10:
                    last_pct[0] = pct
                    send_progress(f"📥 Download {pct:.0f}% ({current//1024//1024}MB/{total//1024//1024}MB)")

        await client.download_media(msg, file=local_path, progress_callback=dl_progress)
        await client.disconnect()

        if not os.path.exists(local_path) or os.path.getsize(local_path) == 0:
            send_progress("❌ Download failed — empty file")
            sb_update_task("failed", "Download empty file")
            return

        actual_mb = os.path.getsize(local_path) / 1024 / 1024
        send_progress(f"✅ Downloaded {actual_mb:.0f}MB")

        # ── Upload to IDrive e2 ───────────────────────────────────────────────
        presigned_url = upload_to_e2(local_path, e2_key)

        # Cleanup local
        try: os.remove(local_path)
        except: pass

        # ── Chain to Channel Manager ──────────────────────────────────────────
        if CHAIN_TO_CHANNEL and CHANNEL_ID:
            chain_to_channel_manager(presigned_url)
            # Task status will be updated by Channel Manager (completed/failed)
        else:
            send_progress(f"✅ Done! e2 key: {e2_key} (chain disabled)")
            sb_update_task("completed")

    except Exception as e:
        send_progress(f"❌ Error: {str(e)[:200]}")
        print(f"[ERROR] {traceback.format_exc()}")
        sb_update_task("failed", str(e)[:300])
        if os.path.exists(local_path):
            try: os.remove(local_path)
            except: pass

if __name__ == "__main__":
    asyncio.run(main())
