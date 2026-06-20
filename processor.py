import os
import asyncio
import re
import time
import json
import hashlib
import requests
import subprocess
import boto3
from botocore.config import Config
from telethon import TelegramClient
from telethon.sessions import StringSession

# ── Env ───────────────────────────────────────────────────────────────────────
API_ID             = int(os.environ.get("API_ID", 0))
API_HASH           = os.environ.get("API_HASH", "")
STRING_SESSION     = os.environ.get("STRING_SESSION_1", "")
BOT_TOKEN          = os.environ.get("BOT_TOKEN", "")
E2_ACCESS_KEY_ID   = os.environ.get("E2_ACCESS_KEY_ID", "")
E2_SECRET_ACCESS_KEY = os.environ.get("E2_SECRET_ACCESS_KEY", "")
E2_ENDPOINT        = os.environ.get("E2_ENDPOINT", "")
E2_BUCKET_NAME     = os.environ.get("E2_BUCKET_NAME", "")
E2_REGION          = os.environ.get("E2_REGION", "ap-northeast-1")
CF_AUTH_EMAIL      = os.environ.get("CF_AUTH_EMAIL", "")
CF_AUTH_KEY        = os.environ.get("CF_AUTH_KEY", "")
CF_ACCOUNT_ID      = os.environ.get("CF_ACCOUNT_ID", "")
CF_KV_NAMESPACE_ID = os.environ.get("CF_KV_NAMESPACE_ID", "")
GITHUB_TOKEN       = os.environ.get("GITHUB_TOKEN", "")
WORKER_URL         = os.environ.get("WORKER_URL", "")

# ── S3 Client ─────────────────────────────────────────────────────────────────
def get_s3():
    return boto3.client(
        "s3",
        endpoint_url=f"https://{E2_ENDPOINT}" if not E2_ENDPOINT.startswith("http") else E2_ENDPOINT,
        aws_access_key_id=E2_ACCESS_KEY_ID,
        aws_secret_access_key=E2_SECRET_ACCESS_KEY,
        region_name=E2_REGION,
        config=Config(signature_version="s3v4", retries={"max_attempts": 3}),
    )

# ── Telegram Bot API ───────────────────────────────────────────────────────────
def send_tg(method, payload, retries=3):
    for attempt in range(retries):
        try:
            r = requests.post(
                f"https://api.telegram.org/bot{BOT_TOKEN}/{method}",
                json=payload, timeout=15
            )
            if r.status_code == 200:
                return r.json()
            print(f"[WARN] TG {method} attempt {attempt+1}: {r.status_code} {r.text[:100]}")
        except Exception as e:
            print(f"[WARN] TG {method} attempt {attempt+1}: {e}")
        time.sleep(2)
    return None

def send_msg(chat_id, text, parse_mode="Markdown"):
    return send_tg("sendMessage", {"chat_id": chat_id, "text": text, "parse_mode": parse_mode})

def edit_msg(chat_id, msg_id, text, keyboard=None):
    payload = {"chat_id": chat_id, "message_id": msg_id, "text": text, "parse_mode": "Markdown"}
    if keyboard:
        payload["reply_markup"] = {"inline_keyboard": keyboard}
    return send_tg("editMessageText", payload)

# ── CF KV ─────────────────────────────────────────────────────────────────────
KV_BASE = lambda: f"https://api.cloudflare.com/client/v4/accounts/{CF_ACCOUNT_ID}/storage/kv/namespaces/{CF_KV_NAMESPACE_ID}"
CF_HEADERS = lambda: {"X-Auth-Email": CF_AUTH_EMAIL, "X-Auth-Key": CF_AUTH_KEY, "Content-Type": "application/json"}

def kv_put(key, value, expiration_ttl=None):
    params = {}
    if expiration_ttl:
        params["expiration_ttl"] = expiration_ttl
    headers = {"X-Auth-Email": CF_AUTH_EMAIL, "X-Auth-Key": CF_AUTH_KEY}
    r = requests.put(
        f"{KV_BASE()}/values/{key}",
        headers=headers,
        data=value if isinstance(value, str) else json.dumps(value),
        params=params, timeout=15
    )
    if r.status_code not in (200, 204):
        print(f"[WARN] kv_put '{key}': {r.status_code} {r.text[:80]}")

def kv_get(key):
    r = requests.get(f"{KV_BASE()}/values/{key}", headers=CF_HEADERS(), timeout=15)
    if r.status_code == 200:
        try:
            return r.json()
        except Exception:
            return r.text
    return None

def kv_delete(key):
    r = requests.delete(f"{KV_BASE()}/values/{key}", headers=CF_HEADERS(), timeout=15)
    return r.status_code in (200, 204)

async def get_kv_tasks():
    all_tasks = []
    for prefix in ["task:", "cmd:"]:
        r = requests.get(f"{KV_BASE()}/keys?prefix={prefix}", headers=CF_HEADERS(), timeout=30)
        if r.status_code == 401:
            raise Exception("CF KV 401 — check CF_AUTH_EMAIL / CF_AUTH_KEY")
        r.raise_for_status()
        for k in r.json().get("result", []):
            key_name = k["name"]
            val_r = requests.get(f"{KV_BASE()}/values/{key_name}", headers=CF_HEADERS(), timeout=30)
            if val_r.status_code != 200:
                continue
            try:
                val = val_r.json()
            except Exception:
                continue
            all_tasks.append({"key": key_name, "data": val})
            kv_delete(key_name)
    return all_tasks

# ── Progress Reporter ─────────────────────────────────────────────────────────
class Progress:
    def __init__(self, chat_id, msg_id, filename="..."):
        self.chat_id = chat_id
        self.msg_id = msg_id
        self.filename = filename
        self.last_update = 0
        self.start = time.time()

    def bar(self, pct):
        done = int(pct / 10)
        return "█" * done + "░" * (10 - done)

    def update(self, current, total, action="Downloading"):
        now = time.time()
        if now - self.last_update < 5 and current < total:
            return
        self.last_update = now
        pct = (current / total * 100) if total else 0
        elapsed = now - self.start
        speed = current / elapsed if elapsed > 0 else 0
        eta = time.strftime("%M:%S", time.gmtime((total - current) / speed)) if speed > 0 else "--:--"
        edit_msg(self.chat_id, self.msg_id,
            f"⏳ *{action}*\n"
            f"📄 `{self.filename}`\n"
            f"`[{self.bar(pct)}] {pct:.1f}%`\n"
            f"⏱️ ETA: `{eta}`"
        )

# ── Dispatch Channel Manager ──────────────────────────────────────────────────
def dispatch_channel_manager(payload, wf_index=1):
    wf_events = {1: "process_video_v1", 2: "process_video_v2", 3: "process_video_v3",
                 4: "process_video_v4", 5: "process_video_v5"}
    event_type = wf_events.get(wf_index, "process_video_v1")
    repo = os.environ.get("CHANNEL_MANAGER_REPO", "ccorryxx-bot/master-channel-manager")

    r = requests.post(
        f"https://api.github.com/repos/{repo}/dispatches",
        headers={"Authorization": f"token {GITHUB_TOKEN}", "Accept": "application/vnd.github.v3+json"},
        json={"event_type": event_type, "client_payload": payload},
        timeout=15
    )
    if r.status_code == 204:
        print(f"[INFO] Channel manager dispatched (event={event_type})")
        return True
    print(f"[WARN] Dispatch failed: {r.status_code} {r.text[:100]}")
    return False

# ── Storage Commands ───────────────────────────────────────────────────────────
async def process_storage_command(cmd_data):
    chat_id  = cmd_data.get("chatId")
    msg_id   = cmd_data.get("msgId")
    cmd_type = cmd_data.get("type")
    s3 = get_s3()

    def reply(text, keyboard=None):
        if msg_id:
            edit_msg(chat_id, msg_id, text, keyboard)
        else:
            send_msg(chat_id, text)

    if cmd_type == "list_storage":
        try:
            paginator = s3.get_paginator("list_objects_v2")
            files = []
            for page in paginator.paginate(Bucket=E2_BUCKET_NAME):
                files.extend(page.get("Contents", []))

            if not files:
                reply("📊 *IDrive e2 Storage*\n\n✅ Bucket ဗလာ — ဖိုင်မရှိပါ")
                return

            total_bytes = sum(f.get("Size", 0) for f in files)
            total_gb = total_bytes / 1024 / 1024 / 1024
            total_mb = total_bytes / 1024 / 1024

            filelist = [{"key": f["Key"], "size": f.get("Size", 0)} for f in files]
            kv_put(f"filelist:{chat_id}", json.dumps(filelist))

            lines = [
                f"📊 *IDrive e2 Storage*\n",
                f"📁 ဖိုင်: `{len(files)}`",
                f"💾 Storage: `{total_gb:.3f} GB ({total_mb:.1f} MB)`\n",
            ]
            for i, f in enumerate(files[:15]):
                sz = f.get("Size", 0) / 1024 / 1024
                key = f["Key"]
                short = key[:28] + "…" if len(key) > 28 else key
                mod = f.get("LastModified")
                age_h = (time.time() - mod.timestamp()) / 3600 if mod else 0
                lines.append(f"`{i+1}.` 📄 `{short}`\n    `{sz:.0f}MB` | `{age_h:.1f}h ago`")

            if len(files) > 15:
                lines.append(f"\n_...နောက်ထပ် {len(files)-15} ဖိုင်_")

            keyboard = []
            for i, f in enumerate(files[:15]):
                sz = f.get("Size", 0) / 1024 / 1024
                short = f["Key"][:20] + "…" if len(f["Key"]) > 20 else f["Key"]
                keyboard.append([{"text": f"🗑 {short} ({sz:.0f}MB)", "callback_data": f"del|{i}|{chat_id}"}])
            keyboard.append([{"text": f"💣 အကုန်ဖျက် ({len(files)} files)", "callback_data": f"delete_all_confirm|{chat_id}|{len(files)}"}])
            keyboard.append([{"text": "🔄 Refresh", "callback_data": "storage_refresh"}])
            reply("\n".join(lines), keyboard)

        except Exception as e:
            reply(f"❌ `{e}`")

    elif cmd_type == "delete":
        file_key = cmd_data.get("fileKey")
        try:
            s3.delete_object(Bucket=E2_BUCKET_NAME, Key=file_key)
            short = file_key[:40] + "…" if len(file_key) > 40 else file_key
            reply(f"✅ *ဖျက်ပြီး*\n\n`{short}`")
        except Exception as e:
            reply(f"❌ `{e}`")

    elif cmd_type == "delete_all":
        try:
            paginator = s3.get_paginator("list_objects_v2")
            all_keys = []
            for page in paginator.paginate(Bucket=E2_BUCKET_NAME):
                for obj in page.get("Contents", []):
                    all_keys.append({"Key": obj["Key"]})

            if not all_keys:
                reply("✅ Bucket ဗလာ — ဖျက်စရာမရှိ")
                return

            total = len(all_keys)
            reply(f"💣 *{total} ဖိုင် ဖျက်နေသည်...*")
            deleted, errors = 0, []
            for i in range(0, len(all_keys), 1000):
                batch = all_keys[i:i+1000]
                resp = s3.delete_objects(Bucket=E2_BUCKET_NAME, Delete={"Objects": batch, "Quiet": True})
                deleted += len(batch) - len(resp.get("Errors", []))
                errors.extend(resp.get("Errors", []))

            if errors:
                reply(f"⚠️ ဖျက်ပြီး: `{deleted}` | မဖျက်ရ: `{len(errors)}`")
            else:
                reply(f"💣 *ဖျက်ပြီးပါပြီ!*\n\n✅ ဖိုင် `{deleted}` ခု အကုန် ဖျက်သွားပြီ")
        except Exception as e:
            reply(f"❌ `{e}`")

# ── Download Task ─────────────────────────────────────────────────────────────
async def process_task(client, task):
    data        = task["data"]
    chat_id     = data["chatId"]
    media_link  = data["mediaLink"]
    msg_id      = data["statusMessageId"]
    quality     = data.get("quality", "720")
    task_id     = data.get("taskId", "")
    chain       = data.get("chainToChannel", False)
    channel_id  = data.get("channelId", "")
    caption_cfg = data.get("captionConfig", {})
    wf_index    = data.get("wfIndex", 1)

    file_prefix = f"dl_{int(time.time())}_{hashlib.md5(media_link.encode()).hexdigest()[:6]}"
    file_path = None
    reporter = Progress(chat_id, msg_id, "Extracting info...")

    try:
        # ── Download ──────────────────────────────────────────────────────────
        if "t.me/" in media_link:
            match = re.search(r't\.me/(?:c/)?([^/]+)/(\d+)', media_link)
            if not match:
                raise Exception("Invalid Telegram link format")
            chat_id_raw, m_id = match.group(1), int(match.group(2))
            target = int(f"-100{chat_id_raw}") if chat_id_raw.isdigit() else chat_id_raw
            entity = await client.get_entity(target)
            msg = await client.get_messages(entity, ids=m_id)
            if not msg or not msg.file:
                raise Exception("No downloadable file in this message")
            reporter.filename = msg.file.name or (file_prefix + ".mp4")
            file_path = await client.download_media(
                msg, file_prefix,
                progress_callback=lambda c, t: reporter.update(c, t)
            )
        else:
            # yt-dlp for external URLs
            q_format = {"360": "bestvideo[height<=360]+bestaudio/best[height<=360]",
                        "480": "bestvideo[height<=480]+bestaudio/best[height<=480]",
                        "720": "bestvideo[height<=720]+bestaudio/best[height<=720]"}.get(quality, "bestvideo+bestaudio/best")
            info_raw = subprocess.check_output(["yt-dlp", "--dump-json", media_link], timeout=60).decode()
            info = json.loads(info_raw)
            reporter.filename = (info.get("title", "video")[:30] + ".mp4")
            out_path = file_prefix + ".mp4"
            result = subprocess.run([
                "yt-dlp", "-f", q_format, "--merge-output-format", "mp4",
                "-o", out_path, media_link
            ], timeout=7200)
            if result.returncode != 0:
                raise Exception("yt-dlp download failed")
            file_path = out_path

        if not file_path or not os.path.exists(file_path):
            raise Exception(f"Output file not found: {file_path}")
        if os.path.getsize(file_path) == 0:
            raise Exception("Output file is empty")

        file_size_mb = os.path.getsize(file_path) / 1024 / 1024
        object_key = os.path.basename(file_path)

        # ── Upload to IDrive e2 ───────────────────────────────────────────────
        edit_msg(chat_id, msg_id, f"📤 *Uploading to e2...*\n📄 `{reporter.filename}`\n💾 `{file_size_mb:.1f} MB`")
        s3 = get_s3()
        file_size = os.path.getsize(file_path)
        if file_size < 4_900 * 1024 * 1024:
            with open(file_path, "rb") as f:
                s3.put_object(Bucket=E2_BUCKET_NAME, Key=object_key, Body=f)
        else:
            from boto3.s3.transfer import TransferConfig
            tc = TransferConfig(multipart_threshold=5*1024**3, multipart_chunksize=100*1024*1024)
            s3.upload_file(file_path, E2_BUCKET_NAME, object_key, Config=tc)

        # Presigned URL (7 days)
        url = s3.generate_presigned_url(
            "get_object",
            Params={"Bucket": E2_BUCKET_NAME, "Key": object_key},
            ExpiresIn=604800
        )

        # Store result in KV for status tracking
        if task_id:
            kv_put(f"result:{task_id}", json.dumps({
                "url": url, "filename": reporter.filename,
                "size_mb": round(file_size_mb, 1), "object_key": object_key
            }), expiration_ttl=3600)

        # ── Chain to channel manager ──────────────────────────────────────────
        if chain and channel_id:
            edit_msg(chat_id, msg_id,
                f"✅ *Downloaded!*\n📄 `{reporter.filename}`\n💾 `{file_size_mb:.1f} MB`\n\n"
                f"🚀 Channel manager ကို pass လုပ်နေသည်..."
            )
            payload = {
                "video_url": url,
                "target_channel_id": channel_id,
                "chat_id": str(chat_id),
                "worker_url": WORKER_URL + "/progress" if WORKER_URL else "",
                "task_id": task_id,
                **caption_cfg
            }
            dispatched = dispatch_channel_manager(payload, wf_index)
            if not dispatched:
                edit_msg(chat_id, msg_id,
                    f"✅ *Downloaded!*\n📄 `{reporter.filename}`\n💾 `{file_size_mb:.1f} MB`\n\n"
                    f"🔗 [Direct Link]({url})\n\n"
                    f"⚠️ Channel manager dispatch မအောင်မြင် — link ကိုသာ သုံးပါ"
                )
        else:
            edit_msg(chat_id, msg_id,
                f"✅ *Download Complete!*\n\n"
                f"📄 `{reporter.filename}`\n"
                f"💾 `{file_size_mb:.1f} MB`\n"
                f"🔗 [Direct Link]({url})\n\n"
                f"⏰ Link ၇ ရက် valid"
            )

    except Exception as e:
        print(f"[ERROR] Task error: {e}")
        edit_msg(chat_id, msg_id, f"❌ *Download Failed*\n\n`{str(e)[:300]}`")
    finally:
        if file_path and os.path.exists(file_path):
            os.remove(file_path)

# ── Main ──────────────────────────────────────────────────────────────────────
async def main():
    missing = [k for k, v in {
        "STRING_SESSION_1": STRING_SESSION, "API_ID": str(API_ID),
        "API_HASH": API_HASH, "BOT_TOKEN": BOT_TOKEN,
        "E2_ACCESS_KEY_ID": E2_ACCESS_KEY_ID, "E2_SECRET_ACCESS_KEY": E2_SECRET_ACCESS_KEY,
        "E2_ENDPOINT": E2_ENDPOINT, "E2_BUCKET_NAME": E2_BUCKET_NAME,
        "CF_AUTH_EMAIL": CF_AUTH_EMAIL, "CF_AUTH_KEY": CF_AUTH_KEY,
        "CF_ACCOUNT_ID": CF_ACCOUNT_ID, "CF_KV_NAMESPACE_ID": CF_KV_NAMESPACE_ID,
    }.items() if not v or v == "0"]
    if missing:
        raise Exception(f"Missing secrets: {', '.join(missing)}")

    client = TelegramClient(StringSession(STRING_SESSION), API_ID, API_HASH)
    await client.connect()
    print("[INFO] Master Downloader started. Polling CF KV...")

    empty_polls = 0
    while empty_polls < 12:
        tasks = await get_kv_tasks()
        if tasks:
            empty_polls = 0
            print(f"[INFO] {len(tasks)} task(s) found")
            for task in tasks:
                if task["key"].startswith("cmd:"):
                    await process_storage_command(task["data"])
                else:
                    await process_task(client, task)
        else:
            empty_polls += 1
            print(f"[INFO] Empty poll {empty_polls}/12 — sleeping 30s")
            await asyncio.sleep(30)

    print("[INFO] Exiting after 12 empty polls")
    await client.disconnect()

if __name__ == "__main__":
    asyncio.run(main())
