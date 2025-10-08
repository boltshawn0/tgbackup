#!/usr/bin/env python3
import os, re, json, asyncio, tempfile, io, time, traceback
from pathlib import Path
from typing import Optional

from telethon import TelegramClient
from telethon.errors import SessionPasswordNeededError
from telethon.tl.types import Message, MessageMediaDocument, MessageMediaPhoto

# Backblaze B2 SDK
from b2sdk.v2 import InMemoryAccountInfo, B2Api, UploadSourceLocalFile
from b2sdk.v2.exception import NonExistentBucket

# ========= ENV =========
API_ID            = int(os.environ["API_ID"])
API_HASH          = os.environ["API_HASH"]
CHANNEL_ID        = int(os.environ["CHANNEL_ID"])
B2_KEY_ID         = os.environ["B2_KEY_ID"]
B2_APP_KEY        = os.environ["B2_APP_KEY"]
B2_BUCKET         = os.environ["B2_BUCKET"]
MAX_CONCURRENCY   = int(os.getenv("MAX_CONCURRENCY", "4"))
TWOFA_PASSWORD    = os.getenv("TELEGRAM_2FA_PASSWORD", "")
DISABLE_B2_EXISTS = os.getenv("DISABLE_B2_EXISTS", "0") in ("1","true","True","YES","yes")

# ========= STATE (session + resume) =========
STATE_DIR = Path("./state"); STATE_DIR.mkdir(parents=True, exist_ok=True)
MANIFEST_PATH = STATE_DIR / "manifest.json"
SESSION_NAME  = str(STATE_DIR / "tg_backup_session")  # file: ./state/tg_backup_session.session

def load_manifest():
    if MANIFEST_PATH.exists():
        try:
            m = json.loads(MANIFEST_PATH.read_text())
            # Back-compat defaults
            m.setdefault("media_ids", [])
            m.setdefault("last_id", 0)
            return m
        except Exception:
            pass
    return {"media_ids": [], "last_id": 0}

def save_manifest(m):
    tmp = MANIFEST_PATH.with_suffix(".tmp")
    tmp.write_text(json.dumps(m))
    tmp.replace(MANIFEST_PATH)

manifest = load_manifest()
seen_ids = set(manifest.get("media_ids", []))
last_id  = int(manifest.get("last_id", 0))

# ========= Backblaze B2 =========
def b2_connect():
    info = InMemoryAccountInfo()
    api = B2Api(info)
    api.authorize_account("production", B2_KEY_ID, B2_APP_KEY)
    try:
        bucket = api.get_bucket_by_name(B2_BUCKET)
    except NonExistentBucket:
        raise SystemExit(f"[FATAL] Bucket {B2_BUCKET} not found or not permitted for this key.")
    return api, bucket

b2_api, b2_bucket = b2_connect()

def b2_exists(remote_path: str) -> bool:
    """
    Portable existence check across b2sdk versions:
    list the directory prefix and compare exact file_name.
    remote_path like "tag/filename.ext"
    """
    if DISABLE_B2_EXISTS:
        return False
    import os as _os
    dir_name = _os.path.dirname(remote_path)
    if dir_name and not dir_name.endswith('/'):
        dir_name += '/'
    # iterate through a small prefix; if big folders, this is slower — disable via env if needed
    for file_version, _ in b2_bucket.ls(dir_name, recursive=True):
        if file_version.file_name == remote_path:
            return True
    return False

def b2_upload(local_path: Path, remote_path: str):
    src = UploadSourceLocalFile(str(local_path))
    b2_bucket.upload(src, remote_path)

# ========= Hashtags / naming =========
HASHTAG_RE = re.compile(r"#([A-Za-z0-9_]+)")

def first_tag(text: Optional[str]) -> Optional[str]:
    if not text: return None
    m = HASHTAG_RE.search(text)
    return m.group(1) if m else None

def safe_name(s: str) -> str:
    return re.sub(r"[^\w\-. ]", "_", s)[:200]

def media_unique_id(m: Message) -> Optional[str]:
    if isinstance(m.media, MessageMediaDocument) and m.media.document:
        return f"doc_{m.media.document.id}"
    if isinstance(m.media, MessageMediaPhoto) and m.photo:
        return f"pho_{m.photo.id}"
    return None

class TagMemory:
    """Carry forward the last seen hashtag for subsequent untagged messages."""
    def __init__(self):
        self.current = "_no_tag"
    def pick(self, caption: Optional[str]) -> str:
        t = first_tag(caption or "")
        if t:
            self.current = t
        return self.current

# ========= QR login (auto-refresh if no valid session) =========
async def ensure_logged_in(client: TelegramClient):
    if await client.is_user_authorized():
        print(">> Already authorized (existing session).")
        return

    print(">> Not authorized. I will keep refreshing the QR login until you scan it.")
    while True:
        qr_login = await client.qr_login()
        print("QR URL:", qr_login.url)
        print("Open on phone: Telegram → Settings → Devices → Link Desktop Device → Scan (within ~60s)")
        try:
            # QR token ~60s — wait slightly less so we can refresh
            await asyncio.wait_for(qr_login.wait(), timeout=55)
        except asyncio.TimeoutError:
            print(">> QR expired. Generating a new one…")
            continue

        # 2FA if needed
        try:
            if not await client.is_user_authorized():
                if TWOFA_PASSWORD:
                    try:
                        await client.sign_in(password=TWOFA_PASSWORD)
                    except SessionPasswordNeededError:
                        raise
                if not await client.is_user_authorized():
                    print(">> Login incomplete (2FA?). Set TELEGRAM_2FA_PASSWORD or disable 2FA temporarily.")
                    continue
        except SessionPasswordNeededError:
            print(">> 2FA password required but TELEGRAM_2FA_PASSWORD not set.")
            continue

        print(">> Authorized successfully via QR.")
        return

# ========= Download one =========
SEMAPHORE = asyncio.Semaphore(MAX_CONCURRENCY)

async def download_one(client: TelegramClient, m: Message, tag_mem: TagMemory):
    if not m or not m.media:
        return

    uid = media_unique_id(m)
    if uid and uid in seen_ids:
        return  # already done by ID

    caption = m.message if m.message else ""
    tag = tag_mem.pick(caption)  # carry forward last seen hashtag
    ts  = m.date.strftime("%Y%m%d_%H%M%S")
    base = f"{ts}_msg{m.id}"

    # filename hint (optional; final name comes from Telethon's saved file)
    fname_hint = None
    if isinstance(m.media, MessageMediaDocument) and m.media.document:
        for a in m.media.document.attributes:
            if hasattr(a, "file_name"):
                fname_hint = a.file_name
                break

    async with SEMAPHORE:
        with tempfile.TemporaryDirectory() as td:
            tmp_stem = Path(td) / (base + ("_" + safe_name(fname_hint) if fname_hint else ""))

            # Refresh expired file reference before downloading
            try:
                refreshed = await client.get_messages(CHANNEL_ID, ids=m.id)
                target_msg = refreshed or m
            except Exception:
                target_msg = m

            # Download and capture the actual saved path
            saved_path = None
            for attempt in range(5):
                try:
                    saved_path = await client.download_media(target_msg, file=str(tmp_stem))
                    break
                except Exception as e:
                    print(f"[warn] retry {attempt+1} on msg {m.id}: {e}")
                    await asyncio.sleep(2 + attempt * 2)
            if not saved_path:
                print(f"[skip] failed to download msg {m.id}")
                return

            local_path = Path(saved_path)
            if not local_path.exists() or local_path.is_dir():
                print(f"[skip] download returned invalid path for msg {m.id}: {saved_path}")
                return

            # Final remote path uses the *real* filename with extension
            remote_path = f"{tag}/{safe_name(local_path.name)}"

            # Optional: remote existence check
            if b2_exists(remote_path):
                if uid:
                    seen_ids.add(uid)
                return

            try:
                b2_upload(local_path, remote_path)
            except Exception as e:
                print(f"[error] b2 upload failed for {remote_path}: {e}")
                return

    # Mark as done
    if uid:
        seen_ids.add(uid)

# ========= Main (oldest → newest with exact resume) =========
async def main():
    # Stable device fingerprint helps avoid suspicious-session bans/logouts
    client = TelegramClient(
        SESSION_NAME,
        API_ID,
        API_HASH,
        device_model="BackupWorker",
        system_version="iOS 16.6",
        app_version="Telethon 1.34",
        lang_code="en",
        system_lang_code="en",
        request_retries=5,
        connection_retries=10,
        retry_delay=2
    )

    await client.connect()
    await ensure_logged_in(client)

    LIMIT = 300   # per page when we “fast-forward” the window
    tag_mem = TagMemory()
    total = 0
    heartbeat_t = time.time()

    # If last_id == 0 we truly start at the very oldest. Otherwise continue from last_id+1
    start_from = int(manifest.get("last_id", 0))
    if start_from > 0:
        print(f">> Resuming at message id > {start_from} (oldest → newest)")
    else:
        print(">> Starting backup… (oldest → newest)")

    # We walk forward in time using offset_id, reverse=True
    offset_id = start_from
    while True:
        batch = await client.get_messages(
            CHANNEL_ID,
            limit=LIMIT,
            offset_id=offset_id,
            reverse=True,  # oldest → newest within this page
        )
        if not batch:
            break

        for m in batch:
            try:
                await download_one(client, m, tag_mem)
            except Exception as e:
                print(f"[error] unhandled during msg {m.id}: {e}")
                traceback.print_exc()

            # Update resume point *after* attempting the download
            manifest["last_id"] = m.id
            if len(seen_ids) != len(manifest.get("media_ids", [])):
                manifest["media_ids"] = list(seen_ids)
            save_manifest(manifest)

            total += 1
            if total % 50 == 0:
                print(f">> Processed {total} messages…")

            # heartbeat
            if time.time() - heartbeat_t > 10:
                print(f">> Heartbeat: total={total}, last_id={manifest['last_id']}, time={time.strftime('%H:%M:%S')}")
                heartbeat_t = time.time()

        # advance the window
        offset_id = batch[-1].id

    await client.disconnect()
    print(">> Done. Manifest saved. last_id =", manifest["last_id"])

if __name__ == "__main__":
    asyncio.run(main())
