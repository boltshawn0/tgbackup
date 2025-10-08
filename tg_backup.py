#!/usr/bin/env python3
import os, re, json, asyncio, tempfile, io, time, traceback
from pathlib import Path

from telethon import TelegramClient, functions
from telethon.errors import (
    SessionPasswordNeededError,
    AuthKeyUnregisteredError,
    SessionRevokedError,
    PhoneCodeInvalidError,
)
from telethon.tl.types import Message, MessageMediaDocument, MessageMediaPhoto

# Backblaze B2 SDK
from b2sdk.v2 import InMemoryAccountInfo, B2Api, UploadSourceLocalFile
try:
    # Older/newer b2sdk place this in different spots; we’ll handle both.
    from b2sdk.v2.exception import NonExistentBucket
except Exception:
    from b2sdk.v2 import B2Error as NonExistentBucket  # fallback; we won’t actually raise it

# QR
import qrcode

# ========= ENV =========
API_ID             = int(os.environ["API_ID"])
API_HASH           = os.environ["API_HASH"]
CHANNEL_ID         = int(os.environ["CHANNEL_ID"])
B2_KEY_ID          = os.environ["B2_KEY_ID"]
B2_APP_KEY         = os.environ["B2_APP_KEY"]
B2_BUCKET          = os.environ["B2_BUCKET"]
MAX_CONCURRENCY    = int(os.getenv("MAX_CONCURRENCY", "4"))
TWOFA_PASSWORD     = os.getenv("TELEGRAM_2FA_PASSWORD", "")        # optional
PHONE_NUMBER       = os.environ.get("PHONE_NUMBER", "")             # optional; not used unless you add phone login
DISABLE_B2_EXISTS  = os.getenv("DISABLE_B2_EXISTS", "0").lower() in ("1","true","yes")

# ========= STATE (resume) =========
STATE_DIR = Path("./state"); STATE_DIR.mkdir(parents=True, exist_ok=True)
MANIFEST_PATH = STATE_DIR / "manifest.json"
SESSION_NAME  = str(STATE_DIR / "tg_backup_session")  # -> ./state/tg_backup_session.session

def load_manifest():
    if MANIFEST_PATH.exists():
        try:
            return json.loads(MANIFEST_PATH.read_text())
        except Exception:
            pass
    return {"media_ids": []}

def save_manifest(m):
    tmp = MANIFEST_PATH.with_suffix(".tmp")
    tmp.write_text(json.dumps(m))
    tmp.replace(MANIFEST_PATH)

manifest = load_manifest()
seen_ids = set(manifest.get("media_ids", []))

# ========= Backblaze B2 =========
def b2_connect():
    info = InMemoryAccountInfo()
    api = B2Api(info)
    api.authorize_account("production", B2_KEY_ID, B2_APP_KEY)
    try:
        bucket = api.get_bucket_by_name(B2_BUCKET)
    except Exception as e:
        raise SystemExit(f"[FATAL] Bucket {B2_BUCKET} not found or not permitted for this key: {e}")
    return api, bucket

b2_api, b2_bucket = b2_connect()

def b2_exists(remote_path: str) -> bool:
    """
    Portable existence check across b2sdk versions.
    Lists by directory prefix and compares exact file_name.
    """
    if DISABLE_B2_EXISTS:
        return False
    import os as _os
    dir_name = _os.path.dirname(remote_path)
    if dir_name and not dir_name.endswith('/'):
        dir_name += '/'
    for file_version, _ in b2_bucket.ls(dir_name, recursive=True):
        if file_version.file_name == remote_path:
            return True
    return False

def b2_upload(local_path: Path, remote_path: str):
    src = UploadSourceLocalFile(str(local_path))
    b2_bucket.upload(src, remote_path)

# ========= Hashtags / names =========
HASHTAG_RE = re.compile(r"#([A-Za-z0-9_]+)")

def first_tag(text: str | None) -> str | None:
    if not text: return None
    m = HASHTAG_RE.search(text)
    return m.group(1) if m else None

def safe_name(s: str) -> str:
    return re.sub(r"[^\w\-. ]", "_", s)[:200]

def media_unique_id(m: Message) -> str | None:
    if isinstance(m.media, MessageMediaDocument) and m.media.document:
        return f"doc_{m.media.document.id}"
    if isinstance(m.media, MessageMediaPhoto) and m.photo:
        return f"pho_{m.photo.id}"
    return None

class TagMemory:
    """Carry forward the last seen hashtag for subsequent untagged messages."""
    def __init__(self):
        self.current = "_no_tag"
    def pick(self, caption: str | None) -> str:
        t = first_tag(caption or "")
        if t:
            self.current = t
        return self.current

# ========= Login helpers =========
async def qr_loop_login(client: TelegramClient):
    print(">> Not authorized. I will keep refreshing the QR login until you scan it.")
    while True:
        qr_login = await client.qr_login()
        try:
            img = qrcode.make(qr_login.url)
            buf = io.StringIO(); img.print_ascii(out=buf)  # type: ignore
            print(buf.getvalue())
        except Exception:
            pass
        print("QR URL:", qr_login.url)
        print("Open on phone: Telegram → Settings → Devices → Link Desktop Device → Scan QR Code (scan within ~60s)")
        try:
            # QR token ~60s; wait slightly less so we can refresh smoothly
            await asyncio.wait_for(qr_login.wait(), timeout=55)
        except asyncio.TimeoutError:
            print(">> QR expired. Generating a new one…")
            continue

        # Handle 2FA if needed (rare for QR but keep it here)
        try:
            if not await client.is_user_authorized():
                if TWOFA_PASSWORD:
                    try:
                        await client.sign_in(password=TWOFA_PASSWORD)
                    except SessionPasswordNeededError:
                        raise
                if not await client.is_user_authorized():
                    print(">> Login still incomplete (2FA likely). Set TELEGRAM_2FA_PASSWORD or disable 2FA temporarily.")
                    continue
        except SessionPasswordNeededError:
            print(">> 2FA password required but TELEGRAM_2FA_PASSWORD not set. Add it and restart.")
            continue

        print(">> Authorized successfully via QR.")
        return

async def ensure_logged_in(client: TelegramClient):
    # If a valid session exists (./state/tg_backup_session.session), this is instant.
    if await client.is_user_authorized():
        print(">> Already authorized (existing session).")
        return
    await qr_loop_login(client)

# ========= Download logic =========
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

            # Refresh expired file reference before downloading (helps with those “file reference expired” retries)
            try:
                refreshed = await client.get_messages(CHANNEL_ID, ids=m.id)
                target_msg = refreshed or m
            except Exception:
                target_msg = m

            # Download and capture the actual saved path (Telethon returns it)
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
    manifest["media_ids"] = list(seen_ids)
    save_manifest(manifest)

async def drain_oldest_to_newest(client: TelegramClient):
    LIMIT = 200     # can raise to 500
    total = 0
    tag_mem = TagMemory()
    print(">> Starting backup… (oldest → newest)")
    last_beat = time.time()

    # OLD → NEW traversal (reverse=True = ascending chronological order)
    max_id = 0
    while True:
        batch = await client.get_messages(
            CHANNEL_ID,
            limit=LIMIT,
            offset_id=max_id,
            reverse=True,   # key: oldest → newest
        )
        if not batch:
            break

        for m in batch:
            try:
                await download_one(client, m, tag_mem)
            except Exception as e:
                print(f"[error] unhandled during msg {m.id}: {e}")
                traceback.print_exc()
            total += 1
            if total % 50 == 0:
                print(f">> Processed {total} messages…")

        # Advance window forward
        max_id = batch[-1].id

        # Heartbeat every ~10s
        if time.time() - last_beat > 10:
            print(f">> Heartbeat: total={total}, last_id={max_id}, time={time.strftime('%H:%M:%S')}")
            last_beat = time.time()

# ========= Main with robust re-login on revocation =========
async def run_once():
    # Setting device/app metadata helps Telegram see this as a steady desktop client
    client = TelegramClient(
        SESSION_NAME, API_ID, API_HASH,
        device_model="MacBook Air",
        system_version="macOS 14",
        app_version="Telegram Desktop 4.16",
        lang_code="en",
        system_lang_code="en-US",
        # Retry config
        connection_retries=None,     # keep retrying network problems
        retry_delay=2,
        flood_sleep_threshold=120,   # auto-sleep on FloodWaits up to 2 minutes
    )

    await client.connect()
    await ensure_logged_in(client)
    try:
        await drain_oldest_to_newest(client)
    finally:
        await client.disconnect()

async def main():
    while True:
        try:
            await run_once()
            print(">> Done. Manifest saved.")
            break
        except (SessionRevokedError, AuthKeyUnregisteredError) as e:
            # Your session was invalidated (e.g., “log out all sessions”)
            print(f"[warn] Session was revoked: {e}. Re-authenticating with QR…")
            # Loop will continue and run_once() will call ensure_logged_in() again
            await asyncio.sleep(3)
            continue
        except PhoneCodeInvalidError:
            print("[error] Phone code invalid. If you switched to phone login, rerun and enter the correct code.")
            raise
        except Exception as e:
            print(f"[error] Top-level exception: {e}")
            traceback.print_exc()
            # Backoff, then try to re-establish in case of transient network/container issues
            await asyncio.sleep(5)
            continue

if __name__ == "__main__":
    asyncio.run(main())
