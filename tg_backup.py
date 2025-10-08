#!/usr/bin/env python3
import os, re, json, asyncio, tempfile, io, time, traceback, base64
from pathlib import Path
from telethon import TelegramClient
from telethon.errors import SessionPasswordNeededError
from telethon.tl.types import Message, MessageMediaDocument, MessageMediaPhoto
from b2sdk.v2 import InMemoryAccountInfo, B2Api, UploadSourceLocalFile
from b2sdk.v2.exception import NonExistentBucket
import qrcode

# ========= ENV =========
API_ID          = int(os.environ["API_ID"])
API_HASH        = os.environ["API_HASH"]
CHANNEL_ID      = int(os.environ["CHANNEL_ID"])
B2_KEY_ID       = os.environ["B2_KEY_ID"]
B2_APP_KEY      = os.environ["B2_APP_KEY"]
B2_BUCKET       = os.environ["B2_BUCKET"]
MAX_CONCURRENCY = int(os.getenv("MAX_CONCURRENCY", "6"))
TWOFA_PASSWORD  = os.getenv("TELEGRAM_2FA_PASSWORD", "")
DISABLE_B2_EXISTS = os.getenv("DISABLE_B2_EXISTS", "0").lower() in ("1","true","yes")
SESSION_B64     = os.getenv("SESSION_B64", "")

# ========= SESSION RESTORE =========
STATE_DIR = Path("./state"); STATE_DIR.mkdir(parents=True, exist_ok=True)
SESSION_NAME  = str(STATE_DIR / "tg_backup_session")
SESSION_FILE  = Path(SESSION_NAME + ".session")

if SESSION_B64 and not SESSION_FILE.exists():
    try:
        SESSION_FILE.write_bytes(base64.b64decode(SESSION_B64))
        print(">> Restored tg_backup_session.session from SESSION_B64")
    except Exception as e:
        print(">> Failed to restore session from base64:", e)

# ========= STATE (resume) =========
MANIFEST_PATH = STATE_DIR / "manifest.json"
def load_manifest():
    if MANIFEST_PATH.exists():
        try: return json.loads(MANIFEST_PATH.read_text())
        except Exception: pass
    return {"media_ids": [], "last_id": 0}

def save_manifest(m):
    tmp = MANIFEST_PATH.with_suffix(".tmp")
    tmp.write_text(json.dumps(m))
    tmp.replace(MANIFEST_PATH)

manifest = load_manifest()
seen_ids = set(manifest.get("media_ids", []))
resume_from_id = int(manifest.get("last_id", 0))

# ========= Backblaze B2 =========
def b2_connect():
    info = InMemoryAccountInfo()
    api = B2Api(info)
    api.authorize_account("production", B2_KEY_ID, B2_APP_KEY)
    try:
        bucket = api.get_bucket_by_name(B2_BUCKET)
    except NonExistentBucket:
        raise SystemExit(f"[FATAL] Bucket {B2_BUCKET} not found or not permitted.")
    return api, bucket

b2_api, b2_bucket = b2_connect()

def b2_exists(remote_path: str) -> bool:
    if DISABLE_B2_EXISTS: return False
    import os as _os
    dir_name = _os.path.dirname(remote_path)
    if dir_name and not dir_name.endswith('/'):
        dir_name += '/'
    for f, _ in b2_bucket.ls(dir_name, recursive=True):
        if f.file_name == remote_path: return True
    return False

def b2_upload(local_path: Path, remote_path: str):
    src = UploadSourceLocalFile(str(local_path))
    b2_bucket.upload(src, remote_path)

# ========= Hashtags =========
HASHTAG_RE = re.compile(r"#([A-Za-z0-9_]+)")
def first_tag(txt): return HASHTAG_RE.search(txt or "") and HASHTAG_RE.search(txt or "").group(1)
def safe_name(s): return re.sub(r"[^\w\-. ]", "_", s)[:200]
def media_uid(m):
    if isinstance(m.media, MessageMediaDocument) and m.media.document:
        return f"doc_{m.media.document.id}"
    if isinstance(m.media, MessageMediaPhoto) and m.photo:
        return f"pho_{m.photo.id}"

class TagMemory:
    def __init__(self): self.current = "_no_tag"
    def pick(self, caption): 
        t = first_tag(caption or "")
        if t: self.current = t
        return self.current

# ========= LOGIN =========
async def ensure_logged_in(client):
    if await client.is_user_authorized():
        print(">> Already authorized (existing session).")
        return
    print(">> Not authorized. Scan one QR only (it’ll save to session).")
    while True:
        qr_login = await client.qr_login()
        print("QR URL:", qr_login.url)
        print("Scan in Telegram → Settings → Devices → Link Desktop Device")
        try:
            await asyncio.wait_for(qr_login.wait(), timeout=55)
        except asyncio.TimeoutError:
            print(">> QR expired, retrying…")
            continue
        if TWOFA_PASSWORD:
            try:
                await client.sign_in(password=TWOFA_PASSWORD)
            except SessionPasswordNeededError:
                print(">> Wrong or missing 2FA password.")
                continue
        if await client.is_user_authorized():
            print(">> Authorized successfully via QR.")
            return

# ========= DOWNLOAD =========
SEMAPHORE = asyncio.Semaphore(MAX_CONCURRENCY)

async def download_one(client, m, tag_mem):
    if not m or not m.media: return
    uid = media_uid(m)
    if uid and uid in seen_ids: return

    tag = tag_mem.pick(m.message)
    ts = m.date.strftime("%Y%m%d_%H%M%S")
    fname_hint = None
    if isinstance(m.media, MessageMediaDocument) and m.media.document:
        for a in m.media.document.attributes:
            if hasattr(a, "file_name"): fname_hint = a.file_name; break

    async with SEMAPHORE:
        with tempfile.TemporaryDirectory() as td:
            tmp_stem = Path(td)/f"{ts}_msg{m.id}_{safe_name(fname_hint) if fname_hint else ''}"
            try:
                refreshed = await client.get_messages(CHANNEL_ID, ids=m.id)
                saved = await client.download_media(refreshed or m, file=str(tmp_stem))
            except Exception as e:
                print(f"[skip] msg {m.id} failed: {e}")
                return
            if not saved: return
            lp = Path(saved)
            remote_path = f"{tag}/{safe_name(lp.name)}"
            if not DISABLE_B2_EXISTS and b2_exists(remote_path): return
            try: b2_upload(lp, remote_path)
            except Exception as e: print(f"[error] b2 upload {remote_path}: {e}"); return
    if uid: seen_ids.add(uid)
    manifest["media_ids"] = list(seen_ids)
    save_manifest(manifest)

async def main():
    client = TelegramClient(
        SESSION_NAME, API_ID, API_HASH,
        device_model="BackupVM",
        system_version="Linux 5.x",
        app_version="TG-Backup 1.0",
        lang_code="en",
        system_lang_code="en",
    )
    await client.connect()
    await ensure_logged_in(client)

    LIMIT = 200
    total = 0
    tag_mem = TagMemory()
    print(f">> Resume: {len(seen_ids)} files done; resuming from last_id={resume_from_id}")
    last_id = resume_from_id
    last_beat = time.time()

    while True:
        batch = await client.get_messages(CHANNEL_ID, limit=LIMIT, offset_id=last_id, reverse=True)
        if not batch: break
        for m in batch:
            try: await download_one(client, m, tag_mem)
            except Exception as e:
                print(f"[error] msg {m.id}: {e}"); traceback.print_exc()
            total += 1
            if total % 50 == 0:
                print(f">> Processed {total} messages…")
        last_id = batch[-1].id
        manifest["last_id"] = last_id
        save_manifest(manifest)
        if time.time() - last_beat > 10:
            print(f">> Heartbeat: total={total}, last_id={last_id}, time={time.strftime('%H:%M:%S')}")
            last_beat = time.time()
    await client.disconnect()
    print(">> Done. Manifest saved.")

if __name__ == "__main__":
    asyncio.run(main())
