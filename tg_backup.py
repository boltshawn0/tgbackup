import os, re, json, asyncio, tempfile, io
from pathlib import Path
from datetime import datetime

from telethon import TelegramClient
from telethon.tl.types import Message, MessageMediaDocument, MessageMediaPhoto

# Backblaze B2 SDK
from b2sdk.v2 import InMemoryAccountInfo, B2Api
from b2sdk.v2 import UploadSourceLocalFile
from b2sdk.v2.exception import NonExistentBucket

# Optional QR in logs
import qrcode

# ========= ENV =========
API_ID        = int(os.environ["API_ID"])
API_HASH      = os.environ["API_HASH"]
PHONE_NUMBER  = os.environ["PHONE_NUMBER"]           # used after QR if needed
CHANNEL_ID    = int(os.environ["CHANNEL_ID"])
B2_KEY_ID     = os.environ["B2_KEY_ID"]
B2_APP_KEY    = os.environ["B2_APP_KEY"]
B2_BUCKET     = os.environ["B2_BUCKET"]
MAX_CONCURRENCY = int(os.getenv("MAX_CONCURRENCY", "4"))

# ========= STATE (for resume) =========
STATE_DIR = Path("./state")
STATE_DIR.mkdir(parents=True, exist_ok=True)
MANIFEST_PATH = STATE_DIR / "manifest.json"
SESSION_NAME  = str(STATE_DIR / "tg_backup_session")

def load_manifest():
    if MANIFEST_PATH.exists():
        try: return json.loads(MANIFEST_PATH.read_text())
        except: pass
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
    except NonExistentBucket:
        raise SystemExit(f"[FATAL] Bucket {B2_BUCKET} not found in this key scope.")
    return api, bucket

b2_api, b2_bucket = b2_connect()

def b2_exists(remote_path: str) -> bool:
    # remote_path like "tag/filename.ext"
    # We'll check by listing that exact name
    for f, _ in b2_bucket.ls(remote_path, show_versions=False, recursive=False):
        if f.file_name == remote_path:
            return True
    return False

def b2_upload(local_path: Path, remote_path: str):
    # remote_path = "<tag>/<filename>"
    src = UploadSourceLocalFile(str(local_path))
    b2_bucket.upload(src, remote_path)

# ========= Helpers =========
HASHTAG_RE = re.compile(r"#([A-Za-z0-9_]+)")

def first_tag(text: str | None) -> str:
    if not text: return "_no_tag"
    m = HASHTAG_RE.search(text)
    return m.group(1) if m else "_no_tag"

def safe_name(s: str) -> str:
    return re.sub(r"[^\w\-. ]", "_", s)[:200]

def media_unique_id(m: Message) -> str | None:
    if isinstance(m.media, MessageMediaDocument) and m.media.document:
        return f"doc_{m.media.document.id}"
    if isinstance(m.media, MessageMediaPhoto) and m.photo:
        return f"pho_{m.photo.id}"
    return None

# ========= QR login (no interactive typing) =========
async def ensure_logged_in(client: TelegramClient):
    if await client.is_user_authorized():
        return
    print(">> Not authorized. Showing Telegram QR in logs. Open your Telegram app → Settings → Devices → Scan QR")
    qr_login = await client.qr_login()
    try:
        # Render QR to terminal as ASCII
        img = qrcode.make(qr_login.url)
        buf = io.StringIO()
        img.print_ascii(out=buf)  # type: ignore
        print(buf.getvalue())
    except Exception:
        # fallback: just print the URL (you can convert to QR on your phone)
        print("QR URL:", qr_login.url)

    await qr_login.wait()  # wait until you scan and approve
    print(">> Authorized successfully via QR.")

# ========= Download logic =========
SEMAPHORE = asyncio.Semaphore(MAX_CONCURRENCY)

async def download_one(client: TelegramClient, m: Message):
    if not m or not m.media:
        return

    uid = media_unique_id(m)
    if uid and uid in seen_ids:
        return

    tag = first_tag(m.message if m.message else "")
    ts  = m.date.strftime("%Y%m%d_%H%M%S")
    base = f"{ts}_msg{m.id}"

    # original filename hint
    fname_hint = None
    if isinstance(m.media, MessageMediaDocument) and m.media.document:
        for a in m.media.document.attributes:
            if hasattr(a, "file_name"):
                fname_hint = a.file_name
                break
    if fname_hint:
        base += "_" + safe_name(fname_hint)

    remote_path = f"{tag}/{base}"
    if b2_exists(remote_path):
        if uid: seen_ids.add(uid)
        return

    async with SEMAPHORE:
        with tempfile.TemporaryDirectory() as td:
            tmp = Path(td) / base
            # no size cap: allow >1.5GB
            for attempt in range(5):
                try:
                    await client.download_media(m, file=str(tmp))
                    break
                except Exception as e:
                    print(f"[warn] retry {attempt+1} on msg {m.id}: {e}")
                    await asyncio.sleep(2 + attempt * 2)
            else:
                print(f"[skip] failed to download msg {m.id}")
                return

            try:
                b2_upload(tmp, remote_path)
            except Exception as e:
                print(f"[error] b2 upload failed for {remote_path}: {e}")
                return

    if uid:
        seen_ids.add(uid)
    manifest["media_ids"] = list(seen_ids)
    save_manifest(manifest)

async def main():
    client = TelegramClient(SESSION_NAME, API_ID, API_HASH)
    await client.connect()
    await ensure_logged_in(client)

    LIMIT = 200
    offset_id = 0
    total = 0
    print(">> Starting backup…")

    while True:
        batch = await client.get_messages(CHANNEL_ID, limit=LIMIT, offset_id=offset_id)
        if not batch:
            break
        for m in reversed(batch):     # oldest → newest
            await download_one(client, m)
            total += 1
            if total % 50 == 0:
                print(f">> Processed {total} messages…")
        offset_id = batch[-1].id

    await client.disconnect()
    print(">> Done. Manifest saved.")

if __name__ == "__main__":
    asyncio.run(main())
