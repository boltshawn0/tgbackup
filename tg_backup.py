#!/usr/bin/env python3
import os, re, json, asyncio, tempfile, io, time, traceback
from pathlib import Path

from telethon import TelegramClient
from telethon.errors import SessionPasswordNeededError
from telethon.tl.types import Message, MessageMediaDocument, MessageMediaPhoto

# 🔒 Force a stable Telegram device identity (helps avoid random logouts)
from telethon.client import auth
auth._client_info = [
    (5, "TengokuBackup"),
    (200, "MacBook Air"),
    (300, "macOS 14"),
    (400, "Stable Backup Client"),
    (500, "en"),
    (600, "CA"),
]

# ========= ENV =========
API_ID          = int(os.environ["API_ID"])
API_HASH        = os.environ["API_HASH"]
CHANNEL_ID      = int(os.environ["CHANNEL_ID"])

B2_KEY_ID       = os.environ["B2_KEY_ID"]
B2_APP_KEY      = os.environ["B2_APP_KEY"]
B2_BUCKET       = os.environ["B2_BUCKET"]

MAX_CONCURRENCY = int(os.getenv("MAX_CONCURRENCY", "4"))
TWOFA_PASSWORD  = os.getenv("TELEGRAM_2FA_PASSWORD", "")  # optional

# NEW: gate processing until we hit this hashtag (no #, case-insensitive)
START_FROM_TAG  = os.getenv("START_FROM_TAG", "").strip()  # e.g. "kaniiberry"

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
from b2sdk.v2 import InMemoryAccountInfo, B2Api, UploadSourceLocalFile

# Handle import across b2sdk versions
try:
    from b2sdk.v2.exception import NonExistentBucket as BucketNotFound
except ImportError:
    try:
        from b2sdk.exception import NonExistentBucket as BucketNotFound
    except ImportError:
        class BucketNotFound(Exception):
            """Fallback dummy exception for b2sdk compatibility"""
            pass

def b2_connect():
    info = InMemoryAccountInfo()
    api = B2Api(info)
    api.authorize_account("production", B2_KEY_ID, B2_APP_KEY)
    try:
        bucket = api.get_bucket_by_name(B2_BUCKET)
    except BucketNotFound:
        raise SystemExit(f"[FATAL] Bucket {B2_BUCKET} not found or not permitted for this key.")
    return api, bucket

b2_api, b2_bucket = b2_connect()

def b2_exists(remote_path: str) -> bool:
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

# ========= Hashtags / tags =========
HASHTAG_RE = re.compile(r"#([A-Za-z0-9_]+)")
def all_tags(text: str | None) -> set[str]:
    if not text:
        return set()
    return {m.lower() for m in HASHTAG_RE.findall(text)}

def first_tag(text: str | None) -> str | None:
    if not text: return None
    m = HASHTAG_RE.search(text)
    return m.group(1) if m else None

def has_tag(text: str | None, tag: str) -> bool:
    if not tag:
        return True
    tag = tag.lower().lstrip("#")
    return tag in all_tags(text)

def safe_name(s: str) -> str:
    return re.sub(r"[^\w\-. ]", "_", s)[:200]

def media_unique_id(m: Message) -> str | None:
    if isinstance(m.media, MessageMediaDocument) and m.media.document:
        return f"doc_{m.media.document.id}"
    if isinstance(m.media, MessageMediaPhoto) and m.photo:
        return f"pho_{m.photo.id}"
    return None

class TagMemory:
    """Carry forward last seen hashtag for grouped/untagged media."""
    def __init__(self):
        self.current = "_no_tag"
    def pick(self, caption: str | None) -> str:
        t = first_tag(caption or "")
        if t:
            self.current = t
        return self.current

# ========= Ensure logged in (expects an existing session file) =========
async def ensure_logged_in(client: TelegramClient):
    if await client.is_user_authorized():
        print(">> Already authorized (existing session).")
        return
    print(">> Not authorized. Please log in once with make_session_phone.py (phone code).")
    raise SystemExit("Session missing or invalid. Run make_session_phone.py first.")

# ========= Download logic =========
SEMAPHORE = asyncio.Semaphore(MAX_CONCURRENCY)

async def download_one(client: TelegramClient, m: Message, tag_mem: TagMemory):
    if not m or not m.media:
        return

    uid = media_unique_id(m)
    if uid and uid in seen_ids:
        return

    caption = m.message or ""
    tag = tag_mem.pick(caption)
    ts  = m.date.strftime("%Y%m%d_%H%M%S")
    base = f"{ts}_msg{m.id}"

    fname_hint = None
    if isinstance(m.media, MessageMediaDocument) and m.media.document:
        for a in m.media.document.attributes:
            if hasattr(a, "file_name"):
                fname_hint = a.file_name
                break

    async with SEMAPHORE:
        with tempfile.TemporaryDirectory() as td:
            tmp_stem = Path(td) / (base + ("_" + safe_name(fname_hint) if fname_hint else ""))

            try:
                refreshed = await client.get_messages(CHANNEL_ID, ids=m.id)
                target_msg = refreshed or m
            except Exception:
                target_msg = m

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
                print(f"[skip] invalid download path for msg {m.id}: {saved_path}")
                return

            remote_path = f"{tag}/{safe_name(local_path.name)}"

            if b2_exists(remote_path):
                if uid:
                    seen_ids.add(uid)
                return

            try:
                b2_upload(local_path, remote_path)
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
    total = 0
    tag_mem = TagMemory()
    print(">> Starting backup… (oldest → newest)")
    last_beat = time.time()

    # start gate: hold off processing until we hit START_FROM_TAG (if set)
    waiting_for_tag = bool(START_FROM_TAG)
    started_from = None  # message id where we started

    max_id = 0
    while True:
        batch = await client.get_messages(CHANNEL_ID, limit=LIMIT, offset_id=max_id, reverse=True)
        if not batch:
            break

        for m in batch:
            if waiting_for_tag:
                if has_tag(m.message, START_FROM_TAG):
                    waiting_for_tag = False
                    started_from = m.id
                    print(f">> Start tag '#{START_FROM_TAG}' found at msg {m.id} — beginning downloads")
                else:
                    # still scanning; skip processing but keep advancing
                    continue

            try:
                await download_one(client, m, tag_mem)
            except Exception as e:
                print(f"[error] unhandled during msg {m.id}: {e}")
                traceback.print_exc()
            total += 1
            if total % 50 == 0:
                print(f">> Processed {total} messages…")

        max_id = batch[-1].id
        if time.time() - last_beat > 10:
            hb = f">> Heartbeat: total={total}, last_id={max_id}, time={time.strftime('%H:%M:%S')}"
            if started_from:
                hb += f" (started_at={started_from})"
            print(hb)
            last_beat = time.time()

    await client.disconnect()
    print(">> Done. Manifest saved.")

if __name__ == "__main__":
    asyncio.run(main())
