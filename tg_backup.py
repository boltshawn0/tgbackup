#!/usr/bin/env python3
import os, re, json, asyncio, tempfile, time, io, traceback
from pathlib import Path

from telethon import TelegramClient
from telethon.tl.types import Message, MessageMediaDocument, MessageMediaPhoto

# ========= ENV =========
API_ID          = int(os.environ["API_ID"])
API_HASH        = os.environ["API_HASH"]
CHANNEL_ID      = int(os.environ["CHANNEL_ID"])

B2_KEY_ID       = os.environ["B2_KEY_ID"]
B2_APP_KEY      = os.environ["B2_APP_KEY"]
B2_BUCKET       = os.environ["B2_BUCKET"]

MAX_CONCURRENCY = int(os.getenv("MAX_CONCURRENCY", "10"))
TWOFA_PASSWORD  = os.getenv("TELEGRAM_2FA_PASSWORD", "")        # optional (not used here)
BACKUP_DIRECTION= os.getenv("BACKUP_DIRECTION", "new2old").lower().strip()  # "new2old" (default) or "old2new"
START_FROM_TAG  = os.getenv("START_FROM_TAG", "").strip()        # optional gate, e.g. "kaniiberry" (no '#')

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
# Robust import for bucket-not-found across b2sdk versions
BucketNotFound = None
try:
    from b2sdk.v2.exception import NonExistentBucket as BucketNotFound  # older path
except Exception:
    try:
        from b2sdk.exception import NonExistentBucket as BucketNotFound  # other path
    except Exception:
        class _BNF(Exception): pass
        BucketNotFound = _BNF

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
    """Check existence by listing the parent prefix; compatible with b2sdk 2.5.0."""
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
    if not text:
        return None
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
    print(">> Not authorized. Please log in once with the phone-code helper (make_session_phone.py).")
    raise SystemExit("Session missing or invalid. Create ./state/tg_backup_session.session first.")

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
    # Use official client identity fields (no private monkey-patch)
    client = TelegramClient(
        SESSION_NAME,
        API_ID,
        API_HASH,
        device_model="BackupWorker",
        system_version="macOS 14",
        app_version="1.0.0",
        lang_code="en",
        system_lang_code="en",
        request_retries=5,
        flood_sleep_threshold=60,
    )
    await client.connect()
    await ensure_logged_in(client)

    LIMIT = None  # stream everything
    total = 0
    tag_mem = TagMemory()

    reverse = (BACKUP_DIRECTION == "old2new")
    print(f">> Starting backup… direction={'old2new' if reverse else 'new2old'}  (reverse={reverse})")
    last_beat = time.time()

    waiting_for_tag = bool(START_FROM_TAG)

    async for m in client.iter_messages(CHANNEL_ID, limit=LIMIT, reverse=reverse):
        if waiting_for_tag:
            if has_tag(getattr(m, "message", None), START_FROM_TAG):
                waiting_for_tag = False
                print(f">> Start tag '#{START_FROM_TAG}' found at msg {m.id} — beginning downloads")
            else:
                continue

        try:
            await download_one(client, m, tag_mem)
        except Exception as e:
            print(f"[error] unhandled during msg {m.id}: {e}")
            traceback.print_exc()

        total += 1
        if total % 50 == 0:
            print(f">> Processed {total} messages…")

        if time.time() - last_beat > 10:
            print(f">> Heartbeat: total={total}, last_id={m.id}, time={time.strftime('%H:%M:%S')}")
            last_beat = time.time()

    await client.disconnect()
    print(">> Done. Manifest saved.")

if __name__ == "__main__":
    asyncio.run(main())
