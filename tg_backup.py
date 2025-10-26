#!/usr/bin/env python3
import os, re, json, asyncio, tempfile, time, traceback
from pathlib import Path

from telethon import TelegramClient
from telethon.tl.types import Message, MessageMediaDocument, MessageMediaPhoto
from telethon.errors.common import TypeNotFoundError

# ========= ENV =========
API_ID          = int(os.environ["API_ID"])
API_HASH        = os.environ["API_HASH"]
CHANNEL_ID      = int(os.environ["CHANNEL_ID"])

B2_KEY_ID       = os.environ["B2_KEY_ID"]
B2_APP_KEY      = os.environ["B2_APP_KEY"]
B2_BUCKET       = os.environ["B2_BUCKET"]

MAX_CONCURRENCY = int(os.getenv("MAX_CONCURRENCY", "4"))
DISABLE_B2_EXISTS = os.getenv("DISABLE_B2_EXISTS", "0") in ("1","true","True","YES","yes")

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

# Handle exception import across b2sdk versions
try:
    from b2sdk.v2.exception import NonExistentBucket as BucketNotFound
except Exception:
    try:
        from b2sdk.exception import NonExistentBucket as BucketNotFound
    except Exception:
        class BucketNotFound(Exception):
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

# ========= Session handling =========
async def ensure_logged_in(client: TelegramClient):
    if await client.is_user_authorized():
        print(">> Already authorized (existing session).")
        return
    print(">> Not authorized. Please log in once with make_session_phone.py (phone code).")
    raise SystemExit("Session missing or invalid. Run make_session_phone.py first.")

async def reconnect(client: TelegramClient):
    try:
        await client.disconnect()
    except Exception:
        pass
    await asyncio.sleep(5)
    await client.connect()
    if not await client.is_user_authorized():
        raise SystemExit("Session lost; re-run make_session_phone.py")

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

            # refresh the message handle (helps with grouped media)
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
                except TypeNotFoundError as e:
                    print(f"[warn] schema hiccup on msg {m.id} (attempt {attempt+1}/5): {e}. Reconnecting…")
                    await reconnect(client)
                    try:
                        refreshed = await client.get_messages(CHANNEL_ID, ids=m.id)
                        target_msg = refreshed or m
                    except Exception:
                        target_msg = m
                    await asyncio.sleep(1 + attempt)
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

# ========= Main (NEW → OLD) =========
async def main():
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
        connection_retries=5,
        flood_sleep_threshold=60,
    )
    await client.connect()
    await ensure_logged_in(client)

    tag_mem = TagMemory()
    total = 0
    last_beat = time.time()

    print(">> Starting backup… direction=new2old  (reverse=False)")

    # Iterate newest -> oldest. This avoids scanning through the first N ids.
    # We only process messages that have media.
    try:
        async for m in client.iter_messages(
            CHANNEL_ID,
            limit=None,            # all
            reverse=False,         # NEW -> OLD
        ):
            try:
                await download_one(client, m, tag_mem)
            except Exception as e:
                print(f"[error] unhandled during msg {getattr(m,'id',None)}: {e}")
                traceback.print_exc()

            total += 1
            if total % 50 == 0:
                print(f">> Processed {total} messages…")

            if time.time() - last_beat > 10:
                print(f">> Heartbeat: total={total}, last_id={getattr(m,'id',None)}, time={time.strftime('%H:%M:%S')}")
                last_beat = time.time()

    except TypeNotFoundError as e:
        print(f"[warn] stream schema hiccup: {e}. Reconnecting and continuing…")
        await reconnect(client)
        # Let the process restart (Railway will usually restart on non-zero exit),
        # or simply end here gracefully:
    finally:
        await client.disconnect()

    print(">> Done. Manifest saved.")

if __name__ == "__main__":
    asyncio.run(main())
