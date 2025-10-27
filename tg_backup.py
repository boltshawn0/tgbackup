#!/usr/bin/env python3
import os, re, json, asyncio, tempfile, time, traceback
from pathlib import Path

from telethon import TelegramClient
from telethon.tl.types import Message, MessageMediaDocument, MessageMediaPhoto

# ========= ENV =========
API_ID            = int(os.environ["API_ID"])
API_HASH          = os.environ["API_HASH"]
CHANNEL_ID        = int(os.environ["CHANNEL_ID"])

B2_KEY_ID         = os.environ["B2_KEY_ID"]
B2_APP_KEY        = os.environ["B2_APP_KEY"]
B2_BUCKET         = os.environ["B2_BUCKET"]

# parallelism
MAX_CONCURRENCY   = int(os.getenv("MAX_CONCURRENCY", "6"))      # per-file download concurrency guard
MAX_INFLIGHT      = int(os.getenv("MAX_INFLIGHT", str(MAX_CONCURRENCY)))  # how many messages we work on at once

# optional controls
DISABLE_B2_EXISTS = os.getenv("DISABLE_B2_EXISTS", "0").lower() in ("1","true","yes")
START_FROM_TAG    = os.getenv("START_FROM_TAG", "").strip()      # e.g. cinnanoe (no '#')
START_FROM_MSG_ID = os.getenv("START_FROM_MSG_ID", "").strip()   # numeric string to override the tag

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

# Handle NonExistentBucket across b2sdk versions
BucketNotFound = None
for path in ("b2sdk.v2.exception", "b2sdk.exception"):
    try:
        mod = __import__(path, fromlist=["NonExistentBucket"])
        BucketNotFound = getattr(mod, "NonExistentBucket")
        break
    except Exception:
        pass
if BucketNotFound is None:
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

# ========= Tags / filenames / ids =========
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

# ========= Login (expects an existing session file) =========
async def ensure_logged_in(client: TelegramClient):
    if await client.is_user_authorized():
        print(">> Already authorized (existing session).")
        return
    print(">> Not authorized. Please log in once with your phone-code script to create the session.")
    raise SystemExit("Session missing or invalid.")

# ========= Helper: find start point from tag / msg id =========
async def resolve_start_from_tag_id(client: TelegramClient):
    if not START_FROM_TAG:
        return None
    try:
        res = await client.get_messages(
            CHANNEL_ID,
            search=f"#{START_FROM_TAG}",
            limit=1
        )
        if res and len(res) > 0 and res[0]:
            print(f">> START_FROM_TAG found: #{START_FROM_TAG} at msg {res[0].id}")
            return res[0].id
        else:
            print(f">> START_FROM_TAG not found in channel: #{START_FROM_TAG}")
            return None
    except Exception as e:
        print(f">> START_FROM_TAG lookup failed: {e}")
        return None

def parse_start_msg_id():
    if not START_FROM_MSG_ID:
        return None
    try:
        return int(START_FROM_MSG_ID)
    except Exception:
        return None

# ========= Per-message download =========
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

            # try fetching a fresh view of the msg to avoid stale refs
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
                except asyncio.CancelledError:
                    raise
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

# ========= Main =========
async def main():
    client = TelegramClient(SESSION_NAME, API_ID, API_HASH)
    await client.connect()
    await ensure_logged_in(client)

    tag_mem = TagMemory()
    total = 0
    last_beat = time.time()

    # Direction: NEW -> OLD (reverse=False)
    start_id = parse_start_msg_id()
    if start_id is None:
        start_id = await resolve_start_from_tag_id(client)

    print(">> Starting backup… direction=new2old  (reverse=False)")
    if start_id:
        print(f">> Starting from msg id {start_id}")

    inflight = set()

    async for m in client.iter_messages(
        CHANNEL_ID,
        limit=None,
        reverse=False,          # new -> old
        offset_id=start_id or 0 # 0 if None
    ):
        try:
            t = asyncio.create_task(download_one(client, m, tag_mem))
            inflight.add(t)
            # backpressure
            if len(inflight) >= MAX_INFLIGHT:
                done, inflight = await asyncio.wait(inflight, return_when=asyncio.FIRST_COMPLETED)
        except Exception as e:
            print(f"[error] scheduling msg {getattr(m,'id', '?')}: {e}")
            traceback.print_exc()

        total += 1
        if time.time() - last_beat > 10:
            print(f">> Heartbeat: total={total}, last_id={m.id}, time={time.strftime('%H:%M:%S')}")
            last_beat = time.time()

    # drain remaining tasks
    if inflight:
        await asyncio.gather(*inflight, return_exceptions=True)

    await client.disconnect()
    print(">> Done. Manifest saved.")

if __name__ == "__main__":
    asyncio.run(main())
