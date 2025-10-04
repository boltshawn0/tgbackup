import os, re, json, asyncio, tempfile, subprocess, shlex
from pathlib import Path
from datetime import datetime
from telethon import TelegramClient
from telethon.tl.types import Message, MessageMediaDocument, MessageMediaPhoto

# ====== ENV ======
API_ID = int(os.environ["API_ID"])
API_HASH = os.environ["API_HASH"]
PHONE = os.environ["PHONE_NUMBER"]
CHANNEL_ID = int(os.environ["CHANNEL_ID"])
B2_BUCKET = os.environ["B2_BUCKET"]
MAX_CONCURRENCY = int(os.getenv("MAX_CONCURRENCY", "4"))

# Local state lives here; we sync it to B2:_state/
APP_DIR = Path("/app")
STATE_DIR = APP_DIR / "state"
STATE_DIR.mkdir(parents=True, exist_ok=True)
MANIFEST_PATH = STATE_DIR / "manifest.json"
SESSION_NAME = str(STATE_DIR / "tg_backup_session")

# ====== Manifest (downloaded media IDs) ======
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
save_counter = 0

# ====== Helpers ======
HASHTAG_RE = re.compile(r"#([A-Za-z0-9_]+)")

def first_tag(text: str | None) -> str:
    if not text: return "_no_tag"
    m = HASHTAG_RE.search(text)
    return m.group(1) if m else "_no_tag"

def safe_name(s: str) -> str:
    return re.sub(r"[^\w\-. ]", "_", s)[:200]

def rclone_copy(local_path: Path, remote_path: str) -> None:
    cmd = f'rclone copy "{local_path}" "{remote_path}" --ignore-existing --transfers 8'
    subprocess.run(cmd, shell=True, check=True)

def rclone_exists(remote_path: str) -> bool:
    # Use rclone lsjson; returns [] if not found
    cmd = f'rclone lsjson "{remote_path}"'
    p = subprocess.run(cmd, shell=True, capture_output=True, text=True)
    if p.returncode != 0:
        return False
    try:
        data = json.loads(p.stdout.strip() or "[]")
        # If remote_path points to a single file, lsjson returns a list with 1 item
        return isinstance(data, list) and len(data) > 0
    except Exception:
        return False

async def save_state_to_b2():
    # Push manifest + session to B2:_state/
    remote_state = f"b2:{B2_BUCKET}/_state"
    try:
        rclone_copy(MANIFEST_PATH, remote_state)
    except Exception:
        pass
    # session file(s)
    for f in STATE_DIR.glob("tg_backup_session*"):
        try:
            rclone_copy(f, remote_state)
        except Exception:
            pass

def media_unique_id(m: Message) -> str | None:
    if isinstance(m.media, MessageMediaDocument) and m.media.document:
        # Telegram Document has a stable id
        return f"doc_{m.media.document.id}"
    if isinstance(m.media, MessageMediaPhoto) and m.photo:
        return f"pho_{m.photo.id}"
    return None

# ====== Download logic ======
SEMAPHORE = asyncio.Semaphore(MAX_CONCURRENCY)

async def download_one(client: TelegramClient, m: Message):
    global save_counter
    if not m or not m.media:
        return

    uid = media_unique_id(m)
    if uid and uid in seen_ids:
        return  # already done

    # Build remote target folder from first hashtag
    tag = first_tag(m.message if m.message else "")
    # filename base
    ts = m.date.strftime("%Y%m%d_%H%M%S")
    base = f"{ts}_msg{m.id}"

    # Try to get original filename
    fname_hint = None
    if isinstance(m.media, MessageMediaDocument) and m.media.document:
        for a in m.media.document.attributes:
            if hasattr(a, "file_name"):
                fname_hint = a.file_name
                break
    if fname_hint:
        base += "_" + safe_name(fname_hint)

    # We let Telethon pick extension; store to a temp file
    remote_dir = f"b2:{B2_BUCKET}/{tag}"
    # If a file with same constructed name already exists remotely, skip
    remote_obj = f'{remote_dir}/{base}'
    if rclone_exists(remote_obj):
        if uid:
            seen_ids.add(uid)
        return

    async with SEMAPHORE:
        # Download to temp
        with tempfile.TemporaryDirectory() as td:
            tmpfile = Path(td) / base
            # Retry a few times on network hiccups
            for attempt in range(5):
                try:
                    await client.download_media(m, file=str(tmpfile))
                    break
                except Exception:
                    await asyncio.sleep(2 + attempt * 2)
            else:
                return  # failed

            # Upload to B2 and remove local temp
            try:
                rclone_copy(tmpfile, remote_dir)
            except Exception:
                return

    # Mark as done
    if uid:
        seen_ids.add(uid)
    manifest["media_ids"] = list(seen_ids)
    save_manifest(manifest)
    save_counter += 1
    if save_counter % 25 == 0:
        await save_state_to_b2()

async def main():
    client = TelegramClient(SESSION_NAME, API_ID, API_HASH)
    await client.start(phone=PHONE)

    # After login, push session/manifest so we can resume if Railway restarts
    await save_state_to_b2()

    LIMIT = 200
    offset_id = 0
    total = 0

    while True:
        batch = await client.get_messages(CHANNEL_ID, limit=LIMIT, offset_id=offset_id)
        if not batch:
            break
        # process oldest -> newest
        for m in reversed(batch):
            await download_one(client, m)
            total += 1
        offset_id = batch[-1].id

    # Final state sync
    await save_state_to_b2()
    await client.disconnect()

if __name__ == "__main__":
    asyncio.run(main())
