#!/bin/bash
set -euo pipefail

# Configure rclone remote for B2
curl -fsSL https://rclone.org/install.sh | bash
rclone config create b2 b2 account "$B2_KEY_ID" key "$B2_APP_KEY" >/dev/null 2>&1 || true

# Pull previous state (resume) from B2 if present
mkdir -p /app/state
rclone copy b2:"$B2_BUCKET/_state" /app/state --ignore-existing || true

# Run backup
python tg_backup.py
