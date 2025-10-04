#!/bin/bash
rclone config create b2 b2 account "$B2_KEY_ID" key "$B2_APP_KEY"
python tg_backup.py
