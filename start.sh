#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")"
if ! command -v ffmpeg >/dev/null || ! command -v ffprobe >/dev/null; then
  echo "Нужен ffmpeg. Установите: sudo apt install ffmpeg"
  exit 1
fi
export PYTHONUNBUFFERED=1
exec python3 -u ./app.py "$@"
