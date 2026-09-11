#!/usr/bin/env python3
"""Локальный веб-конвертер: MP4 → MKV без перекодирования (remux)."""

from __future__ import annotations

import argparse
import json
import os
import queue
import shutil
import subprocess
import sys
import threading
import time
import webbrowser
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from urllib.parse import parse_qs, unquote, urlparse

ROOT = Path(__file__).resolve().parent
DEFAULT_DIR = Path("/home/kot9/Загрузки/YouTube")
VIDEO_EXT = {".mp4", ".m4v", ".mov", ".m4a", ".ts", ".m2ts", ".webm"}
HOST = "127.0.0.1"
PORT = 8765

_probe_cache: dict[str, tuple[float, int, dict]] = {}
_probe_lock = threading.Lock()


def json_bytes(data) -> bytes:
    return json.dumps(data, ensure_ascii=False).encode("utf-8")


def format_size(n: int) -> str:
    units = ["Б", "КБ", "МБ", "ГБ", "ТБ"]
    size = float(n)
    for unit in units:
        if size < 1024 or unit == units[-1]:
            if unit == "Б":
                return f"{int(size)} {unit}"
            return f"{size:.1f} {unit}"
        size /= 1024
    return f"{n} Б"


def format_duration(seconds: float | None) -> str | None:
    if seconds is None or seconds < 0:
        return None
    total = int(round(seconds))
    h, rem = divmod(total, 3600)
    m, s = divmod(rem, 60)
    if h:
        return f"{h}:{m:02d}:{s:02d}"
    return f"{m}:{s:02d}"


def is_video(path: Path) -> bool:
    return path.is_file() and path.suffix.lower() in VIDEO_EXT


def probe_media(path: Path) -> dict:
    key = str(path)
    try:
        st = path.stat()
    except OSError as exc:
        return {"ok": False, "error": str(exc)}

    with _probe_lock:
        cached = _probe_cache.get(key)
        if cached and cached[0] == st.st_mtime and cached[1] == st.st_size:
            return cached[2]

    result = {
        "ok": False,
        "error": None,
        "duration": None,
        "duration_label": None,
        "width": None,
        "height": None,
        "video_codec": None,
        "audio_codec": None,
        "label": None,
    }
    try:
        proc = subprocess.run(
            [
                "ffprobe",
                "-v",
                "error",
                "-print_format",
                "json",
                "-show_format",
                "-show_streams",
                str(path),
            ],
            capture_output=True,
            text=True,
            timeout=20,
        )
    except (subprocess.TimeoutExpired, FileNotFoundError) as exc:
        result["error"] = str(exc)
        return result

    if proc.returncode != 0:
        err = (proc.stderr or "").strip()
        if "moov atom not found" in err:
            result["error"] = "файл не докачан или повреждён (нет moov)"
        else:
            result["error"] = err.splitlines()[-1] if err else "не удалось прочитать файл"
        with _probe_lock:
            _probe_cache[key] = (st.st_mtime, st.st_size, result)
        return result

    try:
        data = json.loads(proc.stdout or "{}")
    except json.JSONDecodeError:
        result["error"] = "ffprobe вернул некорректные данные"
        return result

    fmt = data.get("format") or {}
    try:
        duration = float(fmt.get("duration") or 0) or None
    except (TypeError, ValueError):
        duration = None
    result["duration"] = duration
    result["duration_label"] = format_duration(duration)

    video = next((s for s in data.get("streams") or [] if s.get("codec_type") == "video"), None)
    audio = next((s for s in data.get("streams") or [] if s.get("codec_type") == "audio"), None)
    if video:
        result["width"] = video.get("width")
        result["height"] = video.get("height")
        result["video_codec"] = (video.get("codec_name") or "").upper()
    if audio:
        result["audio_codec"] = (audio.get("codec_name") or "").upper()

    parts = []
    if result["width"] and result["height"]:
        parts.append(f"{result['width']}×{result['height']}")
    if result["video_codec"]:
        parts.append(result["video_codec"])
    if result["audio_codec"]:
        parts.append(result["audio_codec"])
    if result["duration_label"]:
        parts.append(result["duration_label"])
    result["label"] = " · ".join(parts) if parts else None
    result["ok"] = True
    with _probe_lock:
        _probe_cache[key] = (st.st_mtime, st.st_size, result)
    return result


def collect_videos(paths: list[str]) -> list[Path]:
    found: list[Path] = []
    seen: set[str] = set()
    for raw in paths:
        path = Path(raw).expanduser()
        try:
            path = path.resolve()
        except OSError:
            continue
        if not path.exists():
            continue
        candidates: list[Path] = []
        if path.is_dir():
            candidates = sorted(p for p in path.rglob("*") if is_video(p))
        elif is_video(path):
            candidates = [path]
        for item in candidates:
            key = str(item)
            if key not in seen:
                seen.add(key)
                found.append(item)
    return found


def parse_ffmpeg_progress(line: str, state: dict) -> None:
    if "=" not in line:
        return
    key, _, value = line.partition("=")
    key, value = key.strip(), value.strip()
    if key in {"out_time_ms", "out_time_us"}:
        try:
            number = int(value)
            state["out_time"] = number / (1_000_000 if key.endswith("us") else 1000)
        except ValueError:
            pass
    elif key == "out_time":
        # HH:MM:SS.micro
        parts = value.split(":")
        try:
            if len(parts) == 3:
                state["out_time"] = int(parts[0]) * 3600 + int(parts[1]) * 60 + float(parts[2])
        except ValueError:
            pass
    elif key == "speed":
        state["speed"] = value
    elif key == "progress":
        state["done"] = value == "end"


class Converter:
    def __init__(self) -> None:
        self.lock = threading.Lock()
        self.listeners: list[queue.Queue] = []
        self.job: dict | None = None
        self._cancel = threading.Event()
        self._proc: subprocess.Popen | None = None

    def subscribe(self) -> queue.Queue:
        q: queue.Queue = queue.Queue()
        with self.lock:
            self.listeners.append(q)
            if self.job:
                q.put({"type": "job", **self.snapshot()})
        return q

    def unsubscribe(self, q: queue.Queue) -> None:
        with self.lock:
            if q in self.listeners:
                self.listeners.remove(q)

    def emit(self, payload: dict) -> None:
        with self.lock:
            dead = []
            for q in self.listeners:
                try:
                    q.put_nowait(payload)
                except Exception:
                    dead.append(q)
            for q in dead:
                self.listeners.remove(q)

    def snapshot(self) -> dict:
        if not self.job:
            return {"active": False}
        return {"active": True, **self.job}

    def cancel(self) -> None:
        self._cancel.set()
        proc = self._proc
        if proc and proc.poll() is None:
            proc.terminate()

    def start(self, files: list[str], output_dir: str | None, overwrite: bool, delete_source: bool) -> dict:
        with self.lock:
            if self.job and self.job.get("status") == "running":
                return {"ok": False, "error": "Конвертация уже идёт"}
        paths = collect_videos(files)
        if not paths:
            return {"ok": False, "error": "Не выбрано ни одного видео"}
        items = []
        for path in paths:
            dest_dir = Path(output_dir).expanduser().resolve() if output_dir else path.parent
            dest = dest_dir / (path.stem + ".mkv")
            items.append(
                {
                    "src": str(path),
                    "dst": str(dest),
                    "name": path.name,
                    "size": path.stat().st_size if path.exists() else 0,
                    "status": "queued",
                    "percent": 0,
                    "error": None,
                }
            )
        self._cancel.clear()
        self.job = {
            "status": "running",
            "items": items,
            "index": 0,
            "percent": 0,
            "speed": None,
            "message": "Подготовка…",
            "overwrite": overwrite,
            "delete_source": delete_source,
            "started_at": time.time(),
        }
        threading.Thread(target=self._run, daemon=True).start()
        self.emit({"type": "job", **self.snapshot()})
        return {"ok": True, "count": len(items)}

    def _run(self) -> None:
        assert self.job is not None
        items = self.job["items"]
        overwrite = self.job["overwrite"]
        delete_source = self.job["delete_source"]
        failed = 0
        for i, item in enumerate(items):
            if self._cancel.is_set():
                item["status"] = "cancelled"
                self.job["status"] = "cancelled"
                self.job["message"] = "Остановлено"
                self.emit({"type": "job", **self.snapshot()})
                return
            self.job["index"] = i
            src = Path(item["src"])
            dst = Path(item["dst"])
            item["status"] = "running"
            self.job["message"] = src.name
            self.job["percent"] = 0
            self.job["speed"] = None
            self.emit({"type": "job", **self.snapshot()})

            media = probe_media(src)
            if not media.get("ok"):
                item["status"] = "error"
                item["error"] = media.get("error") or "файл нельзя прочитать"
                failed += 1
                self.emit({"type": "job", **self.snapshot()})
                continue

            try:
                dst.parent.mkdir(parents=True, exist_ok=True)
            except OSError as exc:
                item["status"] = "error"
                item["error"] = f"нет папки назначения: {exc}"
                failed += 1
                continue

            if dst.exists() and not overwrite:
                item["status"] = "skipped"
                item["percent"] = 100
                item["error"] = "MKV уже есть"
                self.emit({"type": "job", **self.snapshot()})
                continue

            try:
                free = shutil.disk_usage(str(dst.parent)).free
                if free < src.stat().st_size * 1.05:
                    item["status"] = "error"
                    item["error"] = "недостаточно места на диске"
                    failed += 1
                    self.emit({"type": "job", **self.snapshot()})
                    continue
            except OSError:
                pass

            tmp = dst.with_name(dst.stem + ".converting.mkv")
            duration = media.get("duration") or 0
            cmd = [
                "ffmpeg",
                "-hide_banner",
                "-nostdin",
                "-y",
                "-i",
                str(src),
                "-map",
                "0:v?",
                "-map",
                "0:a?",
                "-map",
                "0:s?",
                "-c",
                "copy",
                "-avoid_negative_ts",
                "make_zero",
                "-f",
                "matroska",
                "-progress",
                "pipe:1",
                "-nostats",
                str(tmp),
            ]
            try:
                proc = subprocess.Popen(
                    cmd,
                    stdout=subprocess.PIPE,
                    stderr=subprocess.PIPE,
                    text=True,
                    bufsize=1,
                )
            except FileNotFoundError:
                item["status"] = "error"
                item["error"] = "ffmpeg не найден. Установите: sudo apt install ffmpeg"
                self.job["status"] = "error"
                self.job["message"] = item["error"]
                self.emit({"type": "job", **self.snapshot()})
                return

            self._proc = proc
            state: dict = {"out_time": 0.0, "speed": None, "done": False}
            last_emit = 0.0
            assert proc.stdout is not None
            for line in proc.stdout:
                parse_ffmpeg_progress(line.strip(), state)
                now = time.time()
                if duration and state.get("out_time"):
                    item["percent"] = max(0, min(99, int(state["out_time"] * 100 / duration)))
                    self.job["percent"] = item["percent"]
                if state.get("speed"):
                    self.job["speed"] = state["speed"]
                if now - last_emit > 0.25:
                    last_emit = now
                    self.emit({"type": "job", **self.snapshot()})
            stderr = proc.stderr.read() if proc.stderr else ""
            code = proc.wait()
            self._proc = None

            if self._cancel.is_set():
                tmp.unlink(missing_ok=True)
                item["status"] = "cancelled"
                self.job["status"] = "cancelled"
                self.job["message"] = "Остановлено"
                self.emit({"type": "job", **self.snapshot()})
                return

            if code != 0 or not tmp.exists() or tmp.stat().st_size < 1024:
                tmp.unlink(missing_ok=True)
                item["status"] = "error"
                err_line = next((ln.strip() for ln in reversed((stderr or "").splitlines()) if ln.strip()), "ошибка ffmpeg")
                item["error"] = err_line[:240]
                failed += 1
                self.emit({"type": "job", **self.snapshot()})
                continue

            try:
                tmp.replace(dst)
            except OSError as exc:
                tmp.unlink(missing_ok=True)
                item["status"] = "error"
                item["error"] = str(exc)
                failed += 1
                continue

            item["percent"] = 100
            item["status"] = "done"
            if delete_source:
                try:
                    src.unlink()
                except OSError as exc:
                    item["error"] = f"MKV готов, исходник не удалился: {exc}"
            self.emit({"type": "job", **self.snapshot()})

        self.job["percent"] = 100
        if failed:
            self.job["status"] = "error"
            self.job["message"] = f"Готово с ошибками: {failed} из {len(items)}"
        else:
            self.job["status"] = "done"
            self.job["message"] = f"Готово: {len(items)} файл(ов)"
        self.emit({"type": "job", **self.snapshot()})


converter = Converter()


def browse(raw_path: str | None) -> dict:
    path = Path(raw_path or DEFAULT_DIR).expanduser()
    try:
        path = path.resolve()
    except OSError:
        path = Path.home()
    if not path.exists() or not path.is_dir():
        path = Path.home() if Path.home().is_dir() else Path("/")

    parent = str(path.parent) if path.parent != path else None
    entries = []
    try:
        children = list(path.iterdir())
    except OSError as exc:
        return {"ok": False, "error": str(exc), "path": str(path)}

    def sort_key(p: Path):
        return (not p.is_dir(), p.name.lower())

    for child in sorted(children, key=sort_key):
        if child.name.startswith("."):
            continue
        try:
            st = child.stat()
        except OSError:
            continue
        item = {
            "name": child.name,
            "path": str(child),
            "is_dir": child.is_dir(),
            "size": st.st_size if child.is_file() else None,
            "size_label": format_size(st.st_size) if child.is_file() else None,
            "is_video": is_video(child),
            "media": None,
        }
        if item["is_video"]:
            item["media"] = probe_media(child)
        entries.append(item)

    crumbs = []
    acc = Path(path.anchor or "/")
    parts = path.parts[1:] if path.anchor else path.parts
    crumbs.append({"name": "/", "path": str(Path(path.anchor or "/"))})
    for part in path.parts[1:]:
        acc = acc / part
        crumbs.append({"name": part, "path": str(acc)})

    return {
        "ok": True,
        "path": str(path),
        "parent": parent,
        "crumbs": crumbs,
        "entries": entries,
        "videos": sum(1 for e in entries if e["is_video"]),
        "broken": sum(1 for e in entries if e.get("media") and not e["media"].get("ok")),
        "writable": os.access(path, os.W_OK),
    }


def expand_selection(paths: list[str]) -> dict:
    files = collect_videos(paths)
    items = []
    for path in files:
        try:
            size = path.stat().st_size
        except OSError:
            size = 0
        media = probe_media(path)
        items.append(
            {
                "path": str(path),
                "name": path.name,
                "dir": str(path.parent),
                "size": size,
                "size_label": format_size(size),
                "media": media,
            }
        )
    return {"ok": True, "items": items, "total_size": sum(i["size"] for i in items)}


class Handler(BaseHTTPRequestHandler):
    server_version = "MkvConverter/1.0"

    def log_message(self, fmt: str, *args) -> None:
        sys.stderr.write("[%s] %s\n" % (self.log_date_time_string(), fmt % args))

    def _send(self, code: int, body: bytes, content_type: str) -> None:
        self.send_response(code)
        self.send_header("Content-Type", content_type)
        self.send_header("Content-Length", str(len(body)))
        self.send_header("Cache-Control", "no-store")
        self.end_headers()
        self.wfile.write(body)

    def _send_json(self, data, code: int = 200) -> None:
        self._send(code, json_bytes(data), "application/json; charset=utf-8")

    def _read_json(self) -> dict:
        length = int(self.headers.get("Content-Length") or 0)
        raw = self.rfile.read(length) if length else b"{}"
        if not raw:
            return {}
        return json.loads(raw.decode("utf-8"))

    def do_GET(self) -> None:
        parsed = urlparse(self.path)
        path = unquote(parsed.path)
        query = parse_qs(parsed.query)

        if path in {"/", "/index.html"}:
            html = (ROOT / "index.html").read_bytes()
            self._send(200, html, "text/html; charset=utf-8")
            return
        if path == "/api/browse":
            target = (query.get("path") or [None])[0]
            self._send_json(browse(target))
            return
        if path == "/api/status":
            self._send_json(converter.snapshot())
            return
        if path == "/api/defaults":
            videos = Path.home() / "Videos"
            downloads = Path.home() / "Загрузки"
            self._send_json(
                {
                    "default_dir": str(DEFAULT_DIR if DEFAULT_DIR.is_dir() else downloads),
                    "shortcuts": [
                        {"name": "YouTube", "path": str(DEFAULT_DIR)},
                        {"name": "Загрузки", "path": str(downloads)},
                        {"name": "Видео", "path": str(videos)},
                        {"name": "Домашняя", "path": str(Path.home())},
                    ],
                }
            )
            return
        if path == "/api/events":
            self._sse()
            return
        self._send(404, b"Not found", "text/plain; charset=utf-8")

    def do_POST(self) -> None:
        parsed = urlparse(self.path)
        path = unquote(parsed.path)
        try:
            payload = self._read_json()
        except json.JSONDecodeError:
            self._send_json({"ok": False, "error": "Некорректный JSON"}, 400)
            return
        if path == "/api/expand":
            self._send_json(expand_selection(payload.get("paths") or []))
            return
        if path == "/api/convert":
            output_dir = payload.get("output_dir") or None
            if output_dir == "":
                output_dir = None
            result = converter.start(
                files=payload.get("files") or [],
                output_dir=output_dir,
                overwrite=bool(payload.get("overwrite")),
                delete_source=bool(payload.get("delete_source")),
            )
            self._send_json(result, 200 if result.get("ok") else 400)
            return
        if path == "/api/cancel":
            converter.cancel()
            self._send_json({"ok": True})
            return
        self._send(404, b"Not found", "text/plain; charset=utf-8")

    def _sse(self) -> None:
        q = converter.subscribe()
        self.send_response(200)
        self.send_header("Content-Type", "text/event-stream; charset=utf-8")
        self.send_header("Cache-Control", "no-cache")
        self.send_header("Connection", "keep-alive")
        self.end_headers()
        try:
            self.wfile.write(b": connected\n\n")
            self.wfile.flush()
            while True:
                try:
                    event = q.get(timeout=15)
                    blob = json_bytes(event)
                    self.wfile.write(b"data: " + blob + b"\n\n")
                    self.wfile.flush()
                except queue.Empty:
                    self.wfile.write(b": ping\n\n")
                    self.wfile.flush()
        except (BrokenPipeError, ConnectionResetError, OSError):
            pass
        finally:
            converter.unsubscribe(q)


def main() -> None:
    parser = argparse.ArgumentParser(description="MP4 → MKV без потери качества")
    parser.add_argument("--host", default=HOST)
    parser.add_argument("--port", type=int, default=PORT)
    parser.add_argument("--no-browser", action="store_true")
    args = parser.parse_args()

    if not shutil.which("ffmpeg") or not shutil.which("ffprobe"):
        print("Нужен ffmpeg. Установите: sudo apt install ffmpeg", file=sys.stderr)
        sys.exit(1)
    if not (ROOT / "index.html").exists():
        print("Не найден index.html рядом со скриптом", file=sys.stderr)
        sys.exit(1)

    ThreadingHTTPServer.allow_reuse_address = True
    httpd = ThreadingHTTPServer((args.host, args.port), Handler)
    url = f"http://{args.host}:{args.port}/"
    print(f"Конвертер открыт: {url}")
    print("Остановка: Ctrl+C")
    if not args.no_browser:
        threading.Timer(0.4, lambda: webbrowser.open(url)).start()
    try:
        httpd.serve_forever()
    except KeyboardInterrupt:
        print("\nОстановлено")
        httpd.shutdown()


if __name__ == "__main__":
    main()
