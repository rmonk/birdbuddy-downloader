# AGENTS.md - Developer & AI Agent Context Guide

This document provides architectural context, development guidelines, CLI flags, database schemas, and operational instructions for AI agents working on or maintaining this codebase.

---

## 1. Project Overview

`birdbuddy-downloader` is a Python utility that connects to the unofficial Bird Buddy GraphQL API (via `pybirdbuddy`) to automatically download photos and videos captured across all connected camera feeders.

### Core Capabilities
- **De-Duplication**: Tracks downloaded media items in a local SQLite database (`birdbuddy_downloader.db`).
- **Timestamp & EXIF Preservation**: Sets file `mtime`/`atime` and embeds EXIF date metadata into downloaded JPEG images.
- **Custom Templating**: Dynamic subdirectory (`--dir-template`) and filename (`--filename-template`) generation with support for detection/postcard event grouping (`{detection_id}`, `{postcard_id}`, `{sighting_id}`).
- **Incremental Syncing**: Uses the latest DB capture timestamp minus a 2-hour buffer (`--buffer-hours 2.0`) as the cutoff point to halt feed pagination early for maximum efficiency.
- **Automated Database Cleanup**: Automatically purges database records older than 14 days (`--db-retention-days 14`) after each run.
- **Web Status Dashboard**: Embedded web interface (`--web-port 8080`) displaying sync interval, last sync timestamp/status, next sync countdown, and per-feeder download breakdown across the past hour, day (24h), and week (7d).
- **Persistent Auth & Session Resilience**: Retains OAuth tokens across intervals, refreshing access tokens without triggering repeated full password logins.
- **Robust Sockets & Timeouts**: Granular connection (`15s`) and socket read (`30s`) timeouts prevent network stalls from hanging the daemon.
- **Account & Hardware Reporting**: Detailed camera information (`--info`), including battery level, food level, Wi-Fi signal, date ranges, and species counts.
- **Dry-Run Mode**: Non-destructive preview (`--dry-run`) of files to be downloaded versus skipped.
- **Graceful Termination**: Handles `SIGINT` (Ctrl-C) and `SIGTERM` instantly, cancelling network streams and cleaning up temporary `.tmp` download files.

---

## 2. Directory Structure & Key Files

```text
birdbuddy-downloader/
├── .github/
│   └── workflows/
│       └── docker-publish.yml # GitHub Actions CI/CD to build & push container
├── AGENTS.md                  # Context & guidelines for AI agents
├── LICENSE                    # MIT License file
├── README.md                  # End-user documentation
├── Containerfile              # Container build definition (exposes 8080)
├── docker-compose.yml         # Docker Compose configuration
├── podman-compose.yml         # Podman Compose configuration
├── downloader.py              # Main Python application entry point
├── requirements.txt           # Python dependency requirements
├── .env                       # Local credentials (USERNAME, PASSWORD) - DO NOT COMMIT
├── .env.example               # Template environment configuration
├── .gitignore                 # Standard git ignore patterns
├── birdbuddy_downloader.db    # SQLite database (auto-created at runtime)
├── venv/                      # Python virtual environment (auto-created)
└── downloads/                 # Default directory for saved media
```

---

## 3. Python Environment & Setup

- **Python Version**: Python 3.10+ (Tested on Python 3.14)
- **Virtual Environment**: `./venv/`
- **Execution Rule**: Always execute Python commands using the virtual environment interpreter:
  ```bash
  ./venv/bin/python3 downloader.py [FLAGS]
  ```

### Dependencies
- `pybirdbuddy`: Unofficial GraphQL client for Bird Buddy.
- `python-dotenv`: Environment variable loading from `.env`.
- `piexif` & `pillow`: EXIF manipulation and image handling.
- `opencv-python-headless` & `numpy`: Image sharpness calculation (variance of Laplacian).
- `requests` & `aiohttp`: HTTP requests, async downloading, and embedded web status server.

---

## 4. Architecture & Implementation Details (`downloader.py`)

### Database Schema (`birdbuddy_downloader.db`)
```sql
CREATE TABLE IF NOT EXISTS downloaded_media (
    media_id TEXT PRIMARY KEY,
    feeder_id TEXT,
    feeder_name TEXT,
    species_name TEXT,
    media_type TEXT,
    created_at TEXT,
    downloaded_at TEXT,
    file_path TEXT,
    sighting_id TEXT,
    bird_score REAL,
    bird_detected INTEGER DEFAULT 0,
    sharpness_score REAL,
    is_deleted INTEGER DEFAULT 0,
    trash_path TEXT,
    deleted_at TEXT
);
CREATE INDEX IF NOT EXISTS idx_downloaded_at ON downloaded_media(downloaded_at);
CREATE INDEX IF NOT EXISTS idx_feeder_name ON downloaded_media(feeder_name);
CREATE INDEX IF NOT EXISTS idx_created_at ON downloaded_media(created_at);
CREATE INDEX IF NOT EXISTS idx_sighting_id ON downloaded_media(sighting_id);
CREATE INDEX IF NOT EXISTS idx_bird_score ON downloaded_media(bird_score);
CREATE INDEX IF NOT EXISTS idx_is_deleted ON downloaded_media(is_deleted);
```

### Key Functions & Classes
- `BirdBuddyDownloader`: Main orchestrator class handling authentication, API queries, feed pagination, state tracking, and media downloading.
- `detect_birds()`: Analyzes downloaded images via OpenCV DNN (`cv2.dnn`) with a lightweight ONNX YOLO object detector to calculate bird presence likelihood and confidence score.
- `get_recent_sightings()`: Queries media from the past 7 days grouped by `sighting_id`, tagging the highest bird likelihood photo (`is_best_view = True`) per sighting and tracking active vs removed items.
- `soft_delete_media()` / `restore_media()`: Safely moves deleted files to `.trash/` or restores them to original path.
- `cleanup_old_db_records()`: Purges records older than `--db-retention-days` (default 14 days); permanently removes aged `.trash/` files from disk while preserving active files.
- `get_feeder_download_stats()`: Queries SQLite for per-feeder download breakdown (past hour, 24 hours, 7 days, all-time totals, and recent activity).
- `create_web_app()`: Sets up the asynchronous `aiohttp.web` dashboard serving HTML and `/api/status`, `/api/sightings`, `/api/media/{id}/view`, `/api/media/{id}/thumb`, `/api/media/delete`, `/api/media/restore`, `/api/sync` endpoints.
- `get_latest_download_timestamp()`: Finds the max `created_at` timestamp in SQLite to calculate the incremental sync cutoff.
- `build_template_vars()`: Generates a dictionary of metadata placeholders (`feeder_name`, `species_name`, `detection_id`, `postcard_id`, `sighting_id`, `year`, `month`, `day`, `media_id_short`, etc.).
- `render_dest_path()`: Safely formats directory and filename strings using `SafeDict` to prevent `KeyError` crashes, sanitizes path components, and ensures correct extensions (`.jpg`/`.mp4`).
- `download_file()`: Downloads files atomically to `.tmp` files first with explicit socket timeouts, replacing destination upon completion.
- `apply_timestamps_and_exif()`: Writes EXIF `DateTimeOriginal`, `DateTimeDigitized`, `DateTime` and updates file system modification time (`os.utime`).

---

## 5. CLI Arguments Quick Reference

| Argument | Default | Description |
| :--- | :--- | :--- |
| `--env-file` | `.env` | Path to `.env` file containing credentials |
| `--username` | Env `USERNAME` | Bird Buddy account email |
| `--password` | Env `PASSWORD` | Bird Buddy account password |
| `--download-dir` | `./downloads` | Root directory for downloaded media |
| `--db-path` | `./birdbuddy_downloader.db` | Path to SQLite de-duplication database |
| `--dir-template` | `{feeder_name}/{species_name}` | Output subdirectory template relative to `--download-dir` |
| `--filename-template` | `{year}{month}{day}_{hour}{minute}{second}_{media_id_short}.{ext}` | Output filename template |
| `--buffer-hours` | `2.0` | Hours before latest downloaded detection to start fetching feed items |
| `--db-retention-days` | `14` | Days to retain records in database before cleanup (0 to disable) |
| `--min-bird-confidence` | `0.25` | Minimum detection confidence threshold to count as a bird match |
| `--model-path` | `models/yolov8n.onnx` | Path to ONNX object detection model for bird identification |
| `--no-detect` | `False` | Skip bird detection likelihood scoring on downloaded images |
| `--full-sync` | `False` | Bypass incremental cutoff and perform a full feed sync |
| `--interval` | `0` | Polling interval in seconds (0 = single run) |
| `--max-pages` | `0` | Max feed pages to fetch (0 = unlimited) |
| `--no-images` | `False` | Skip downloading `.jpg` files |
| `--no-videos` | `False` | Skip downloading `.mp4` files |
| `--feeder-filter` | `None` | Case-insensitive substring filter for feeder names |
| `--dry-run` | `False` | Preview downloads without writing files or mutating DB |
| `--info` | `False` | Print feeder status, battery level, species count report |
| `--json` | `False` | Output `--info` as raw JSON |
| `--web-port` | `8080` | Port for embedded web status dashboard (or `WEB_PORT` env var) |
| `--web-host` | `0.0.0.0` | Host to bind embedded web dashboard (or `WEB_HOST` env var) |
| `--web-api-key` | `None` | Optional API key secret to protect mutating endpoints |
| `--no-web` | `False` | Disable embedded web status dashboard |
| `-v`, `--verbose` | `False` | Enable debug logging |

---

## 6. Testing & Validation Checklist for Agents

When making code edits or refactoring, verify the following:

1. **Syntax, Imports & Black Formatting**:
   ```bash
   ./venv/bin/black --check .
   ./venv/bin/python3 -c "import downloader; print('OK')"
   ```
2. **Dry Run & Incremental Cutoff**:
   ```bash
   ./venv/bin/python3 downloader.py --dry-run --max-pages 5
   ```
3. **Detection Grouping Check**:
   ```bash
   ./venv/bin/python3 downloader.py --dry-run --full-sync --max-pages 1 --dir-template="{feeder_name}/{species_name}/{date}_{detection_id_short}" --filename-template="{detection_id_short}_{id_short}.{ext}"
   ```
4. **Info & Account Metrics**:
   ```bash
   ./venv/bin/python3 downloader.py --info
   ./venv/bin/python3 downloader.py --info --json
   ```
5. **Git Branching & Pre-Commit Hook**:
   Always create a dedicated feature branch for new work. The pre-commit hook runs `leaktk` and `black --check .` before allowing any commit to proceed.
