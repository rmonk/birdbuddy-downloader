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
- **Account & Hardware Reporting**: Detailed camera information (`--info`), including battery level, food level, Wi-Fi signal, date ranges, and species counts.
- **Dry-Run Mode**: Non-destructive preview (`--dry-run`) of files to be downloaded versus skipped.
- **Graceful Termination**: Handles `SIGINT` (Ctrl-C) and `SIGTERM` instantly, cancelling network streams and cleaning up temporary `.tmp` download files.

---

## 2. Directory Structure & Key Files

```text
birdbuddy-downloader/
├── AGENTS.md                  # Context & guidelines for AI agents
├── LICENSE                    # MIT License file
├── README.md                  # End-user documentation
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
- `requests` & `aiohttp`: HTTP requests and async downloading.

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
    file_path TEXT
);
```

### Key Functions & Classes
- `BirdBuddyDownloader`: Main orchestrator class handling authentication, API queries, feed pagination, and media downloading.
- `get_latest_download_timestamp()`: Finds the max `created_at` timestamp in SQLite to calculate the incremental sync cutoff.
- `cleanup_old_db_records()`: Purges records older than `--db-retention-days` (default 14 days) from SQLite after each run.
- `build_template_vars()`: Generates a dictionary of metadata placeholders (`feeder_name`, `species_name`, `detection_id`, `postcard_id`, `sighting_id`, `year`, `month`, `day`, `media_id_short`, etc.).
- `render_dest_path()`: Safely formats directory and filename strings using `SafeDict` to prevent `KeyError` crashes, sanitizes path components, and ensures correct extensions (`.jpg`/`.mp4`).
- `download_file()`: Downloads files atomically to `.tmp` files first, replacing the destination file upon completion. Checks `stop_checker` mid-stream for cancellation.
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
| `--full-sync` | `False` | Bypass incremental cutoff and perform a full feed sync |
| `--interval` | `0` | Polling interval in seconds (0 = single run) |
| `--max-pages` | `0` | Max feed pages to fetch (0 = unlimited) |
| `--no-images` | `False` | Skip downloading `.jpg` files |
| `--no-videos` | `False` | Skip downloading `.mp4` files |
| `--feeder-filter` | `None` | Case-insensitive substring filter for feeder names |
| `--dry-run` | `False` | Preview downloads without writing files or mutating DB |
| `--info` | `False` | Print feeder status, battery level, species count report |
| `--json` | `False` | Output `--info` as raw JSON |
| `-v`, `--verbose` | `False` | Enable debug logging |

---

## 6. Testing & Validation Checklist for Agents

When making code edits or refactoring, verify the following:

1. **Syntax & Imports**:
   ```bash
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
5. **Signal Handling (Ctrl-C)**:
   Verify that sending `SIGINT` (signal 2) immediately halts loop execution and closes the database cleanly without hanging or leaving `.tmp` files behind.
