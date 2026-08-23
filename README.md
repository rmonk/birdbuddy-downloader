# Bird Buddy Automatic Media Downloader

An automated python utility to download images and videos from all Bird Buddy cameras attached to your account.

## Features

- **Multi-Camera Support**: Downloads media from all Bird Buddy cameras associated with your account automatically.
- **Detection & Visit Grouping Placeholders**:
  - Group all photos and videos captured during a single visitor detection event into dedicated folders or filenames using `{detection_id}`, `{detection_id_short}`, `{postcard_id}`, or `{sighting_id}`.
- **Efficient Incremental Sync (`--buffer-hours`)**:
  - Uses the timestamp of the latest downloaded media item in the database as the starting point.
  - Goes back 2 hours prior to the latest detection timestamp (`--buffer-hours 2`) to ensure no missed events, then halts feed pagination automatically.
  - Can be overridden using `--full-sync` to force a complete re-scan.
- **Automatic Database Cleanup (`--db-retention-days`)**:
  - Automatically purges database records older than 14 days (`--db-retention-days 14`) after each run to keep tracking compact.
- **Customizable Output Directory & Filename Templating**:
  - Customize subdirectories (`--dir-template`) and filenames (`--filename-template`).
  - Supports placeholders for feeder name, species, owner name, date, time, year, month, day, media ID, detection ID, media type, extension, and more.
  - Maintains default structure (`{feeder_name}/{species_name}/{year}{month}{day}_{hour}{minute}{second}_{media_id_short}.{ext}`) if unconfigured.
- **Feeder & Account Reporting (`--info`)**:
  - Displays real-time feeder statistics: battery level, charging status, food level, Wi-Fi signal strength, temperature, and camera state.
  - Breakdown of available images and videos per bird species.
  - Earliest and latest event capture timestamps recorded for each camera feeder.
  - Output as formatted text or machine-readable JSON (`--json`).
- **Dry-Run Mode (`--dry-run`)**:
  - Preview media items that would be downloaded vs skipped without performing disk writes or database mutations.
- **De-duplication**: Tracks downloaded media in a local SQLite database (`birdbuddy_downloader.db`). Never downloads the same image or video twice.
- **Timestamp Preservation**:
  - Sets the file modification time (`mtime` / `atime`) on all images and videos to match the capture timestamp from Bird Buddy metadata.
  - Embeds EXIF metadata (`DateTime`, `DateTimeOriginal`, `DateTimeDigitized`) into JPEG image files.
- **Flexible Execution**:
  - **Single-run mode**: Ideal for running via `cron` or `systemd` timers.
  - **Continuous daemon mode**: Polls periodically on a schedule (`--interval 3600`).
- **Web Status Dashboard**:
  - Embedded real-time web dashboard running on port `8080` (configurable via `--web-port` / `WEB_PORT`).
  - Displays sync interval, last sync execution time/status, next scheduled sync countdown, and connected feeder hardware status.
  - Shows breakdown table of photos and videos downloaded over the **past hour**, **past day (24 hours)**, and **past week (7 days)** per camera feeder and overall totals.
  - Provides a "Sync Now" button to trigger immediate on-demand syncing.
- **Session Resilience & Anti-Throttling**:
  - Automatically preserves and refreshes OAuth tokens across runs, preventing excessive password authentication that causes account/IP throttling.
- **Robust Sockets & Timeouts**:
  - Granular connection (`15s`) and socket read (`30s`) timeouts prevent network stalls from hanging the daemon.
- **Media Filtering**: Option to skip videos (`--no-videos`), skip images (`--no-images`), or filter by specific feeder name (`--feeder-filter`).

---

## Setup & Installation

### 1. Clone or Open the Repository
```bash
cd /path/to/birdbuddy-downloader
```

### 2. Set Up Virtual Environment
Create and activate a Python virtual environment:
```bash
python3 -m venv venv
source venv/bin/activate
```

### 3. Install Dependencies
Install all required packages from `requirements.txt`:
```bash
pip install -r requirements.txt
```

### 4. Configure Credentials & Options
Edit the `.env` file with your Bird Buddy account email and password (and optional configuration):
```env
USERNAME="your_email@example.com"
PASSWORD="your_password"

# Optional settings
# BUFFER_HOURS=2.0
# DB_RETENTION_DAYS=14
# DIR_TEMPLATE="{feeder_name}/{species_name}/{date}_{detection_id_short}"
# FILENAME_TEMPLATE="{detection_id_short}_{id_short}.{ext}"
```

---

## Container Deployment (Podman / Docker)

You can run `birdbuddy-downloader` in a container using **Podman** or **Docker**.

### 1. Configure `.env`
Ensure your `.env` file contains your credentials and desired options:
```env
USERNAME="your_email@example.com"
PASSWORD="your_password"
INTERVAL=3600  # Set to >0 for continuous polling mode in container
```

### 2. Run with `podman-compose` or `docker compose`
Build and launch the container in background mode:
```bash
# Using podman-compose
podman-compose up -d --build

# Using docker compose
docker compose up -d --build
```

### 3. Run single commands or dry-runs via container
```bash
# Build image manually
podman build -t birdbuddy-downloader -f Containerfile .

# Run dry-run via container
podman run --rm --env-file .env \
  -v ./downloads:/app/downloads:z \
  -v ./data:/app/data:z \
  birdbuddy-downloader --dry-run
```

### 4. Automated Container Builds (GitHub Actions)

A GitHub Actions workflow is provided at [`.github/workflows/docker-publish.yml`](.github/workflows/docker-publish.yml) to automatically build multi-arch container images (`linux/amd64` and `linux/arm64`) and push them to `docker.io/moosemouse/birdbuddy-downloader:latest` on every push to `main` or new version tag.

**Required GitHub Repository Secrets**:
- `DOCKERHUB_USERNAME`: Your Docker Hub username (e.g. `moosemouse`).
- `DOCKERHUB_TOKEN`: Docker Hub Personal Access Token (PAT) with Read/Write permissions.

---

You can define custom directory structures and filenames using `--dir-template` and `--filename-template` flags (or `DIR_TEMPLATE` and `FILENAME_TEMPLATE` in `.env`).

### Available Template Variables

| Variable | Description | Example |
| :--- | :--- | :--- |
| `{feeder_name}` or `{feeder}` | Name of the camera feeder | `Back Porch` |
| `{feeder_id}` | Unique ID of the feeder | `516e45ea-449c-4da2-88b0...` |
| `{species_name}` or `{species}` | Detected bird/animal species | `Northern Cardinal` |
| `{owner_name}` or `{owner}` | Owner of the feeder camera | `moosemaus` |
| `{detection_id}` or `{postcard_id}` | Unique ID grouping all media from a single visit/postcard detection | `86394c2e-97a0-49e1-bbac...` |
| `{detection_id_short}` or `{postcard_id_short}` | First 8 characters of detection/postcard ID | `86394c2e` |
| `{sighting_id}` | Sighting report ID | `e9d4437e-6c52-48ae-b896...` |
| `{sighting_id_short}` | First 8 characters of sighting report ID | `e9d4437e` |
| `{year}` or `{YYYY}` | 4-digit capture year | `2026` |
| `{year_short}` or `{YY}` | 2-digit capture year | `26` |
| `{month}` or `{MM}` | 2-digit capture month | `07` |
| `{day}` or `{DD}` | 2-digit capture day | `21` |
| `{hour}` or `{HH}` | 2-digit capture hour (24-hour) | `18` |
| `{minute}` or `{min}` or `{mm}` | 2-digit capture minute | `29` |
| `{second}` or `{sec}` or `{SS}` | 2-digit capture second | `44` |
| `{date}` | Compact date (`YYYYMMDD`) | `20260721` |
| `{iso_date}` | ISO formatted date (`YYYY-MM-DD`) | `2026-07-21` |
| `{time}` | Compact time (`HHMMSS`) | `182944` |
| `{iso_time}` | ISO formatted time (`HH-MM-SS`) | `18-29-44` |
| `{media_id}` or `{id}` | Full unique media UUID | `16223fa7-7cfc-4543...` |
| `{media_id_short}` or `{id_short}` | First 8 characters of media UUID | `16223fa7` |
| `{media_type}` or `{type}` | Media type (`image` or `video`) | `image` |
| `{ext}` or `{extension}` | File extension (`jpg` or `mp4`) | `jpg` |

---

## Grouping Media by Detection Event

Because a single bird visit/detection generates multiple photos and 1 video, you can group them into a subfolder per detection using `{detection_id_short}` or `{postcard_id_short}`:

```bash
./venv/bin/python3 downloader.py \
  --dir-template="{feeder_name}/{species_name}/{date}_{detection_id_short}" \
  --filename-template="{detection_id_short}_{id_short}.{ext}"
```

**Resulting Structure**:
```text
downloads/Back Porch/Tufted Titmouse/20260721_86394c2e/
├── 86394c2e_16223fa7.jpg
├── 86394c2e_36fd43ed.jpg
└── 86394c2e_3c68fcd6.mp4
```

---

## Usage & Commands

### Display Feeder Information & Species Breakdown
To view camera battery levels, food status, signal strength, earliest/latest event timestamps, and species counts:
```bash
./venv/bin/python3 downloader.py --info
```

### Dry-Run Mode
To see what files would be downloaded without saving files or modifying the database:
```bash
./venv/bin/python3 downloader.py --dry-run
```

### Force Full Sync
To bypass the 2-hour buffer incremental cutoff and scan all feed pages:
```bash
./venv/bin/python3 downloader.py --full-sync
```

### Run Media Sync Once (Default)
To scan for and download all new images and videos from all cameras:
```bash
./venv/bin/python3 downloader.py
```

### Command Line Options

```text
usage: downloader.py [-h] [--env-file ENV_FILE] [--username USERNAME] [--password PASSWORD]
                     [--download-dir DOWNLOAD_DIR] [--db-path DB_PATH]
                     [--dir-template DIR_TEMPLATE] [--filename-template FILENAME_TEMPLATE]
                     [--buffer-hours BUFFER_HOURS] [--db-retention-days DB_RETENTION_DAYS]
                     [--full-sync] [--interval INTERVAL] [--max-pages MAX_PAGES]
                     [--no-images] [--no-videos] [--feeder-filter FEEDER_FILTER] [--dry-run]
                     [--info] [--json] [-v]

Automatic Bird Buddy media downloader with de-duplication, metadata timestamping, and feeder info.

options:
  -h, --help            show this help message and exit
  --env-file ENV_FILE   Path to .env file containing credentials (default: .env)
  --username USERNAME   Bird Buddy account email (overrides .env)
  --password PASSWORD   Bird Buddy account password (overrides .env)
  --download-dir DOWNLOAD_DIR
                        Directory to save downloaded media (default: ./downloads)
  --db-path DB_PATH     SQLite DB path for de-duplication (default: ./birdbuddy_downloader.db)
  --dir-template DIR_TEMPLATE
                        Template for output subdirectories relative to download-dir (default: '{feeder_name}/{species_name}').
  --filename-template FILENAME_TEMPLATE
                        Template for output filenames (default: '{year}{month}{day}_{hour}{minute}{second}_{media_id_short}.{ext}').
  --buffer-hours BUFFER_HOURS
                        Hours before latest downloaded detection to start fetching feed items (default: 2.0)
  --db-retention-days DB_RETENTION_DAYS
                        Days to retain records in database before cleanup (default: 14; set to 0 to disable)
  --full-sync           Bypass latest detection cutoff and perform a full feed sync
  --interval INTERVAL   Interval in seconds for continuous polling mode (0 for single run)
  --max-pages MAX_PAGES Maximum feed pages to fetch (0 for unlimited)
  --no-images           Skip downloading image files
  --no-videos           Skip downloading video files
  --feeder-filter FEEDER_FILTER
                        Filter downloads by feeder name (case-insensitive substring)
  --dry-run             Perform a dry run without downloading files or modifying database
  --info                Display feeder information, battery status, species media counts, and event date ranges
  --json                Output --info as raw JSON
  --web-port WEB_PORT   Port for embedded web status dashboard (default: 8080 or WEB_PORT env var)
  --web-host WEB_HOST   Host address to bind embedded web dashboard (default: 0.0.0.0 or WEB_HOST env var)
  --no-web              Disable embedded web status dashboard
  -v, --verbose         Enable debug logging
```

---

## Regular Automated Execution

### Option A: Running as a Background Daemon (`--interval`)
You can run the downloader continuously in the background to poll every hour (3600 seconds):
```bash
./venv/bin/python3 downloader.py --interval 3600
```

### Option B: Cron Job (Every Hour)
Open your crontab:
```bash
crontab -e
```
Add the following entry to check for new media every hour:
```cron
0 * * * * cd /home/rmonk/repos/birdbuddy-downloader && ./venv/bin/python3 downloader.py >> downloader.log 2>&1
```

### Option C: Systemd Service & Timer (Linux)

1. Create a service file `/etc/systemd/system/birdbuddy-downloader.service`:
```ini
[Unit]
Description=Bird Buddy Media Downloader
After=network.target

[Service]
Type=oneshot
WorkingDirectory=/home/rmonk/repos/birdbuddy-downloader
ExecStart=/home/rmonk/repos/birdbuddy-downloader/venv/bin/python3 /home/rmonk/repos/birdbuddy-downloader/downloader.py

[Install]
WantedBy=multi-user.target
```

2. Create a timer file `/etc/systemd/system/birdbuddy-downloader.timer`:
```ini
[Unit]
Description=Run Bird Buddy Downloader hourly

[Timer]
OnCalendar=hourly
Persistent=true

[Install]
WantedBy=timers.target
```

3. Enable and start the timer:
```bash
sudo systemctl daemon-reload
sudo systemctl enable --now birdbuddy-downloader.timer
```

---

## License

This project is licensed under the [MIT License](LICENSE).
