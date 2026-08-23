#!/usr/bin/env python3
"""
Bird Buddy Automatic Media Downloader

Downloads images and videos from all Bird Buddy cameras associated with an account.
Features:
- De-duplication via SQLite database tracking
- Preserves capture timestamps in file modification dates and EXIF metadata
- Organizes media into camera/feeder and species folders
- Custom directory and filename templating (--dir-template, --filename-template)
- Detection / Postcard event grouping placeholders ({postcard_id}, {detection_id}, {sighting_id})
- Efficient incremental sync starting from latest detection minus buffer hours (--buffer-hours)
- Configurable database retention cleanup after each run (--db-retention-days)
- Supports single-run mode (cron-friendly) or continuous polling mode (--interval)
- Embedded web interface showing sync interval, last sync, next sync, and per-feeder download statistics (past hour, day, week)
- Provides detailed camera information, battery levels, species media counts, and event date ranges
- Supports --dry-run mode to list items that would be downloaded vs skipped
- Persistent authentication with automatic token refresh to prevent API rate-limiting
- Granular network socket timeouts to prevent indefinite hanging
- Instant and graceful shutdown on Ctrl-C (SIGINT) or SIGTERM
"""

import argparse
import asyncio
import json
import logging
import os
import re
import signal
import sqlite3
import sys
import time
from datetime import datetime, timezone, timedelta
import aiohttp
from aiohttp import web
import requests
from dotenv import load_dotenv

import piexif
from PIL import Image

try:
    from birdbuddy.client import BirdBuddy
    from birdbuddy.feed import FeedNodeType
    from birdbuddy.queries import me as me_queries
except ImportError:
    print(
        "Error: pybirdbuddy package not found. Please install requirements.txt first."
    )
    sys.exit(1)

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger("birdbuddy-downloader")

DB_SCHEMA = """
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
CREATE INDEX IF NOT EXISTS idx_downloaded_at ON downloaded_media(downloaded_at);
CREATE INDEX IF NOT EXISTS idx_feeder_name ON downloaded_media(feeder_name);
CREATE INDEX IF NOT EXISTS idx_created_at ON downloaded_media(created_at);
"""


def init_db(db_path: str) -> sqlite3.Connection:
    """Initialize SQLite database for tracking downloaded media."""
    db_dir = os.path.dirname(os.path.abspath(db_path))
    if db_dir:
        os.makedirs(db_dir, exist_ok=True)
    conn = sqlite3.connect(db_path, check_same_thread=False, timeout=30.0)
    conn.execute("PRAGMA journal_mode=WAL;")
    conn.execute("PRAGMA synchronous=NORMAL;")
    with conn:
        conn.executescript(DB_SCHEMA)
    return conn


def is_media_downloaded(conn: sqlite3.Connection, media_id: str) -> bool:
    """Check if a media item has already been downloaded."""
    cursor = conn.cursor()
    cursor.execute("SELECT 1 FROM downloaded_media WHERE media_id = ?", (media_id,))
    return cursor.fetchone() is not None


def record_media_downloaded(
    conn: sqlite3.Connection,
    media_id: str,
    feeder_id: str,
    feeder_name: str,
    species_name: str,
    media_type: str,
    created_at: str,
    file_path: str,
):
    """Record media item in database after successful download."""
    now_iso = datetime.now(timezone.utc).isoformat()
    with conn:
        conn.execute(
            """
            INSERT OR REPLACE INTO downloaded_media
            (media_id, feeder_id, feeder_name, species_name, media_type, created_at, downloaded_at, file_path)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                media_id,
                feeder_id,
                feeder_name,
                species_name,
                media_type,
                created_at,
                now_iso,
                file_path,
            ),
        )


def get_latest_download_timestamp(conn: sqlite3.Connection) -> datetime | None:
    """Find the latest created_at datetime among recorded media in the database."""
    cursor = conn.cursor()
    cursor.execute("SELECT created_at FROM downloaded_media")
    rows = cursor.fetchall()
    latest_dt = None
    for (created_at_str,) in rows:
        dt = parse_iso_datetime(created_at_str)
        if dt:
            if latest_dt is None or dt > latest_dt:
                latest_dt = dt
    return latest_dt


def cleanup_old_db_records(conn: sqlite3.Connection, retention_days: int = 14) -> int:
    """Delete records from downloaded_media database where created_at is older than retention_days."""
    if retention_days <= 0:
        return 0
    cutoff = datetime.now(timezone.utc) - timedelta(days=retention_days)
    cursor = conn.cursor()
    cursor.execute("SELECT media_id, created_at FROM downloaded_media")
    rows = cursor.fetchall()
    to_delete = []
    for media_id, created_at_str in rows:
        dt = parse_iso_datetime(created_at_str)
        if dt and dt < cutoff:
            to_delete.append(media_id)

    if to_delete:
        cursor.executemany(
            "DELETE FROM downloaded_media WHERE media_id = ?", [(m,) for m in to_delete]
        )
        conn.commit()
    return len(to_delete)


def get_feeder_download_stats(conn: sqlite3.Connection) -> dict:
    """
    Query database for image and video download statistics grouped by feeder.
    Returns counts for past hour, past day (24h), past week (7d), and all time.
    """
    now = datetime.now(timezone.utc)
    hour_cutoff = (now - timedelta(hours=1)).isoformat()
    day_cutoff = (now - timedelta(days=1)).isoformat()
    week_cutoff = (now - timedelta(days=7)).isoformat()

    cursor = conn.cursor()

    # Query all feeder statistics
    query = """
    SELECT
        COALESCE(NULLIF(feeder_name, ''), 'Bird Buddy') as feeder,
        -- Past hour
        SUM(CASE WHEN downloaded_at >= ? AND media_type = 'image' THEN 1 ELSE 0 END) as h_images,
        SUM(CASE WHEN downloaded_at >= ? AND media_type = 'video' THEN 1 ELSE 0 END) as h_videos,
        -- Past 24 hours
        SUM(CASE WHEN downloaded_at >= ? AND media_type = 'image' THEN 1 ELSE 0 END) as d_images,
        SUM(CASE WHEN downloaded_at >= ? AND media_type = 'video' THEN 1 ELSE 0 END) as d_videos,
        -- Past 7 days
        SUM(CASE WHEN downloaded_at >= ? AND media_type = 'image' THEN 1 ELSE 0 END) as w_images,
        SUM(CASE WHEN downloaded_at >= ? AND media_type = 'video' THEN 1 ELSE 0 END) as w_videos,
        -- All time
        SUM(CASE WHEN media_type = 'image' THEN 1 ELSE 0 END) as total_images,
        SUM(CASE WHEN media_type = 'video' THEN 1 ELSE 0 END) as total_videos,
        MAX(downloaded_at) as latest_download
    FROM downloaded_media
    GROUP BY feeder
    ORDER BY total_images + total_videos DESC
    """

    cursor.execute(
        query,
        (hour_cutoff, hour_cutoff, day_cutoff, day_cutoff, week_cutoff, week_cutoff),
    )
    rows = cursor.fetchall()

    feeders = {}
    totals = {
        "past_hour": {"images": 0, "videos": 0, "total": 0},
        "past_day": {"images": 0, "videos": 0, "total": 0},
        "past_week": {"images": 0, "videos": 0, "total": 0},
        "all_time": {"images": 0, "videos": 0, "total": 0},
    }

    for row in rows:
        fname = row[0]
        h_img, h_vid = row[1] or 0, row[2] or 0
        d_img, d_vid = row[3] or 0, row[4] or 0
        w_img, w_vid = row[5] or 0, row[6] or 0
        tot_img, tot_vid = row[7] or 0, row[8] or 0
        latest_dl = row[9]

        feeders[fname] = {
            "feeder_name": fname,
            "past_hour": {"images": h_img, "videos": h_vid, "total": h_img + h_vid},
            "past_day": {"images": d_img, "videos": d_vid, "total": d_img + d_vid},
            "past_week": {"images": w_img, "videos": w_vid, "total": w_img + w_vid},
            "all_time": {
                "images": tot_img,
                "videos": tot_vid,
                "total": tot_img + tot_vid,
            },
            "latest_download": latest_dl,
        }

        totals["past_hour"]["images"] += h_img
        totals["past_hour"]["videos"] += h_vid
        totals["past_hour"]["total"] += h_img + h_vid

        totals["past_day"]["images"] += d_img
        totals["past_day"]["videos"] += d_vid
        totals["past_day"]["total"] += d_img + d_vid

        totals["past_week"]["images"] += w_img
        totals["past_week"]["videos"] += w_vid
        totals["past_week"]["total"] += w_img + w_vid

        totals["all_time"]["images"] += tot_img
        totals["all_time"]["videos"] += tot_vid
        totals["all_time"]["total"] += tot_img + tot_vid

    # Recent downloads
    cursor.execute("""
        SELECT media_id, feeder_name, species_name, media_type, created_at, downloaded_at, file_path
        FROM downloaded_media
        ORDER BY downloaded_at DESC
        LIMIT 15
        """)
    recent_rows = cursor.fetchall()
    recent_downloads = []
    for r in recent_rows:
        recent_downloads.append(
            {
                "media_id": r[0],
                "feeder_name": r[1] or "Bird Buddy",
                "species_name": r[2] or "Unknown",
                "media_type": r[3],
                "created_at": r[4],
                "downloaded_at": r[5],
                "filename": os.path.basename(r[6]) if r[6] else "",
            }
        )

    return {
        "generated_at": now.isoformat(),
        "feeders": feeders,
        "totals": totals,
        "recent_downloads": recent_downloads,
    }


def sanitize_filename(name: str) -> str:
    """Sanitize string for use as a directory or filename component."""
    if not name:
        return "Unknown"
    clean = re.sub(r'[\\/*?:"<>|]', "", name).strip()
    return clean if clean else "Unknown"


def parse_iso_datetime(dt_str: str | None) -> datetime | None:
    """Parse ISO datetime string into datetime object."""
    if not dt_str:
        return None
    try:
        if dt_str.endswith("Z"):
            dt_str = dt_str[:-1] + "+00:00"
        return datetime.fromisoformat(dt_str)
    except Exception as e:
        logger.debug(f"Failed to parse datetime '{dt_str}': {e}")
        return None


def apply_timestamps_and_exif(file_path: str, dt: datetime, is_image: bool):
    """Apply timestamp metadata to file system (mtime) and EXIF (for JPEG images)."""
    try:
        epoch = dt.timestamp()
        os.utime(file_path, (epoch, epoch))
    except Exception as e:
        logger.warning(f"Could not set file mtime on {file_path}: {e}")

    if is_image and file_path.lower().endswith((".jpg", ".jpeg")):
        try:
            formatted_date = dt.strftime("%Y:%m:%d %H:%M:%S")
            exif_dict = {
                "0th": {piexif.ImageIFD.DateTime: formatted_date.encode("utf-8")},
                "Exif": {
                    piexif.ExifIFD.DateTimeOriginal: formatted_date.encode("utf-8"),
                    piexif.ExifIFD.DateTimeDigitized: formatted_date.encode("utf-8"),
                },
            }
            exif_bytes = piexif.dump(exif_dict)
            piexif.insert(exif_bytes, file_path)
            os.utime(file_path, (epoch, epoch))
        except Exception as e:
            logger.debug(f"Could not write EXIF metadata to {file_path}: {e}")


async def download_file(url: str, dest_path: str, stop_checker=None) -> bool:
    """Download file from URL atomically using temporary file with explicit connection and socket timeouts."""
    os.makedirs(os.path.dirname(dest_path), exist_ok=True)
    temp_path = dest_path + ".tmp"
    timeout = aiohttp.ClientTimeout(total=120, connect=15, sock_read=30)

    try:
        async with aiohttp.ClientSession(timeout=timeout) as session:
            async with session.get(url) as resp:
                if resp.status != 200:
                    logger.error(f"Download failed with status {resp.status} for {url}")
                    return False
                with open(temp_path, "wb") as f:
                    while True:
                        if stop_checker and stop_checker():
                            logger.info(
                                "Download cancelled mid-stream due to stop request."
                            )
                            f.close()
                            if os.path.exists(temp_path):
                                os.remove(temp_path)
                            return False
                        chunk = await resp.content.read(65536)
                        if not chunk:
                            break
                        f.write(chunk)
        os.replace(temp_path, dest_path)
        return True
    except asyncio.CancelledError:
        logger.info("Download task cancelled.")
        if os.path.exists(temp_path):
            try:
                os.remove(temp_path)
            except Exception:
                pass
        raise
    except Exception as e:
        logger.error(f"Error downloading {url}: {e}")
        if os.path.exists(temp_path):
            try:
                os.remove(temp_path)
            except Exception:
                pass
        return False


def extract_species_name(sighting) -> str:
    """Extract human-readable species name from postcard sighting."""
    try:
        if hasattr(sighting, "report") and sighting.report:
            for s in sighting.report.sightings:
                if s.species and s.species.name:
                    return s.species.name
    except Exception:
        pass
    return "Unrecognized Species"


class SafeDict(dict):
    """Dictionary subclass that keeps missing format keys intact instead of throwing KeyError."""

    def __missing__(self, key):
        return f"{{{key}}}"


def build_template_vars(
    media_id: str,
    media_type: str,
    created_at_str: str,
    feeder_name: str,
    feeder_id: str,
    species_name: str,
    owner_name: str = "Unknown Owner",
    postcard_id: str | None = None,
    sighting_id: str | None = None,
) -> dict:
    """Build dictionary of variables for directory and filename templates."""
    dt = parse_iso_datetime(created_at_str)
    clean_feeder = sanitize_filename(feeder_name)
    clean_species = sanitize_filename(species_name)
    clean_owner = sanitize_filename(owner_name)
    ext = "jpg" if media_type == "image" else "mp4"

    effective_postcard_id = postcard_id or media_id
    effective_sighting_id = sighting_id or effective_postcard_id

    return {
        "feeder_name": clean_feeder,
        "feeder": clean_feeder,
        "feeder_id": feeder_id,
        "species_name": clean_species,
        "species": clean_species,
        "owner_name": clean_owner,
        "owner": clean_owner,
        "year": dt.strftime("%Y") if dt else "0000",
        "YYYY": dt.strftime("%Y") if dt else "0000",
        "year_short": dt.strftime("%y") if dt else "00",
        "YY": dt.strftime("%y") if dt else "00",
        "month": dt.strftime("%m") if dt else "00",
        "MM": dt.strftime("%m") if dt else "00",
        "day": dt.strftime("%d") if dt else "00",
        "DD": dt.strftime("%d") if dt else "00",
        "hour": dt.strftime("%H") if dt else "00",
        "HH": dt.strftime("%H") if dt else "00",
        "minute": dt.strftime("%M") if dt else "00",
        "min": dt.strftime("%M") if dt else "00",
        "mm": dt.strftime("%M") if dt else "00",
        "second": dt.strftime("%S") if dt else "00",
        "sec": dt.strftime("%S") if dt else "00",
        "SS": dt.strftime("%S") if dt else "00",
        "date": dt.strftime("%Y%m%d") if dt else "00000000",
        "iso_date": dt.strftime("%Y-%m-%d") if dt else "0000-00-00",
        "time": dt.strftime("%H%M%S") if dt else "000000",
        "iso_time": dt.strftime("%H-%M-%S") if dt else "00-00-00",
        "media_id": media_id,
        "id": media_id,
        "media_id_short": media_id[:8],
        "id_short": media_id[:8],
        "postcard_id": effective_postcard_id,
        "postcard": effective_postcard_id,
        "postcard_id_short": effective_postcard_id[:8],
        "postcard_short": effective_postcard_id[:8],
        "detection_id": effective_postcard_id,
        "detection": effective_postcard_id,
        "detection_id_short": effective_postcard_id[:8],
        "detection_short": effective_postcard_id[:8],
        "sighting_id": effective_sighting_id,
        "sighting": effective_sighting_id,
        "sighting_id_short": effective_sighting_id[:8],
        "sighting_short": effective_sighting_id[:8],
        "media_type": media_type,
        "type": media_type,
        "ext": ext,
        "extension": ext,
    }


def render_dest_path(
    download_dir: str,
    dir_template: str,
    filename_template: str,
    template_vars: dict,
) -> tuple[str, str, str]:
    """
    Renders destination directory, filename, and full path given templates and template variables.
    Returns tuple: (formatted_dir, formatted_filename, full_dest_path)
    """
    raw_dir = dir_template.format_map(SafeDict(template_vars))
    dir_parts = [
        sanitize_filename(part)
        for part in raw_dir.replace("\\", "/").split("/")
        if part and part != "."
    ]
    formatted_dir = os.path.join(*dir_parts) if dir_parts else ""

    raw_filename = filename_template.format_map(SafeDict(template_vars))
    ext = template_vars["ext"]

    # Append extension if template does not format to ending with .ext or .extension
    if not raw_filename.lower().endswith(f".{ext}"):
        raw_filename = f"{raw_filename}.{ext}"

    filename_base, filename_ext = os.path.splitext(raw_filename)
    clean_base = sanitize_filename(filename_base)
    formatted_filename = f"{clean_base}{filename_ext}"

    dest_path = os.path.join(download_dir, formatted_dir, formatted_filename)
    return formatted_dir, formatted_filename, dest_path


class BirdBuddyDownloader:
    def __init__(self, args, conn: sqlite3.Connection):
        self.args = args
        self.conn = conn
        self.bb: BirdBuddy | None = None
        self.feeders_map = {}
        self.stop_requested = False
        self.sync_trigger_event = asyncio.Event()

        # Operational state metrics for web dashboard & monitoring
        self.start_time = datetime.now(timezone.utc)
        self.last_sync_time: datetime | None = None
        self.last_sync_status = "Not run yet"
        self.last_sync_downloaded = 0
        self.is_syncing = False
        self.next_sync_time: datetime | None = None
        self.last_error: str | None = None

        self.dir_template = (
            getattr(self.args, "dir_template", None)
            or os.getenv("DIR_TEMPLATE")
            or "{feeder_name}/{species_name}"
        )
        self.filename_template = (
            getattr(self.args, "filename_template", None)
            or os.getenv("FILENAME_TEMPLATE")
            or "{year}{month}{day}_{hour}{minute}{second}_{media_id_short}.{ext}"
        )

    def request_stop(self):
        """Signal downloader to stop processing immediately."""
        self.stop_requested = True
        self.sync_trigger_event.set()

    def trigger_sync(self):
        """Trigger an immediate sync cycle without waiting for interval."""
        self.sync_trigger_event.set()

    async def authenticate(self) -> bool:
        """
        Authenticate with Bird Buddy API using persistent session reuse and token refresh.
        Only performs a full username/password sign-in when initial login is needed
        or when the refresh token has expired.
        """
        if self.stop_requested:
            return False

        username = self.args.username or os.getenv("USERNAME")
        password = self.args.password or os.getenv("PASSWORD")

        if not username or not password:
            logger.error(
                "Bird Buddy credentials not specified. Set USERNAME & PASSWORD in .env or pass --username/--password."
            )
            self.last_error = "Credentials missing"
            return False

        # Attempt to reuse existing BirdBuddy instance & refresh session tokens
        if self.bb is not None:
            try:
                success = await self.bb.refresh()
                if success:
                    self.feeders_map = self.bb.feeders or {}
                    logger.debug(
                        "Successfully refreshed existing Bird Buddy session and feeder data."
                    )
                    return True
                logger.warning(
                    "Session refresh returned False; re-authenticating with credentials..."
                )
            except Exception as e:
                logger.warning(
                    f"Session refresh failed ({e}); re-authenticating with credentials..."
                )

        # Create new BirdBuddy instance and perform full authentication
        logger.info(f"Authenticating with Bird Buddy account: {username}")
        self.bb = BirdBuddy(username, password)
        try:
            success = await self.bb.refresh()
            if not success:
                logger.error("Bird Buddy authentication failed.")
                self.last_error = "Authentication failed"
                return False

            self.feeders_map = self.bb.feeders or {}
            logger.info(
                f"Successfully authenticated! Found {len(self.feeders_map)} connected camera feeder(s):"
            )
            for fid, fdata in self.feeders_map.items():
                name = fdata.get("name", "Unnamed Feeder")
                owner = fdata.get("ownerName", "Unknown Owner")
                logger.info(f" - Feeder '{name}' (ID: {fid}, Owner: {owner})")
            self.last_error = None
            return True
        except Exception as e:
            logger.error(f"Failed to authenticate: {e}")
            self.last_error = f"Auth error: {str(e)}"
            return False

    async def process_media_item(
        self,
        media_id: str,
        media_type: str,
        content_url: str,
        created_at_str: str,
        feeder_name: str,
        feeder_id: str,
        species_name: str,
        owner_name: str = "Unknown Owner",
        postcard_id: str | None = None,
        sighting_id: str | None = None,
    ) -> str:
        """Process and download a single media item (image or video).
        Returns status: 'downloaded', 'would_download', 'skipped', or 'filtered'.
        """
        if self.stop_requested or not content_url:
            return "filtered"

        if media_type == "image" and self.args.no_images:
            return "filtered"
        if media_type == "video" and self.args.no_videos:
            return "filtered"

        dt = parse_iso_datetime(created_at_str)

        owner_name = owner_name or self.feeders_map.get(feeder_id, {}).get(
            "ownerName", "Unknown Owner"
        )
        t_vars = build_template_vars(
            media_id=media_id,
            media_type=media_type,
            created_at_str=created_at_str,
            feeder_name=feeder_name,
            feeder_id=feeder_id,
            species_name=species_name,
            owner_name=owner_name,
            postcard_id=postcard_id,
            sighting_id=sighting_id,
        )

        formatted_dir, filename, dest_path = render_dest_path(
            download_dir=self.args.download_dir,
            dir_template=self.dir_template,
            filename_template=self.filename_template,
            template_vars=t_vars,
        )

        already_downloaded = is_media_downloaded(self.conn, media_id) or os.path.exists(
            dest_path
        )

        if already_downloaded:
            if (
                not is_media_downloaded(self.conn, media_id)
                and os.path.exists(dest_path)
                and not self.args.dry_run
            ):
                record_media_downloaded(
                    self.conn,
                    media_id,
                    feeder_id,
                    feeder_name,
                    species_name,
                    media_type,
                    created_at_str,
                    dest_path,
                )
            logger.debug(f"Media {media_id} already downloaded. Skipping.")
            return "skipped"

        if self.args.dry_run:
            logger.info(
                f"[DRY-RUN] Would download [{media_type.upper()}] from '{feeder_name}' ({species_name}): {filename} -> {dest_path}"
            )
            return "would_download"

        logger.info(
            f"Downloading [{media_type.upper()}] from '{feeder_name}' ({species_name}): {filename}"
        )
        success = await download_file(
            content_url, dest_path, stop_checker=lambda: self.stop_requested
        )
        if success and not self.stop_requested:
            if dt:
                apply_timestamps_and_exif(
                    dest_path, dt, is_image=(media_type == "image")
                )
            record_media_downloaded(
                self.conn,
                media_id,
                feeder_id,
                feeder_name,
                species_name,
                media_type,
                created_at_str,
                dest_path,
            )
            logger.info(f"Successfully downloaded and timestamped: {dest_path}")
            return "downloaded"
        return "filtered"

    async def sync_feed(self) -> int:
        """Sync media items from Bird Buddy feed across all cameras. Returns count of newly downloaded items."""
        mode_str = " (Dry-Run mode)" if self.args.dry_run else ""
        logger.info(f"Syncing media from feed{mode_str}...")

        # Calculate incremental sync cutoff if full sync is not forced
        feed_cutoff_dt = None
        if not getattr(self.args, "full_sync", False):
            latest_db_dt = get_latest_download_timestamp(self.conn)
            if latest_db_dt:
                buffer_hours = getattr(self.args, "buffer_hours", 2.0)
                feed_cutoff_dt = latest_db_dt - timedelta(hours=buffer_hours)
                logger.info(
                    f"Incremental sync: latest DB capture at {latest_db_dt.isoformat()}. "
                    f"Going back {buffer_hours}h to cutoff at {feed_cutoff_dt.isoformat()}."
                )

        cursor = None
        page = 0
        total_downloaded = 0
        total_would_download = 0
        total_skipped = 0
        reached_cutoff = False

        while not self.stop_requested and not reached_cutoff:
            page += 1
            if self.args.max_pages > 0 and page > self.args.max_pages:
                logger.info(f"Reached max pages limit ({self.args.max_pages}).")
                break

            logger.debug(f"Fetching feed page {page}...")
            try:
                feed = await self.bb.feed(first=50, after=cursor)
            except Exception as e:
                if self.stop_requested:
                    break
                logger.error(f"Error fetching feed page {page}: {e}")
                break

            if self.stop_requested:
                break

            edges = list(feed.edges)
            if not edges:
                logger.info("Reached end of feed.")
                break

            logger.info(f"Processing feed page {page} ({len(edges)} items)...")
            for edge in edges:
                if self.stop_requested:
                    logger.info("Stop requested. Halting feed processing.")
                    break

                node = edge.node
                node_created_at = node.created_at

                # Stop pagination if node is older than feed_cutoff_dt
                if (
                    feed_cutoff_dt
                    and node_created_at
                    and node_created_at < feed_cutoff_dt
                ):
                    logger.info(
                        f"Reached feed item created at {node_created_at.isoformat()}, "
                        f"which is older than cutoff {feed_cutoff_dt.isoformat()}. Stopping feed pagination."
                    )
                    reached_cutoff = True
                    break

                node_type = node.get("__typename")

                if node_type == "FeedItemNewPostcard":
                    postcard_id = node.get("id")
                    try:
                        sighting = await self.bb.sighting_from_postcard(postcard_id)
                        feeder_info = (sighting.feeder if sighting else {}) or {}
                        feeder_id = feeder_info.get("id", "unknown_feeder")
                        feeder_name = feeder_info.get("name", "Unknown Feeder")
                        owner_name = feeder_info.get(
                            "ownerName"
                        ) or self.feeders_map.get(feeder_id, {}).get(
                            "ownerName", "Unknown Owner"
                        )

                        sighting_id = None
                        if (
                            sighting
                            and hasattr(sighting, "report")
                            and sighting.report
                            and sighting.report.sightings
                        ):
                            sighting_id = sighting.report.sightings[0].id

                        if (
                            self.args.feeder_filter
                            and self.args.feeder_filter.lower()
                            not in feeder_name.lower()
                        ):
                            continue

                        species_name = extract_species_name(sighting)

                        # Process image media
                        if sighting and hasattr(sighting, "medias") and sighting.medias:
                            for m in sighting.medias:
                                if self.stop_requested:
                                    break
                                mid = m.id if hasattr(m, "id") else m.get("id")
                                created_at = (
                                    m.created_at.isoformat()
                                    if hasattr(m, "created_at") and m.created_at
                                    else m.get("createdAt")
                                )
                                content_url = (
                                    m.content_url
                                    if hasattr(m, "content_url")
                                    else m.get("contentUrl")
                                )
                                status = await self.process_media_item(
                                    mid,
                                    "image",
                                    content_url,
                                    created_at,
                                    feeder_name,
                                    feeder_id,
                                    species_name,
                                    owner_name,
                                    postcard_id,
                                    sighting_id,
                                )
                                if status == "downloaded":
                                    total_downloaded += 1
                                elif status == "would_download":
                                    total_would_download += 1
                                elif status == "skipped":
                                    total_skipped += 1

                        # Process video media
                        if (
                            sighting
                            and hasattr(sighting, "video_media")
                            and sighting.video_media
                        ):
                            for vm in sighting.video_media:
                                if self.stop_requested:
                                    break
                                vmid = vm.id if hasattr(vm, "id") else vm.get("id")
                                created_at = (
                                    vm.created_at.isoformat()
                                    if hasattr(vm, "created_at") and vm.created_at
                                    else vm.get("createdAt")
                                )
                                content_url = (
                                    vm.content_url
                                    if hasattr(vm, "content_url")
                                    else vm.get("contentUrl")
                                )
                                status = await self.process_media_item(
                                    vmid,
                                    "video",
                                    content_url,
                                    created_at,
                                    feeder_name,
                                    feeder_id,
                                    species_name,
                                    owner_name,
                                    postcard_id,
                                    sighting_id,
                                )
                                if status == "downloaded":
                                    total_downloaded += 1
                                elif status == "would_download":
                                    total_would_download += 1
                                elif status == "skipped":
                                    total_skipped += 1

                    except Exception as e:
                        if self.stop_requested:
                            break
                        logger.error(f"Error processing postcard {postcard_id}: {e}")

                elif node_type in [
                    "FeedItemCollectedPostcard",
                    "FeedItemSpeciesSighting",
                    "FeedItemSpeciesUnlocked",
                    "FeedItemMediaLiked",
                ]:
                    medias = node.get("medias", [])
                    if "media" in node and node["media"]:
                        medias.append(node["media"])

                    species_info = node.get("species", {}) or node.get(
                        "collection", {}
                    ).get("species", {})
                    species_name = (
                        species_info.get("name", "Unrecognized Species")
                        if isinstance(species_info, dict)
                        else "Unrecognized Species"
                    )

                    postcard_id = node.get("id")

                    for m in medias:
                        if self.stop_requested:
                            break
                        mid = m.get("id")
                        content_url = m.get("contentUrl")
                        created_at = m.get("createdAt")
                        is_video = m.get("__typename") == "MediaVideo"
                        mtype = "video" if is_video else "image"
                        feeder_name = "Bird Buddy"
                        feeder_id = "feed"

                        status = await self.process_media_item(
                            mid,
                            mtype,
                            content_url,
                            created_at,
                            feeder_name,
                            feeder_id,
                            species_name,
                            "Unknown Owner",
                            postcard_id,
                        )
                        if status == "downloaded":
                            total_downloaded += 1
                        elif status == "would_download":
                            total_would_download += 1
                        elif status == "skipped":
                            total_skipped += 1

            cursor = feed.page_end_cursor
            if not cursor or self.stop_requested:
                if not self.stop_requested and not reached_cutoff:
                    logger.info("End of feed reached.")
                break

        if self.args.dry_run:
            logger.info(
                f"[DRY-RUN SUMMARY] Would download: {total_would_download} item(s) | Skipped (already downloaded): {total_skipped} item(s)"
            )
        elif self.stop_requested:
            logger.info(
                f"Feed sync stopped early. Media files downloaded before stop: {total_downloaded}"
            )
        else:
            logger.info(
                f"Feed sync complete. Total new media files downloaded: {total_downloaded}"
            )
        return total_downloaded

    async def sync_collections(self) -> int:
        """Sync media items from saved user collections if available. Returns count of newly downloaded items."""
        if self.stop_requested:
            return 0

        logger.info("Checking collections...")
        total_downloaded = 0
        try:
            data = await self.bb._make_request(query=me_queries.COLLECTIONS)
            collections = data.get("me", {}).get("collections", [])
            if not collections or self.stop_requested:
                return 0

            logger.info(
                f"Found {len(collections)} collection(s). Syncing collection media..."
            )
            total_would_download = 0
            total_skipped = 0

            for c in collections:
                if self.stop_requested:
                    break
                cid = c.get("id")
                species = (
                    c.get("species", {}).get("name", "Unknown Species")
                    if "species" in c
                    else "Unknown Species"
                )

                cursor = None
                while not self.stop_requested:
                    media_data = await self.bb._make_request(
                        query=me_queries.COLLECTIONS_MEDIA,
                        variables={"collectionId": cid, "first": 50, "after": cursor},
                    )
                    collection_obj = media_data.get("collection", {})
                    media_conn = (
                        collection_obj.get("media", {}) if collection_obj else {}
                    )
                    edges = media_conn.get("edges", [])
                    if not edges or self.stop_requested:
                        break

                    for edge in edges:
                        if self.stop_requested:
                            break
                        cnode = edge.get("node", {})
                        m = cnode.get("media", {})
                        mid = m.get("id")
                        content_url = m.get("contentUrl")
                        created_at = m.get("createdAt")
                        is_video = m.get("__typename") == "MediaVideo"
                        mtype = "video" if is_video else "image"
                        feeder_name = cnode.get("feederName", "Bird Buddy")
                        feeder_id = "collection"
                        owner_name = cnode.get("ownerName", "Unknown Owner")

                        if (
                            self.args.feeder_filter
                            and self.args.feeder_filter.lower()
                            not in feeder_name.lower()
                        ):
                            continue

                        status = await self.process_media_item(
                            mid,
                            mtype,
                            content_url,
                            created_at,
                            feeder_name,
                            feeder_id,
                            species,
                            owner_name,
                        )
                        if status == "downloaded":
                            total_downloaded += 1
                        elif status == "would_download":
                            total_would_download += 1
                        elif status == "skipped":
                            total_skipped += 1

                    page_info = media_conn.get("pageInfo", {})
                    if not page_info.get("hasNextPage") or self.stop_requested:
                        break
                    cursor = page_info.get("endCursor")

            if self.args.dry_run:
                logger.info(
                    f"[DRY-RUN COLLECTIONS SUMMARY] Would download: {total_would_download} item(s) | Skipped: {total_skipped} item(s)"
                )
            else:
                logger.info(
                    f"Collections sync complete. New media downloaded: {total_downloaded}"
                )
        except Exception as e:
            if not self.stop_requested:
                logger.debug(f"Collections sync skipped or encountered error: {e}")
        return total_downloaded

    async def get_account_info(self) -> dict:
        """Gather detailed information about connected feeders, species breakdown, and event ranges."""
        if not self.bb and not await self.authenticate():
            return {}

        feeders_info = []
        feeder_events = {}

        cursor = self.conn.cursor()
        cursor.execute("""
            SELECT feeder_name,
                   MIN(created_at) as earliest,
                   MAX(created_at) as latest,
                   COUNT(*) as total_media
            FROM downloaded_media
            GROUP BY feeder_name
            """)
        for row in cursor.fetchall():
            feeder_events[row[0]] = {
                "earliest": row[1],
                "latest": row[2],
                "total_media": row[3],
            }

        for fid, f in self.feeders_map.items():
            fname = f.get("name", "Unnamed Feeder")
            events = feeder_events.get(
                fname, {"earliest": None, "latest": None, "total_media": 0}
            )

            battery = f.get("battery", {})
            food = f.get("food", {})
            signal_info = f.get("signal", {})
            temp = f.get("temperature", {})

            f_detail = {
                "id": fid,
                "name": fname,
                "owner": f.get("ownerName", "Unknown Owner"),
                "location": f"{f.get('locationCity', 'Unknown')}, {f.get('locationCountry', 'Unknown')}",
                "state": f.get("state", "UNKNOWN"),
                "battery": {
                    "percentage": battery.get("percentage"),
                    "charging": battery.get("charging"),
                    "state": battery.get("state"),
                },
                "food_state": food.get("state"),
                "signal": {
                    "state": signal_info.get("state"),
                    "value_dbm": signal_info.get("value"),
                },
                "temperature": temp.get("value"),
                "earliest_event": events["earliest"],
                "latest_event": events["latest"],
                "total_downloaded_media": events["total_media"],
            }

            if "serialNumber" in f:
                f_detail["serial_number"] = f.get("serialNumber")
            if "firmwareVersion" in f:
                f_detail["firmware_version"] = f.get("firmwareVersion")

            feeders_info.append(f_detail)

        species_stats = []
        cursor.execute("""
            SELECT species_name,
                   SUM(CASE WHEN media_type = 'image' THEN 1 ELSE 0 END) as images,
                   SUM(CASE WHEN media_type = 'video' THEN 1 ELSE 0 END) as videos,
                   COUNT(*) as total
            FROM downloaded_media
            GROUP BY species_name
            ORDER BY total DESC
            """)
        for row in cursor.fetchall():
            species_stats.append(
                {
                    "species": row[0],
                    "images": row[1],
                    "videos": row[2],
                    "total": row[3],
                }
            )

        return {
            "account_user": (
                self.bb.user.get("email") if self.bb and self.bb.user else None
            ),
            "feeders": feeders_info,
            "species_summary": species_stats,
        }

    async def run_once(self):
        """Execute one download sync cycle and clean up old database records."""
        self.is_syncing = True
        self.last_sync_status = "Syncing..."
        total_new = 0

        try:
            if not await self.authenticate():
                self.last_sync_status = "Auth Failed"
                return

            feed_count = await self.sync_feed()
            total_new += feed_count

            if not self.stop_requested:
                coll_count = await self.sync_collections()
                total_new += coll_count

            # Database cleanup for entries older than db_retention_days (default 14 days)
            if not self.stop_requested and not self.args.dry_run:
                retention_days = getattr(self.args, "db_retention_days", 14)
                if retention_days > 0:
                    num_cleaned = cleanup_old_db_records(self.conn, retention_days)
                    if num_cleaned > 0:
                        logger.info(
                            f"Database cleanup: removed {num_cleaned} record(s) older than {retention_days} days."
                        )

            self.last_sync_time = datetime.now(timezone.utc)
            self.last_sync_downloaded = total_new
            self.last_sync_status = "Success"
            self.last_error = None
        except Exception as e:
            logger.error(f"Unhandled error in sync cycle: {e}")
            self.last_sync_status = f"Error: {e}"
            self.last_error = str(e)
        finally:
            self.is_syncing = False


# ==============================================================================
# Embedded Web Dashboard & API Endpoints
# ==============================================================================

HTML_DASHBOARD = """<!DOCTYPE html>
<html lang="en">
<head>
  <meta charset="UTF-8">
  <meta name="viewport" content="width=device-width, initial-scale=1.0">
  <title>Bird Buddy Downloader Dashboard</title>
  <style>
    :root {
      --bg-primary: #0f172a;
      --bg-secondary: #1e293b;
      --bg-card: #334155;
      --text-primary: #f8fafc;
      --text-secondary: #94a3b8;
      --accent: #10b981;
      --accent-hover: #059669;
      --border: #475569;
      --danger: #ef4444;
      --warning: #f59e0b;
      --badge-blue: #3b82f6;
    }
    * { box-sizing: border-box; margin: 0; padding: 0; font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", Roboto, Oxygen, Ubuntu, Cantarell, sans-serif; }
    body { background-color: var(--bg-primary); color: var(--text-primary); padding: 24px; min-height: 100vh; }
    .container { max-width: 1200px; margin: 0 auto; }
    header { display: flex; justify-content: space-between; align-items: center; margin-bottom: 24px; padding-bottom: 16px; border-bottom: 1px solid var(--border); flex-wrap: wrap; gap: 16px; }
    .header-title { display: flex; align-items: center; gap: 12px; }
    .header-title h1 { font-size: 1.6rem; font-weight: 700; color: #fff; }
    .header-title .icon { font-size: 1.8rem; }
    .header-actions { display: flex; align-items: center; gap: 12px; }
    .btn { background-color: var(--accent); color: white; border: none; padding: 8px 16px; border-radius: 6px; font-weight: 600; cursor: pointer; font-size: 0.9rem; transition: background 0.2s; display: inline-flex; align-items: center; gap: 6px; }
    .btn:hover { background-color: var(--accent-hover); }
    .btn:disabled { opacity: 0.5; cursor: not-allowed; }
    .auto-refresh { display: flex; align-items: center; gap: 6px; font-size: 0.85rem; color: var(--text-secondary); }
    
    /* Metrics Grid */
    .grid { display: grid; grid-template-columns: repeat(auto-fit, minmax(220px, 1fr)); gap: 16px; margin-bottom: 24px; }
    .card { background-color: var(--bg-secondary); border: 1px solid var(--border); border-radius: 8px; padding: 16px; box-shadow: 0 4px 6px -1px rgba(0, 0, 0, 0.1); }
    .card-label { font-size: 0.8rem; text-transform: uppercase; letter-spacing: 0.05em; color: var(--text-secondary); margin-bottom: 6px; }
    .card-value { font-size: 1.4rem; font-weight: 700; color: #fff; display: flex; align-items: baseline; gap: 8px; }
    .card-sub { font-size: 0.8rem; color: var(--text-secondary); margin-top: 4px; }

    /* Badge */
    .badge { display: inline-block; padding: 3px 8px; border-radius: 9999px; font-size: 0.75rem; font-weight: 600; text-transform: uppercase; }
    .badge-success { background: rgba(16, 185, 129, 0.2); color: #34d399; border: 1px solid rgba(16, 185, 129, 0.4); }
    .badge-syncing { background: rgba(59, 130, 246, 0.2); color: #60a5fa; border: 1px solid rgba(59, 130, 246, 0.4); animation: pulse 1.5s infinite; }
    .badge-error { background: rgba(239, 68, 68, 0.2); color: #f87171; border: 1px solid rgba(239, 68, 68, 0.4); }
    .badge-idle { background: rgba(148, 163, 184, 0.2); color: #cbd5e1; border: 1px solid rgba(148, 163, 184, 0.4); }
    @keyframes pulse { 0%, 100% { opacity: 1; } 50% { opacity: 0.5; } }

    /* Table Section */
    .section { background-color: var(--bg-secondary); border: 1px solid var(--border); border-radius: 8px; padding: 20px; margin-bottom: 24px; }
    .section-title { font-size: 1.15rem; font-weight: 600; margin-bottom: 16px; display: flex; justify-content: space-between; align-items: center; }
    .table-container { overflow-x: auto; }
    table { width: 100%; border-collapse: collapse; text-align: left; font-size: 0.9rem; }
    th { background-color: rgba(51, 65, 85, 0.5); color: var(--text-secondary); font-weight: 600; text-transform: uppercase; font-size: 0.75rem; letter-spacing: 0.05em; padding: 12px 14px; border-bottom: 1px solid var(--border); }
    td { padding: 12px 14px; border-bottom: 1px solid rgba(71, 85, 105, 0.5); color: var(--text-primary); }
    tr:last-child td { border-bottom: none; }
    tr:hover td { background-color: rgba(51, 65, 85, 0.3); }
    .num { font-variant-numeric: tabular-nums; text-align: center; }
    .pill { display: inline-block; padding: 2px 6px; border-radius: 4px; font-size: 0.8rem; font-weight: 600; }
    .pill-img { background: rgba(59, 130, 246, 0.15); color: #93c5fd; }
    .pill-vid { background: rgba(245, 158, 11, 0.15); color: #fcd34d; }
    .empty-state { text-align: center; padding: 32px; color: var(--text-secondary); font-style: italic; }

    /* Hardware Cards */
    .feeder-grid { display: grid; grid-template-columns: repeat(auto-fit, minmax(280px, 1fr)); gap: 16px; }
    .feeder-card { background: var(--bg-card); border-radius: 6px; padding: 14px; border: 1px solid rgba(255,255,255,0.05); }
    .feeder-head { font-weight: 600; margin-bottom: 8px; display: flex; justify-content: space-between; align-items: center; }
    .feeder-stat { font-size: 0.85rem; color: var(--text-secondary); margin-bottom: 4px; display: flex; justify-content: space-between; }
    .feeder-stat span:last-child { color: var(--text-primary); font-weight: 500; }
  </style>
</head>
<body>
  <div class="container">
    <header>
      <div class="header-title">
        <span class="icon">🐦</span>
        <div>
          <h1>Bird Buddy Downloader</h1>
          <div style="font-size: 0.8rem; color: var(--text-secondary);">Automated Media Synchronization & Activity Dashboard</div>
        </div>
      </div>
      <div class="header-actions">
        <label class="auto-refresh">
          <input type="checkbox" id="autoRefreshToggle" checked> Auto-refresh (10s)
        </label>
        <button class="btn" id="syncBtn" onclick="triggerSync()">
          <span>⚡ Sync Now</span>
        </button>
      </div>
    </header>

    <!-- Top Status Metrics -->
    <div class="grid">
      <div class="card">
        <div class="card-label">Sync Status</div>
        <div class="card-value" id="syncStatusBadge"><span class="badge badge-idle">Checking...</span></div>
        <div class="card-sub" id="syncDetails">Connecting to downloader daemon...</div>
      </div>
      <div class="card">
        <div class="card-label">Sync Interval</div>
        <div class="card-value" id="syncInterval">--</div>
        <div class="card-sub" id="nextSyncTime">Next sync: --</div>
      </div>
      <div class="card">
        <div class="card-label">Last Sync Run</div>
        <div class="card-value" id="lastSyncTime">--</div>
        <div class="card-sub" id="lastSyncDownloaded">Downloaded in last run: 0 items</div>
      </div>
      <div class="card">
        <div class="card-label">Total Downloads (All Time)</div>
        <div class="card-value" id="totalAllTime">--</div>
        <div class="card-sub" id="totalBreakdown">-- images | -- videos</div>
      </div>
    </div>

    <!-- Per-Feeder Download Breakdown Table -->
    <div class="section">
      <div class="section-title">
        <span>📸 Feeder Media Downloads (Past Hour / Day / Week)</span>
        <span id="statsUpdated" style="font-size: 0.8rem; color: var(--text-secondary); font-weight: normal;"></span>
      </div>
      <div class="table-container">
        <table>
          <thead>
            <tr>
              <th>Feeder Camera</th>
              <th class="num">Past Hour</th>
              <th class="num">Past 24 Hours</th>
              <th class="num">Past 7 Days</th>
              <th class="num">All Time Total</th>
              <th>Last Downloaded</th>
            </tr>
          </thead>
          <tbody id="feederStatsBody">
            <tr><td colspan="6" class="empty-state">Loading feeder statistics...</td></tr>
          </tbody>
        </table>
      </div>
    </div>

    <!-- Connected Feeders Hardware Info -->
    <div class="section" id="hardwareSection" style="display: none;">
      <div class="section-title">📡 Connected Feeders Hardware Status</div>
      <div class="feeder-grid" id="hardwareGrid"></div>
    </div>

    <!-- Recent Activity Log -->
    <div class="section">
      <div class="section-title">🕒 Recent Media Downloads</div>
      <div class="table-container">
        <table>
          <thead>
            <tr>
              <th>Timestamp</th>
              <th>Feeder</th>
              <th>Species</th>
              <th>Type</th>
              <th>Saved File</th>
            </tr>
          </thead>
          <tbody id="recentMediaBody">
            <tr><td colspan="5" class="empty-state">No recent downloads found.</td></tr>
          </tbody>
        </table>
      </div>
    </div>
  </div>

  <script>
    let refreshTimer = null;

    function formatRelative(isoStr) {
      if (!isoStr) return "Never";
      const d = new Date(isoStr);
      const diffSec = Math.floor((new Date() - d) / 1000);
      if (diffSec < 5) return "Just now";
      if (diffSec < 60) return diffSec + "s ago";
      if (diffSec < 3600) return Math.floor(diffSec / 60) + "m ago";
      if (diffSec < 86400) return Math.floor(diffSec / 3600) + "h ago";
      return Math.floor(diffSec / 86400) + "d ago";
    }

    function formatCountdown(isoStr) {
      if (!isoStr) return "N/A";
      const d = new Date(isoStr);
      const diffSec = Math.floor((d - new Date()) / 1000);
      if (diffSec <= 0) return "Due now";
      if (diffSec < 60) return "in " + diffSec + "s";
      const mins = Math.floor(diffSec / 60);
      const secs = diffSec % 60;
      return "in " + mins + "m " + secs + "s";
    }

    async function loadStatus() {
      try {
        const resp = await fetch("/api/status");
        if (!resp.ok) throw new Error("HTTP error " + resp.status);
        const data = await resp.json();

        // 1. Update Sync Status Badge
        const badgeEl = document.getElementById("syncStatusBadge");
        const detailsEl = document.getElementById("syncDetails");
        const syncBtn = document.getElementById("syncBtn");

        if (data.is_syncing) {
          badgeEl.innerHTML = '<span class="badge badge-syncing">Syncing...</span>';
          detailsEl.innerText = "Processing feed and downloading new media...";
          syncBtn.disabled = true;
          syncBtn.innerText = "⏳ Sync in progress...";
        } else if (data.last_sync_status.startsWith("Error") || data.last_sync_status.startsWith("Auth Failed")) {
          badgeEl.innerHTML = '<span class="badge badge-error">Error</span>';
          detailsEl.innerText = data.last_error || data.last_sync_status;
          syncBtn.disabled = false;
          syncBtn.innerHTML = "<span>⚡ Sync Now</span>";
        } else {
          badgeEl.innerHTML = '<span class="badge badge-success">Active / Idle</span>';
          detailsEl.innerText = "Running continuously in container";
          syncBtn.disabled = false;
          syncBtn.innerHTML = "<span>⚡ Sync Now</span>";
        }

        // 2. Update Sync Interval
        const intervalEl = document.getElementById("syncInterval");
        const nextSyncEl = document.getElementById("nextSyncTime");
        if (data.interval_seconds > 0) {
          intervalEl.innerText = data.interval_seconds >= 60 ? (data.interval_seconds / 60) + " minutes" : data.interval_seconds + " seconds";
          nextSyncEl.innerText = "Next sync: " + formatCountdown(data.next_sync_time);
        } else {
          intervalEl.innerText = "Single Run / Manual";
          nextSyncEl.innerText = "Continuous polling disabled (INTERVAL=0)";
        }

        // 3. Update Last Sync
        const lastSyncEl = document.getElementById("lastSyncTime");
        const lastDlEl = document.getElementById("lastSyncDownloaded");
        if (data.last_sync_time) {
          lastSyncEl.innerText = formatRelative(data.last_sync_time);
          lastDlEl.innerText = "Downloaded: " + data.last_sync_downloaded + " item(s)";
        } else {
          lastSyncEl.innerText = "Not run yet";
          lastDlEl.innerText = "Waiting for initial run...";
        }

        // 4. Update Totals
        const totals = data.stats.totals;
        document.getElementById("totalAllTime").innerText = totals.all_time.total.toLocaleString();
        document.getElementById("totalBreakdown").innerText = totals.all_time.images.toLocaleString() + " images | " + totals.all_time.videos.toLocaleString() + " videos";
        document.getElementById("statsUpdated").innerText = "Updated: " + new Date().toLocaleTimeString();

        // 5. Update Feeder Breakdown Table
        const tbody = document.getElementById("feederStatsBody");
        const feeders = Object.values(data.stats.feeders || {});
        if (feeders.length === 0) {
          tbody.innerHTML = '<tr><td colspan="6" class="empty-state">No media records in database yet.</td></tr>';
        } else {
          let html = "";
          feeders.forEach(f => {
            html += `<tr>
              <td><strong>${escapeHtml(f.feeder_name)}</strong></td>
              <td class="num">
                <span class="pill pill-img">${f.past_hour.images} img</span>
                <span class="pill pill-vid">${f.past_hour.videos} vid</span>
                <strong style="margin-left:4px;">(${f.past_hour.total})</strong>
              </td>
              <td class="num">
                <span class="pill pill-img">${f.past_day.images} img</span>
                <span class="pill pill-vid">${f.past_day.videos} vid</span>
                <strong style="margin-left:4px;">(${f.past_day.total})</strong>
              </td>
              <td class="num">
                <span class="pill pill-img">${f.past_week.images} img</span>
                <span class="pill pill-vid">${f.past_week.videos} vid</span>
                <strong style="margin-left:4px;">(${f.past_week.total})</strong>
              </td>
              <td class="num">
                <span class="pill pill-img">${f.all_time.images} img</span>
                <span class="pill pill-vid">${f.all_time.videos} vid</span>
                <strong style="margin-left:4px; font-size:1.05rem;">(${f.all_time.total})</strong>
              </td>
              <td style="color:var(--text-secondary); font-size:0.85rem;">${formatRelative(f.latest_download)}</td>
            </tr>`;
          });

          // Append totals row
          html += `<tr style="background: rgba(51, 65, 85, 0.4); font-weight: bold;">
            <td>TOTALS</td>
            <td class="num"><span class="pill pill-img">${totals.past_hour.images}</span> <span class="pill pill-vid">${totals.past_hour.videos}</span> (${totals.past_hour.total})</td>
            <td class="num"><span class="pill pill-img">${totals.past_day.images}</span> <span class="pill pill-vid">${totals.past_day.videos}</span> (${totals.past_day.total})</td>
            <td class="num"><span class="pill pill-img">${totals.past_week.images}</span> <span class="pill pill-vid">${totals.past_week.videos}</span> (${totals.past_week.total})</td>
            <td class="num"><span class="pill pill-img">${totals.all_time.images}</span> <span class="pill pill-vid">${totals.all_time.videos}</span> (${totals.all_time.total})</td>
            <td>--</td>
          </tr>`;
          tbody.innerHTML = html;
        }

        // 6. Update Hardware Cards if available
        if (data.feeders_hardware && data.feeders_hardware.length > 0) {
          document.getElementById("hardwareSection").style.display = "block";
          let hwHtml = "";
          data.feeders_hardware.forEach(f => {
            const bat = f.battery ? `${f.battery.percentage ?? 'N/A'}% ${f.battery.charging ? '(⚡ Charging)' : ''}` : 'N/A';
            const sig = f.signal ? `${f.signal.state || 'N/A'} (${f.signal.value_dbm ?? 'N/A'} dBm)` : 'N/A';
            hwHtml += `<div class="feeder-card">
              <div class="feeder-head">
                <span>${escapeHtml(f.name)}</span>
                <span class="badge ${f.state === 'ONLINE' ? 'badge-success' : 'badge-idle'}">${f.state}</span>
              </div>
              <div class="feeder-stat"><span>Battery</span><span>${bat}</span></div>
              <div class="feeder-stat"><span>Food Level</span><span>${f.food_state || 'N/A'}</span></div>
              <div class="feeder-stat"><span>Wi-Fi Signal</span><span>${sig}</span></div>
              <div class="feeder-stat"><span>Temperature</span><span>${f.temperature != null ? f.temperature + '°C' : 'N/A'}</span></div>
            </div>`;
          });
          document.getElementById("hardwareGrid").innerHTML = hwHtml;
        }

        // 7. Update Recent Media Table
        const recentTbody = document.getElementById("recentMediaBody");
        const recents = data.stats.recent_downloads || [];
        if (recents.length === 0) {
          recentTbody.innerHTML = '<tr><td colspan="5" class="empty-state">No downloaded media found yet.</td></tr>';
        } else {
          let rHtml = "";
          recents.forEach(r => {
            const isVid = r.media_type === "video";
            rHtml += `<tr>
              <td style="color:var(--text-secondary); font-size:0.85rem;">${formatRelative(r.downloaded_at)}</td>
              <td>${escapeHtml(r.feeder_name)}</td>
              <td><strong>${escapeHtml(r.species_name)}</strong></td>
              <td><span class="pill ${isVid ? 'pill-vid' : 'pill-img'}">${r.media_type.toUpperCase()}</span></td>
              <td style="font-family:monospace; font-size:0.8rem; color:#94a3b8;">${escapeHtml(r.filename)}</td>
            </tr>`;
          });
          recentTbody.innerHTML = rHtml;
        }

      } catch (err) {
        console.error("Dashboard error:", err);
      }
    }

    async function triggerSync() {
      const btn = document.getElementById("syncBtn");
      btn.disabled = true;
      btn.innerText = "Triggering sync...";
      try {
        const resp = await fetch("/api/sync", { method: "POST" });
        const result = await resp.json();
        setTimeout(loadStatus, 500);
      } catch (e) {
        alert("Error triggering sync: " + e.message);
        btn.disabled = false;
        btn.innerHTML = "<span>⚡ Sync Now</span>";
      }
    }

    function escapeHtml(str) {
      if (!str) return "";
      return str.replace(/&/g, "&amp;").replace(/</g, "&lt;").replace(/>/g, "&gt;").replace(/"/g, "&quot;");
    }

    function setupAutoRefresh() {
      const toggle = document.getElementById("autoRefreshToggle");
      if (refreshTimer) clearInterval(refreshTimer);
      if (toggle.checked) {
        refreshTimer = setInterval(loadStatus, 10000);
      }
      toggle.onchange = setupAutoRefresh;
    }

    loadStatus();
    setupAutoRefresh();
  </script>
</body>
</html>
"""


async def handle_index(request: web.Request) -> web.Response:
    """Serve HTML dashboard page."""
    return web.Response(text=HTML_DASHBOARD, content_type="text/html")


async def handle_api_status(request: web.Request) -> web.Response:
    """Return JSON status report including sync info and per-feeder download counts."""
    downloader: BirdBuddyDownloader = request.app["downloader"]
    stats = get_feeder_download_stats(downloader.conn)

    hardware_feeders = []
    if downloader.feeders_map:
        for fid, f in downloader.feeders_map.items():
            battery = f.get("battery", {})
            food = f.get("food", {})
            signal_info = f.get("signal", {})
            temp = f.get("temperature", {})
            hardware_feeders.append(
                {
                    "id": fid,
                    "name": f.get("name", "Unnamed Feeder"),
                    "state": f.get("state", "UNKNOWN"),
                    "battery": {
                        "percentage": battery.get("percentage"),
                        "charging": battery.get("charging"),
                        "state": battery.get("state"),
                    },
                    "food_state": food.get("state"),
                    "signal": {
                        "state": signal_info.get("state"),
                        "value_dbm": signal_info.get("value"),
                    },
                    "temperature": temp.get("value"),
                }
            )

    data = {
        "status": "ok",
        "is_syncing": downloader.is_syncing,
        "interval_seconds": downloader.args.interval,
        "last_sync_time": (
            downloader.last_sync_time.isoformat() if downloader.last_sync_time else None
        ),
        "last_sync_status": downloader.last_sync_status,
        "last_sync_downloaded": downloader.last_sync_downloaded,
        "next_sync_time": (
            downloader.next_sync_time.isoformat() if downloader.next_sync_time else None
        ),
        "last_error": downloader.last_error,
        "uptime_seconds": int(
            (datetime.now(timezone.utc) - downloader.start_time).total_seconds()
        ),
        "feeders_hardware": hardware_feeders,
        "stats": stats,
    }
    return web.json_response(data)


async def handle_api_sync(request: web.Request) -> web.Response:
    """Trigger on-demand sync cycle immediately."""
    downloader: BirdBuddyDownloader = request.app["downloader"]
    if downloader.is_syncing:
        return web.json_response(
            {
                "status": "already_syncing",
                "message": "A sync cycle is currently active.",
            }
        )

    downloader.trigger_sync()
    return web.json_response(
        {"status": "triggered", "message": "Sync cycle triggered successfully."}
    )


async def create_web_app(downloader: BirdBuddyDownloader) -> web.Application:
    """Create and configure aiohttp web application."""
    app = web.Application()
    app["downloader"] = downloader
    app.router.add_get("/", handle_index)
    app.router.add_get("/api/status", handle_api_status)
    app.router.add_post("/api/sync", handle_api_sync)
    return app


def print_info_report(info: dict, as_json: bool = False):
    """Print clean human-readable or JSON info report."""
    if as_json:
        print(json.dumps(info, indent=2, default=str))
        return

    print("\n" + "=" * 80)
    print(
        "                        BIRD BUDDY ACCOUNT INFORMATION                         "
    )
    print("=" * 80)

    user = info.get("account_user", "N/A")
    print(f"\nAccount Email: {user}")

    feeders = info.get("feeders", [])
    print(f"\n--- CONNECTED FEEDER CAMERAS ({len(feeders)}) ---")
    for idx, f in enumerate(feeders, 1):
        print(f"\n  [{idx}] {f['name']} (ID: {f['id']})")
        print(f"      Owner: {f['owner']} | Location: {f['location']}")
        print(f"      State: {f['state']}")

        bat = f.get("battery", {})
        bat_str = (
            f"{bat.get('percentage')}%" if bat.get("percentage") is not None else "N/A"
        )
        bat_charging = "Charging" if bat.get("charging") else "Not Charging"
        bat_state = bat.get("state") or "N/A"
        print(f"      Battery: {bat_str} ({bat_state}, {bat_charging})")

        print(f"      Food Level: {f.get('food_state') or 'N/A'}")
        sig = f.get("signal", {})
        sig_str = (
            f"{sig.get('state')} ({sig.get('value_dbm')} dBm)"
            if sig.get("value_dbm")
            else "N/A"
        )
        print(f"      Wi-Fi Signal: {sig_str}")
        print(
            f"      Temperature: {f.get('temperature')}°C"
            if f.get("temperature") is not None
            else "      Temperature: N/A"
        )

        earliest = f.get("earliest_event") or "N/A"
        latest = f.get("latest_event") or "N/A"
        print(f"      Earliest Event Seen: {earliest}")
        print(f"      Latest Event Seen:   {latest}")
        print(f"      Total Media Recorded: {f.get('total_downloaded_media', 0)}")

    species_list = info.get("species_summary", [])
    print("\n" + "-" * 80)
    print("--- MEDIA SUMMARY BY BIRD SPECIES ---")
    print("-" * 80)
    print(f"  {'Species Name':<30} {'Images':>8} {'Videos':>8} {'Total Media':>12}")
    print("  " + "-" * 62)

    total_imgs = 0
    total_vids = 0
    total_all = 0

    for s in species_list:
        sp_name = s["species"][:30]
        imgs = s["images"]
        vids = s["videos"]
        tot = s["total"]
        total_imgs += imgs
        total_vids += vids
        total_all += tot
        print(f"  {sp_name:<30} {imgs:>8} {vids:>8} {tot:>12}")

    print("  " + "-" * 62)
    print(f"  {'TOTAL':<30} {total_imgs:>8} {total_vids:>8} {total_all:>12}")
    print("=" * 80 + "\n")


def str_to_bool(val: str | None, default: bool = False) -> bool:
    """Convert string environment variable or value to boolean."""
    if val is None or val == "":
        return default
    if isinstance(val, bool):
        return val
    return str(val).strip().lower() in ("true", "1", "yes", "on", "t", "y")


def str_to_int(val: str | None, default: int = 0) -> int:
    """Convert string environment variable to integer safely."""
    if val is None or val == "":
        return default
    try:
        clean_val = str(val).split("#")[0].strip()
        return int(clean_val)
    except (ValueError, TypeError):
        return default


def str_to_float(val: str | None, default: float = 0.0) -> float:
    """Convert string environment variable to float safely."""
    if val is None or val == "":
        return default
    try:
        clean_val = str(val).split("#")[0].strip()
        return float(clean_val)
    except (ValueError, TypeError):
        return default


def parse_args():
    # Pre-parse --env-file to load .env before establishing parameter defaults
    env_parser = argparse.ArgumentParser(add_help=False)
    env_parser.add_argument("--env-file", default=os.getenv("ENV_FILE", ".env"))
    known_args, _ = env_parser.parse_known_args()

    if os.path.exists(known_args.env_file):
        load_dotenv(known_args.env_file)

    parser = argparse.ArgumentParser(
        description="Automatic Bird Buddy media downloader with de-duplication, metadata timestamping, web dashboard, and feeder info."
    )
    parser.add_argument(
        "--env-file",
        default=known_args.env_file,
        help="Path to .env file containing credentials (default: .env)",
    )
    parser.add_argument(
        "--username",
        default=os.getenv("USERNAME"),
        help="Bird Buddy account email (overrides USERNAME env var)",
    )
    parser.add_argument(
        "--password",
        default=os.getenv("PASSWORD"),
        help="Bird Buddy account password (overrides PASSWORD env var)",
    )
    parser.add_argument(
        "--download-dir",
        default=os.getenv("DOWNLOAD_DIR", "./downloads"),
        help="Directory to save downloaded media (default: ./downloads or DOWNLOAD_DIR env var)",
    )
    parser.add_argument(
        "--db-path",
        default=os.getenv("DB_PATH", "./data/birdbuddy_downloader.db"),
        help="SQLite DB path for de-duplication (default: ./data/birdbuddy_downloader.db or DB_PATH env var)",
    )
    parser.add_argument(
        "--dir-template",
        default=os.getenv("DIR_TEMPLATE", "{feeder_name}/{species_name}"),
        help="Template for output subdirectories relative to download-dir (default: '{feeder_name}/{species_name}'). "
        "Variables: feeder_name, species_name, owner_name, detection_id, postcard_id, sighting_id, year, month, day, hour, minute, second, date, iso_date, time, etc.",
    )
    parser.add_argument(
        "--filename-template",
        default=os.getenv(
            "FILENAME_TEMPLATE",
            "{year}{month}{day}_{hour}{minute}{second}_{media_id_short}.{ext}",
        ),
        help="Template for output filenames (default: '{year}{month}{day}_{hour}{minute}{second}_{media_id_short}.{ext}'). "
        "Variables: year, month, day, hour, minute, second, date, time, media_id_short, detection_id_short, postcard_id_short, feeder_name, species_name, ext, etc.",
    )
    parser.add_argument(
        "--buffer-hours",
        type=float,
        default=str_to_float(os.getenv("BUFFER_HOURS"), 2.0),
        help="Hours before latest downloaded detection to start fetching feed items (default: 2.0 or BUFFER_HOURS env var)",
    )
    parser.add_argument(
        "--db-retention-days",
        type=int,
        default=str_to_int(os.getenv("DB_RETENTION_DAYS"), 14),
        help="Days to retain records in database before cleanup (default: 14 or DB_RETENTION_DAYS env var; set to 0 to disable)",
    )
    parser.add_argument(
        "--full-sync",
        action="store_true",
        default=str_to_bool(os.getenv("FULL_SYNC", "false")),
        help="Bypass latest detection cutoff and perform a full feed sync",
    )
    parser.add_argument(
        "--interval",
        type=int,
        default=str_to_int(os.getenv("INTERVAL"), 0),
        help="Interval in seconds for continuous polling mode (0 for single run or INTERVAL env var)",
    )
    parser.add_argument(
        "--max-pages",
        type=int,
        default=str_to_int(os.getenv("MAX_PAGES"), 0),
        help="Maximum feed pages to fetch (0 for unlimited or MAX_PAGES env var)",
    )
    parser.add_argument(
        "--no-images",
        action="store_true",
        default=str_to_bool(os.getenv("NO_IMAGES", "false")),
        help="Skip downloading image files",
    )
    parser.add_argument(
        "--no-videos",
        action="store_true",
        default=str_to_bool(os.getenv("NO_VIDEOS", "false")),
        help="Skip downloading video files",
    )
    parser.add_argument(
        "--feeder-filter",
        default=os.getenv("FEEDER_FILTER"),
        help="Filter downloads by feeder name (case-insensitive substring)",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        default=str_to_bool(os.getenv("DRY_RUN", "false")),
        help="Perform a dry run without downloading files or modifying database",
    )
    parser.add_argument(
        "--info",
        action="store_true",
        default=str_to_bool(os.getenv("INFO", "false")),
        help="Display feeder information, battery status, species media counts, and event date ranges",
    )
    parser.add_argument(
        "--json",
        action="store_true",
        default=str_to_bool(os.getenv("JSON", "false")),
        help="Output --info as raw JSON",
    )
    parser.add_argument(
        "--web-port",
        type=int,
        default=str_to_int(os.getenv("WEB_PORT"), 8080),
        help="Port for embedded web status dashboard (default: 8080 or WEB_PORT env var)",
    )
    parser.add_argument(
        "--web-host",
        default=os.getenv("WEB_HOST", "0.0.0.0"),
        help="Host address to bind embedded web dashboard (default: 0.0.0.0 or WEB_HOST env var)",
    )
    parser.add_argument(
        "--no-web",
        action="store_true",
        default=str_to_bool(os.getenv("NO_WEB", "false")),
        help="Disable embedded web status dashboard",
    )
    parser.add_argument(
        "-v",
        "--verbose",
        action="store_true",
        default=str_to_bool(os.getenv("VERBOSE", "false")),
        help="Enable debug logging",
    )
    return parser.parse_args()


async def main():
    args = parse_args()

    if args.verbose:
        logger.setLevel(logging.DEBUG)

    conn = init_db(args.db_path)
    downloader = BirdBuddyDownloader(args, conn)

    if args.info:
        info_data = await downloader.get_account_info()
        print_info_report(info_data, as_json=args.json)
        conn.close()
        return

    main_task = asyncio.current_task()

    def signal_handler(signum, frame):
        logger.info(
            "\n[!] Ctrl-C / Stop signal received! Stopping downloader immediately..."
        )
        downloader.request_stop()
        if main_task and not main_task.done():
            main_task.cancel()

    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)

    # Start embedded web dashboard server if enabled
    runner = None
    if not args.no_web:
        try:
            app = await create_web_app(downloader)
            runner = web.AppRunner(app)
            await runner.setup()
            site = web.TCPSite(runner, host=args.web_host, port=args.web_port)
            await site.start()
            logger.info(
                f"Web status dashboard running at http://{args.web_host}:{args.web_port}/"
            )
        except Exception as e:
            logger.warning(
                f"Could not start web dashboard server on {args.web_host}:{args.web_port}: {e}"
            )

    try:
        mode_label = " (Dry-Run mode)" if args.dry_run else ""
        if args.interval <= 0:
            logger.info(
                f"Starting Bird Buddy Downloader (Single Run mode){mode_label}..."
            )
            await downloader.run_once()
            if not downloader.stop_requested:
                logger.info(f"Done{mode_label}!")
            # If web dashboard is active, keep running until stopped
            if not args.no_web and not downloader.stop_requested:
                logger.info(
                    "Web dashboard active. Keeping process alive (Press Ctrl-C to exit)..."
                )
                while not downloader.stop_requested:
                    try:
                        downloader.sync_trigger_event.clear()
                        await asyncio.wait_for(
                            downloader.sync_trigger_event.wait(), timeout=1.0
                        )
                        if not downloader.stop_requested:
                            logger.info("Manual sync triggered via web interface...")
                            await downloader.run_once()
                    except asyncio.TimeoutError:
                        pass
        else:
            logger.info(
                f"Starting Bird Buddy Downloader in Continuous Daemon mode (Interval: {args.interval}s){mode_label}..."
            )
            while not downloader.stop_requested:
                try:
                    await downloader.run_once()
                except (KeyboardInterrupt, asyncio.CancelledError):
                    logger.info("Run cycle interrupted by user.")
                    break
                except Exception as e:
                    logger.error(f"Error during download cycle: {e}")

                if downloader.stop_requested:
                    break

                downloader.next_sync_time = datetime.now(timezone.utc) + timedelta(
                    seconds=args.interval
                )
                downloader.sync_trigger_event.clear()
                logger.info(
                    f"Sleeping for {args.interval} seconds until next check (Next sync at {downloader.next_sync_time.strftime('%H:%M:%S')} UTC)..."
                )

                sleep_remaining = args.interval
                while sleep_remaining > 0 and not downloader.stop_requested:
                    try:
                        await asyncio.wait_for(
                            downloader.sync_trigger_event.wait(),
                            timeout=min(1.0, sleep_remaining),
                        )
                        if (
                            downloader.sync_trigger_event.is_set()
                            and not downloader.stop_requested
                        ):
                            logger.info("Immediate sync requested via web interface!")
                            downloader.sync_trigger_event.clear()
                            break
                    except asyncio.TimeoutError:
                        sleep_remaining -= 1.0

    except (KeyboardInterrupt, asyncio.CancelledError):
        logger.info("Downloader process stopped by user signal.")
    finally:
        if runner:
            logger.info("Shutting down web dashboard server...")
            await runner.cleanup()
        conn.close()
        logger.info("Database closed. Exiting.")


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except (KeyboardInterrupt, SystemExit):
        sys.exit(0)
