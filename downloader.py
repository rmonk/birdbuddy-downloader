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
- Automatic 14-day database retention cleanup after each run (--db-retention-days)
- Supports single-run mode (cron-friendly) or continuous polling mode
- Provides detailed camera information, battery levels, species media counts, and event date ranges
- Supports --dry-run mode to list items that would be downloaded vs skipped
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
import requests
from dotenv import load_dotenv

import piexif
from PIL import Image

try:
    from birdbuddy.client import BirdBuddy
    from birdbuddy.feed import FeedNodeType
    from birdbuddy.queries import me as me_queries
except ImportError:
    print("Error: pybirdbuddy package not found. Please install requirements.txt first.")
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
"""


def init_db(db_path: str) -> sqlite3.Connection:
    """Initialize SQLite database for tracking downloaded media."""
    db_dir = os.path.dirname(os.path.abspath(db_path))
    if db_dir:
        os.makedirs(db_dir, exist_ok=True)
    conn = sqlite3.connect(db_path)
    with conn:
        conn.execute(DB_SCHEMA)
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
        cursor.executemany("DELETE FROM downloaded_media WHERE media_id = ?", [(m,) for m in to_delete])
        conn.commit()
    return len(to_delete)


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
    """Download file from URL atomically using temporary file."""
    os.makedirs(os.path.dirname(dest_path), exist_ok=True)
    temp_path = dest_path + ".tmp"

    try:
        async with aiohttp.ClientSession() as session:
            async with session.get(url, timeout=60) as resp:
                if resp.status != 200:
                    logger.error(f"Download failed with status {resp.status} for {url}")
                    return False
                with open(temp_path, "wb") as f:
                    while True:
                        if stop_checker and stop_checker():
                            logger.info("Download cancelled mid-stream due to stop request.")
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

    async def authenticate(self) -> bool:
        """Authenticate with Bird Buddy API."""
        if self.stop_requested:
            return False

        username = self.args.username or os.getenv("USERNAME")
        password = self.args.password or os.getenv("PASSWORD")

        if not username or not password:
            logger.error(
                "Bird Buddy credentials not specified. Set USERNAME & PASSWORD in .env or pass --username/--password."
            )
            return False

        logger.info(f"Authenticating with Bird Buddy account: {username}")
        self.bb = BirdBuddy(username, password)
        try:
            success = await self.bb.refresh()
            if not success:
                logger.error("Bird Buddy authentication failed.")
                return False

            self.feeders_map = self.bb.feeders or {}
            logger.info(f"Successfully authenticated! Found {len(self.feeders_map)} connected camera feeder(s):")
            for fid, fdata in self.feeders_map.items():
                name = fdata.get("name", "Unnamed Feeder")
                owner = fdata.get("ownerName", "Unknown Owner")
                logger.info(f" - Feeder '{name}' (ID: {fid}, Owner: {owner})")
            return True
        except Exception as e:
            logger.error(f"Failed to authenticate: {e}")
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

        owner_name = owner_name or self.feeders_map.get(feeder_id, {}).get("ownerName", "Unknown Owner")
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

        already_downloaded = is_media_downloaded(self.conn, media_id) or os.path.exists(dest_path)

        if already_downloaded:
            if not is_media_downloaded(self.conn, media_id) and os.path.exists(dest_path) and not self.args.dry_run:
                record_media_downloaded(
                    self.conn, media_id, feeder_id, feeder_name, species_name, media_type, created_at_str, dest_path
                )
            logger.debug(f"Media {media_id} already downloaded. Skipping.")
            return "skipped"

        if self.args.dry_run:
            logger.info(
                f"[DRY-RUN] Would download [{media_type.upper()}] from '{feeder_name}' ({species_name}): {filename} -> {dest_path}"
            )
            return "would_download"

        logger.info(f"Downloading [{media_type.upper()}] from '{feeder_name}' ({species_name}): {filename}")
        success = await download_file(content_url, dest_path, stop_checker=lambda: self.stop_requested)
        if success and not self.stop_requested:
            if dt:
                apply_timestamps_and_exif(dest_path, dt, is_image=(media_type == "image"))
            record_media_downloaded(
                self.conn, media_id, feeder_id, feeder_name, species_name, media_type, created_at_str, dest_path
            )
            logger.info(f"Successfully downloaded and timestamped: {dest_path}")
            return "downloaded"
        return "filtered"

    async def sync_feed(self):
        """Sync media items from Bird Buddy feed across all cameras."""
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
                if feed_cutoff_dt and node_created_at and node_created_at < feed_cutoff_dt:
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
                        feeder_info = sighting.feeder or {}
                        feeder_id = feeder_info.get("id", "unknown_feeder")
                        feeder_name = feeder_info.get("name", "Unknown Feeder")
                        owner_name = feeder_info.get("ownerName") or self.feeders_map.get(feeder_id, {}).get("ownerName", "Unknown Owner")

                        sighting_id = None
                        if hasattr(sighting, "report") and sighting.report and sighting.report.sightings:
                            sighting_id = sighting.report.sightings[0].id

                        if self.args.feeder_filter and self.args.feeder_filter.lower() not in feeder_name.lower():
                            continue

                        species_name = extract_species_name(sighting)

                        # Process image media
                        for m in sighting.medias:
                            if self.stop_requested:
                                break
                            mid = m.id if hasattr(m, "id") else m.get("id")
                            created_at = (
                                m.created_at.isoformat()
                                if hasattr(m, "created_at") and m.created_at
                                else m.get("createdAt")
                            )
                            content_url = m.content_url if hasattr(m, "content_url") else m.get("contentUrl")
                            status = await self.process_media_item(
                                mid, "image", content_url, created_at, feeder_name, feeder_id, species_name, owner_name, postcard_id, sighting_id
                            )
                            if status == "downloaded":
                                total_downloaded += 1
                            elif status == "would_download":
                                total_would_download += 1
                            elif status == "skipped":
                                total_skipped += 1

                        # Process video media
                        for vm in sighting.video_media:
                            if self.stop_requested:
                                break
                            vmid = vm.id if hasattr(vm, "id") else vm.get("id")
                            created_at = (
                                vm.created_at.isoformat()
                                if hasattr(vm, "created_at") and vm.created_at
                                else vm.get("createdAt")
                            )
                            content_url = vm.content_url if hasattr(vm, "content_url") else vm.get("contentUrl")
                            status = await self.process_media_item(
                                vmid, "video", content_url, created_at, feeder_name, feeder_id, species_name, owner_name, postcard_id, sighting_id
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

                    species_info = node.get("species", {}) or node.get("collection", {}).get("species", {})
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
                            mid, mtype, content_url, created_at, feeder_name, feeder_id, species_name, "Unknown Owner", postcard_id
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
            logger.info(f"Feed sync stopped early. Media files downloaded before stop: {total_downloaded}")
        else:
            logger.info(f"Feed sync complete. Total new media files downloaded: {total_downloaded}")

    async def sync_collections(self):
        """Sync media items from saved user collections if available."""
        if self.stop_requested:
            return

        logger.info("Checking collections...")
        try:
            data = await self.bb._make_request(query=me_queries.COLLECTIONS)
            collections = data.get("me", {}).get("collections", [])
            if not collections or self.stop_requested:
                return

            logger.info(f"Found {len(collections)} collection(s). Syncing collection media...")
            total_downloaded = 0
            total_would_download = 0
            total_skipped = 0

            for c in collections:
                if self.stop_requested:
                    break
                cid = c.get("id")
                species = c.get("species", {}).get("name", "Unknown Species") if "species" in c else "Unknown Species"

                cursor = None
                while not self.stop_requested:
                    media_data = await self.bb._make_request(
                        query=me_queries.COLLECTIONS_MEDIA,
                        variables={"collectionId": cid, "first": 50, "after": cursor},
                    )
                    collection_obj = media_data.get("collection", {})
                    media_conn = collection_obj.get("media", {}) if collection_obj else {}
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

                        if self.args.feeder_filter and self.args.feeder_filter.lower() not in feeder_name.lower():
                            continue

                        status = await self.process_media_item(
                            mid, mtype, content_url, created_at, feeder_name, feeder_id, species, owner_name
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
                logger.info(f"Collections sync complete. New media downloaded: {total_downloaded}")
        except Exception as e:
            if not self.stop_requested:
                logger.debug(f"Collections sync skipped or encountered error: {e}")

    async def get_account_info(self) -> dict:
        """Gather detailed information about connected feeders, species breakdown, and event ranges."""
        if not self.bb and not await self.authenticate():
            return {}

        feeders_info = []
        feeder_events = {}

        cursor = self.conn.cursor()
        cursor.execute(
            """
            SELECT feeder_name,
                   MIN(created_at) as earliest,
                   MAX(created_at) as latest,
                   COUNT(*) as total_media
            FROM downloaded_media
            GROUP BY feeder_name
            """
        )
        for row in cursor.fetchall():
            feeder_events[row[0]] = {
                "earliest": row[1],
                "latest": row[2],
                "total_media": row[3],
            }

        for fid, f in self.feeders_map.items():
            fname = f.get("name", "Unnamed Feeder")
            events = feeder_events.get(fname, {"earliest": None, "latest": None, "total_media": 0})

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
        cursor.execute(
            """
            SELECT species_name,
                   SUM(CASE WHEN media_type = 'image' THEN 1 ELSE 0 END) as images,
                   SUM(CASE WHEN media_type = 'video' THEN 1 ELSE 0 END) as videos,
                   COUNT(*) as total
            FROM downloaded_media
            GROUP BY species_name
            ORDER BY total DESC
            """
        )
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
            "account_user": self.bb.user.get("email") if self.bb.user else None,
            "feeders": feeders_info,
            "species_summary": species_stats,
        }

    async def run_once(self):
        """Execute one download sync cycle and clean up old database records."""
        if not await self.authenticate():
            return
        await self.sync_feed()
        if not self.stop_requested:
            await self.sync_collections()

        # Database cleanup for entries older than db_retention_days (default 14 days)
        if not self.stop_requested and not self.args.dry_run:
            retention_days = getattr(self.args, "db_retention_days", 14)
            if retention_days > 0:
                num_cleaned = cleanup_old_db_records(self.conn, retention_days)
                if num_cleaned > 0:
                    logger.info(f"Database cleanup: removed {num_cleaned} record(s) older than {retention_days} days.")


def print_info_report(info: dict, as_json: bool = False):
    """Print clean human-readable or JSON info report."""
    if as_json:
        print(json.dumps(info, indent=2, default=str))
        return

    print("\n" + "=" * 80)
    print("                        BIRD BUDDY ACCOUNT INFORMATION                         ")
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
        bat_str = f"{bat.get('percentage')}%" if bat.get("percentage") is not None else "N/A"
        bat_charging = "Charging" if bat.get("charging") else "Not Charging"
        bat_state = bat.get("state") or "N/A"
        print(f"      Battery: {bat_str} ({bat_state}, {bat_charging})")

        print(f"      Food Level: {f.get('food_state') or 'N/A'}")
        sig = f.get("signal", {})
        sig_str = f"{sig.get('state')} ({sig.get('value_dbm')} dBm)" if sig.get("value_dbm") else "N/A"
        print(f"      Wi-Fi Signal: {sig_str}")
        print(f"      Temperature: {f.get('temperature')}°C" if f.get("temperature") is not None else "      Temperature: N/A")

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


def parse_args():
    parser = argparse.ArgumentParser(
        description="Automatic Bird Buddy media downloader with de-duplication, metadata timestamping, and feeder info."
    )
    parser.add_argument("--env-file", default=".env", help="Path to .env file containing credentials (default: .env)")
    parser.add_argument("--username", help="Bird Buddy account email (overrides .env)")
    parser.add_argument("--password", help="Bird Buddy account password (overrides .env)")
    parser.add_argument("--download-dir", default="./downloads", help="Directory to save downloaded media (default: ./downloads)")
    parser.add_argument("--db-path", default="./birdbuddy_downloader.db", help="SQLite DB path for de-duplication (default: ./birdbuddy_downloader.db)")
    parser.add_argument(
        "--dir-template",
        help="Template for output subdirectories relative to download-dir (default: '{feeder_name}/{species_name}'). "
             "Variables: feeder_name, species_name, owner_name, detection_id, postcard_id, sighting_id, year, month, day, hour, minute, second, date, iso_date, time, etc.",
    )
    parser.add_argument(
        "--filename-template",
        help="Template for output filenames (default: '{year}{month}{day}_{hour}{minute}{second}_{media_id_short}.{ext}'). "
             "Variables: year, month, day, hour, minute, second, date, time, media_id_short, detection_id_short, postcard_id_short, feeder_name, species_name, ext, etc.",
    )
    parser.add_argument(
        "--buffer-hours",
        type=float,
        default=2.0,
        help="Hours before latest downloaded detection to start fetching feed items (default: 2.0)",
    )
    parser.add_argument(
        "--db-retention-days",
        type=int,
        default=14,
        help="Days to retain records in database before cleanup (default: 14; set to 0 to disable)",
    )
    parser.add_argument(
        "--full-sync",
        action="store_true",
        help="Bypass latest detection cutoff and perform a full feed sync",
    )
    parser.add_argument("--interval", type=int, default=0, help="Interval in seconds for continuous polling mode (0 for single run)")
    parser.add_argument("--max-pages", type=int, default=0, help="Maximum feed pages to fetch (0 for unlimited)")
    parser.add_argument("--no-images", action="store_true", help="Skip downloading image files")
    parser.add_argument("--no-videos", action="store_true", help="Skip downloading video files")
    parser.add_argument("--feeder-filter", help="Filter downloads by feeder name (case-insensitive substring)")
    parser.add_argument("--dry-run", action="store_true", help="Perform a dry run without downloading files or modifying database")
    parser.add_argument("--info", action="store_true", help="Display feeder information, battery status, species media counts, and event date ranges")
    parser.add_argument("--json", action="store_true", help="Output --info as raw JSON")
    parser.add_argument("-v", "--verbose", action="store_true", help="Enable debug logging")
    return parser.parse_args()


async def main():
    args = parse_args()

    if args.verbose:
        logger.setLevel(logging.DEBUG)

    if os.path.exists(args.env_file):
        load_dotenv(args.env_file)

    conn = init_db(args.db_path)
    downloader = BirdBuddyDownloader(args, conn)

    if args.info:
        info_data = await downloader.get_account_info()
        print_info_report(info_data, as_json=args.json)
        conn.close()
        return

    main_task = asyncio.current_task()

    def signal_handler(signum, frame):
        logger.info("\n[!] Ctrl-C / Stop signal received! Stopping downloader immediately...")
        downloader.request_stop()
        if main_task and not main_task.done():
            main_task.cancel()

    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)

    try:
        if args.dry_run:
            logger.info("Starting Bird Buddy Downloader (Dry-Run mode)...")
            await downloader.run_once()
            if not downloader.stop_requested:
                logger.info("Dry-Run completed!")
        elif args.interval <= 0:
            logger.info("Starting Bird Buddy Downloader (Single Run mode)...")
            await downloader.run_once()
            if not downloader.stop_requested:
                logger.info("Done!")
        else:
            logger.info(f"Starting Bird Buddy Downloader in Continuous Daemon mode (Interval: {args.interval}s)...")
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
                logger.info(f"Sleeping for {args.interval} seconds until next check...")
                for _ in range(args.interval):
                    if downloader.stop_requested:
                        break
                    await asyncio.sleep(1)
    except (KeyboardInterrupt, asyncio.CancelledError):
        logger.info("Downloader process stopped by user signal.")
    finally:
        conn.close()
        logger.info("Database closed. Exiting.")


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except (KeyboardInterrupt, SystemExit):
        sys.exit(0)
