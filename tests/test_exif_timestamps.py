import os
import sys
import tempfile
import time
import unittest
from unittest import mock
from datetime import datetime, timezone

import piexif
from PIL import Image

import downloader
from downloader import apply_timestamps_and_exif

# Bird Buddy reports capture times in UTC: 2026-07-21T18:29:44Z.
CAPTURED_UTC = datetime(2026, 7, 21, 18, 29, 44, tzinfo=timezone.utc)


def read_capture_time(path):
    exif = piexif.load(path)["Exif"]
    return (
        exif[piexif.ExifIFD.DateTimeOriginal].decode(),
        exif.get(piexif.ExifIFD.OffsetTimeOriginal, b"").decode(),
    )


class ExifTimestampTest(unittest.TestCase):
    """The capture time is written in the local timezone (TZ), with its offset."""

    def setUp(self):
        fd, self.path = tempfile.mkstemp(suffix=".jpg")
        os.close(fd)
        Image.new("RGB", (8, 8)).save(self.path, "JPEG")
        self.saved_tz = os.environ.get("TZ")

    def tearDown(self):
        os.remove(self.path)
        if self.saved_tz is None:
            os.environ.pop("TZ", None)
        else:
            os.environ["TZ"] = self.saved_tz
        time.tzset()

    def use_tz(self, name):
        os.environ["TZ"] = name
        time.tzset()

    def test_capture_time_is_local_wall_time_with_offset(self):
        self.use_tz("America/New_York")

        apply_timestamps_and_exif(self.path, CAPTURED_UTC, is_image=True)

        self.assertEqual(
            read_capture_time(self.path), ("2026:07:21 14:29:44", "-04:00")
        )

    def test_half_hour_offset_east_of_utc(self):
        self.use_tz("Asia/Kolkata")

        apply_timestamps_and_exif(self.path, CAPTURED_UTC, is_image=True)

        self.assertEqual(
            read_capture_time(self.path), ("2026:07:21 23:59:44", "+05:30")
        )

    def test_utc_when_no_timezone_is_configured(self):
        self.use_tz("UTC")

        apply_timestamps_and_exif(self.path, CAPTURED_UTC, is_image=True)

        self.assertEqual(
            read_capture_time(self.path), ("2026:07:21 18:29:44", "+00:00")
        )

    def test_timezone_from_env_file_applies(self):
        # The process starts in UTC; TZ only arrives through the .env file.
        self.use_tz("UTC")
        os.environ.pop("TZ")
        fd, env_file = tempfile.mkstemp(suffix=".env")
        with os.fdopen(fd, "w") as f:
            f.write("TZ=America/New_York\n")
        self.addCleanup(os.remove, env_file)

        with mock.patch.object(sys, "argv", ["downloader.py", "--env-file", env_file]):
            downloader.parse_args()
        apply_timestamps_and_exif(self.path, CAPTURED_UTC, is_image=True)

        self.assertEqual(
            read_capture_time(self.path), ("2026:07:21 14:29:44", "-04:00")
        )

    def test_mtime_is_the_capture_instant(self):
        self.use_tz("America/New_York")

        apply_timestamps_and_exif(self.path, CAPTURED_UTC, is_image=True)

        self.assertEqual(os.path.getmtime(self.path), CAPTURED_UTC.timestamp())


if __name__ == "__main__":
    unittest.main()
