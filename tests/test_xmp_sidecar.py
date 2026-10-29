import os
import shutil
import sqlite3
import tempfile
import time
import unittest
import xml.etree.ElementTree as ET
from datetime import datetime, timedelta, timezone

import downloader
from downloader import (
    build_xmp_sidecar,
    cleanup_old_db_records,
    feeder_location,
    init_db,
    record_media_downloaded,
    restore_media,
    soft_delete_media,
    update_media_location,
    write_xmp_sidecar,
    xmp_sidecar_path,
)

CAPTURED_UTC = datetime(2026, 7, 21, 18, 29, 44, tzinfo=timezone.utc)

NS = {
    "rdf": "http://www.w3.org/1999/02/22-rdf-syntax-ns#",
    "dc": "http://purl.org/dc/elements/1.1/",
    "exif": "http://ns.adobe.com/exif/1.0/",
    "photoshop": "http://ns.adobe.com/photoshop/1.0/",
    "digiKam": "http://www.digikam.org/ns/1.0/",
    "lr": "http://ns.adobe.com/lightroom/1.0/",
    "bbd": downloader.XMP_BBD_NS,
}


def parse_xmp(xmp: str) -> ET.Element:
    root = ET.fromstring(xmp.encode("utf-8"))
    return root.find("rdf:RDF/rdf:Description", NS)


def items(desc: ET.Element, path: str) -> list[str]:
    return [li.text for li in desc.findall(path + "/rdf:li", NS)]


class XmpSidecarContentTest(unittest.TestCase):
    def setUp(self):
        self.saved_tz = os.environ.get("TZ")
        os.environ["TZ"] = "America/New_York"
        time.tzset()

    def tearDown(self):
        if self.saved_tz is None:
            os.environ.pop("TZ", None)
        else:
            os.environ["TZ"] = self.saved_tz
        time.tzset()

    def test_full_metadata(self):
        desc = parse_xmp(
            build_xmp_sidecar(
                media_id="m-1",
                dt=CAPTURED_UTC,
                species_name="House Finch",
                feeder_name="Backyard",
                location_city="Springfield",
                location_country="United States",
                sighting_id="s-1",
            )
        )
        self.assertEqual(
            desc.find("exif:DateTimeOriginal", NS).text, "2026-07-21T14:29:44-04:00"
        )
        self.assertEqual(desc.find("photoshop:City", NS).text, "Springfield")
        self.assertEqual(desc.find("photoshop:Country", NS).text, "United States")
        self.assertEqual(
            items(desc, "dc:description/rdf:Alt"),
            [
                'House Finch at Bird Buddy feeder "Backyard" '
                "(Springfield, United States), 2026-07-21 14:29 EDT."
            ],
        )
        self.assertEqual(
            items(desc, "digiKam:TagsList/rdf:Seq"),
            [
                "Bird Buddy/Species/House Finch",
                "Bird Buddy/Feeder/Backyard",
                "Bird Buddy/City/Springfield",
            ],
        )
        self.assertEqual(
            items(desc, "lr:hierarchicalSubject/rdf:Bag")[0],
            "Bird Buddy|Species|House Finch",
        )
        self.assertEqual(
            items(desc, "dc:subject/rdf:Bag"),
            ["House Finch", "Backyard", "Springfield"],
        )
        self.assertEqual(desc.find("bbd:MediaID", NS).text, "m-1")
        self.assertEqual(desc.find("bbd:SightingID", NS).text, "s-1")

    def test_missing_location_and_special_characters(self):
        desc = parse_xmp(
            build_xmp_sidecar(
                media_id="m-2",
                dt=None,
                species_name="Unknown Species",
                feeder_name='Mom & Dad\'s "Front/Back" <yard>',
            )
        )
        self.assertIsNone(desc.find("photoshop:City", NS))
        self.assertIsNone(desc.find("exif:DateTimeOriginal", NS))
        self.assertIsNone(desc.find("bbd:SightingID", NS))
        self.assertIn(
            'Bird Buddy/Feeder/Mom & Dad\'s "Front-Back" <yard>',
            items(desc, "digiKam:TagsList/rdf:Seq"),
        )


class FeederLocationTest(unittest.TestCase):
    def test_owner_feeder_nested_location(self):
        self.assertEqual(
            feeder_location({"location": {"city": "Austin", "country": "US"}}),
            ("Austin", "US"),
        )

    def test_member_feeder_flat_location(self):
        self.assertEqual(
            feeder_location({"locationCity": "Leeds", "locationCountry": "UK"}),
            ("Leeds", "UK"),
        )

    def test_missing(self):
        self.assertEqual(feeder_location(None), (None, None))
        self.assertEqual(feeder_location({"location": None}), (None, None))


class DbAndTrashTest(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.mkdtemp()
        self.db_path = os.path.join(self.tmp, "test.db")
        self.download_dir = os.path.join(self.tmp, "downloads")
        os.makedirs(os.path.join(self.download_dir, "Feeder"))
        self.conn = init_db(self.db_path)
        self.media_path = os.path.join(self.download_dir, "Feeder", "a.jpg")
        with open(self.media_path, "wb") as f:
            f.write(b"jpeg")
        write_xmp_sidecar(
            self.media_path,
            build_xmp_sidecar("m-1", CAPTURED_UTC, "Robin", "Feeder"),
        )

    def tearDown(self):
        self.conn.close()
        shutil.rmtree(self.tmp)

    def record(self, created_at: str, **kwargs):
        record_media_downloaded(
            self.conn,
            "m-1",
            "f-1",
            "Feeder",
            "Robin",
            "image",
            created_at,
            self.media_path,
            **kwargs,
        )

    def location(self):
        return self.conn.execute(
            "SELECT location_city, location_country FROM downloaded_media"
        ).fetchone()

    def test_migration_adds_location_columns(self):
        legacy = os.path.join(self.tmp, "legacy.db")
        conn = sqlite3.connect(legacy)
        # The schema before the location columns were added.
        conn.execute(
            downloader.DB_TABLE_SCHEMA.replace(
                ",\n    location_city TEXT,\n    location_country TEXT", ""
            )
        )
        assert "location_city" not in {
            r[1] for r in conn.execute("PRAGMA table_info(downloaded_media)")
        }
        conn.close()
        conn = init_db(legacy)
        cols = {r[1] for r in conn.execute("PRAGMA table_info(downloaded_media)")}
        conn.close()
        self.assertIn("location_city", cols)
        self.assertIn("location_country", cols)

    def test_location_recorded_and_backfilled_without_overwriting(self):
        self.record(CAPTURED_UTC.isoformat(), location_city="Austin")
        update_media_location(self.conn, "m-1", "Dallas", "US")
        self.assertEqual(self.location(), ("Austin", "US"))

    def test_sidecar_follows_soft_delete_and_restore(self):
        self.record(CAPTURED_UTC.isoformat())
        sidecar = xmp_sidecar_path(self.media_path)

        self.assertTrue(soft_delete_media(self.conn, self.download_dir, "m-1"))
        (trash_path,) = self.conn.execute(
            "SELECT trash_path FROM downloaded_media"
        ).fetchone()
        self.assertFalse(os.path.exists(sidecar))
        self.assertTrue(os.path.exists(xmp_sidecar_path(trash_path)))

        self.assertTrue(restore_media(self.conn, self.download_dir, "m-1"))
        self.assertTrue(os.path.exists(sidecar))
        self.assertFalse(os.path.exists(xmp_sidecar_path(trash_path)))

    def test_cleanup_purges_trashed_sidecar(self):
        old = (datetime.now(timezone.utc) - timedelta(days=30)).isoformat()
        self.record(old)
        soft_delete_media(self.conn, self.download_dir, "m-1")
        (trash_path,) = self.conn.execute(
            "SELECT trash_path FROM downloaded_media"
        ).fetchone()

        self.assertEqual(cleanup_old_db_records(self.conn, 14), 1)
        self.assertFalse(os.path.exists(trash_path))
        self.assertFalse(os.path.exists(xmp_sidecar_path(trash_path)))


if __name__ == "__main__":
    unittest.main()
