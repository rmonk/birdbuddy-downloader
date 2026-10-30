import asyncio
import json
import os
import shutil
import tempfile
import unittest
from datetime import datetime, timezone
from types import SimpleNamespace

from aiohttp import web
from aiohttp.test_utils import make_mocked_request

import downloader
from downloader import get_filter_options, get_recent_sightings, init_db

UTC = timezone.utc


class SightingsFilterTest(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.mkdtemp()
        self.conn = init_db(os.path.join(self.tmp, "test.db"))
        # sighting, media, feeder, species, type, created_at, score, city
        rows = [
            (
                "s1",
                "m1",
                "Front Yard",
                "Robin",
                "image",
                "2026-10-20T10:00:00Z",
                0.9,
                "Boston",
            ),
            (
                "s1",
                "m2",
                "Front Yard",
                "Robin",
                "image",
                "2026-10-20T10:00:05Z",
                0.4,
                "Boston",
            ),
            (
                "s1",
                "m3",
                "Front Yard",
                "Robin",
                "video",
                "2026-10-20T10:00:10Z",
                None,
                "Boston",
            ),
            (
                "s2",
                "m4",
                "Back Yard",
                "Blue Jay",
                "image",
                "2026-10-22T08:30:00+00:00",
                0.3,
                "Denver",
            ),
            (
                "s3",
                "m5",
                "Back Yard",
                "",
                "image",
                "2026-10-25T12:00:00.123Z",
                0.6,
                None,
            ),
            ("s4", "m6", "", "Robin", "video", "2026-10-26T09:00:00Z", None, None),
        ]
        for sid, mid, feeder, species, mtype, created, score, city in rows:
            downloader.record_media_downloaded(
                self.conn,
                mid,
                "feeder-id",
                feeder,
                species,
                mtype,
                created,
                f"/downloads/{mid}.{'jpg' if mtype == 'image' else 'mp4'}",
                sighting_id=sid,
                bird_score=score,
                location_city=city,
            )
        with self.conn:
            self.conn.execute(
                "UPDATE downloaded_media SET is_deleted = 1 WHERE media_id = 'm2'"
            )

    def tearDown(self):
        self.conn.close()
        shutil.rmtree(self.tmp)

    def ids(self, **kwargs):
        return [s["sighting_id"] for s in get_recent_sightings(self.conn, **kwargs)]

    def test_no_filters_returns_everything_newest_first(self):
        self.assertEqual(self.ids(), ["s4", "s3", "s2", "s1"])

    def test_date_range_handles_mixed_timestamp_formats(self):
        start = datetime(2026, 10, 22, 8, 30, tzinfo=UTC)
        end = datetime(2026, 10, 25, 12, 0, tzinfo=UTC)
        # start is inclusive (+00:00 format), end exclusive (.123Z is just after it)
        self.assertEqual(self.ids(start=start, end=end), ["s2"])
        self.assertEqual(
            self.ids(start=start, end=datetime(2026, 10, 26, tzinfo=UTC)),
            ["s3", "s2"],
        )

    def test_start_replaces_days_cutoff(self):
        with self.conn:
            self.conn.execute(
                "UPDATE downloaded_media SET downloaded_at = '2020-01-01T00:00:00+00:00'"
            )
        self.assertEqual(self.ids(days=7), [])
        self.assertEqual(
            self.ids(days=7, start=datetime(2026, 10, 25, tzinfo=UTC)), ["s4", "s3"]
        )

    def test_feeder_and_species_use_display_names(self):
        self.assertEqual(self.ids(feeders=["Bird Buddy"]), ["s4"])
        self.assertEqual(self.ids(species=["Unknown Species"]), ["s3"])
        self.assertEqual(self.ids(species=["Robin", "Blue Jay"]), ["s4", "s2", "s1"])
        self.assertEqual(
            self.ids(feeders=["Front Yard", "Back Yard"], species=["Robin"]), ["s1"]
        )

    def test_media_type_and_removed(self):
        self.assertEqual(self.ids(media_types=["video"]), ["s4", "s1"])
        sightings = get_recent_sightings(
            self.conn, media_types=["image"], include_removed=False
        )
        s1 = next(s for s in sightings if s["sighting_id"] == "s1")
        self.assertEqual([m["media_id"] for m in s1["images"]], ["m1"])
        self.assertEqual(s1["videos"], [])

    def test_score_range_uses_best_image(self):
        self.assertEqual(self.ids(min_score=0.5), ["s3", "s1"])
        self.assertEqual(self.ids(max_score=0.5), ["s2"])
        self.assertEqual(self.ids(min_score=0.5, max_score=0.7), ["s3"])
        sightings = get_recent_sightings(self.conn)
        self.assertEqual(
            {s["sighting_id"]: s["best_score"] for s in sightings},
            {"s1": 0.9, "s2": 0.3, "s3": 0.6, "s4": None},
        )

    def test_score_filter_with_videos_uses_photo_scores(self):
        sightings = get_recent_sightings(
            self.conn, media_types=["video"], min_score=0.5
        )
        self.assertEqual([s["sighting_id"] for s in sightings], ["s1"])
        s1 = sightings[0]
        self.assertEqual(s1["best_score"], 0.9)
        self.assertEqual(s1["images"], [])
        self.assertEqual([m["media_id"] for m in s1["videos"]], ["m3"])
        self.assertEqual(s1["total_items"], 1)

    def test_text_search(self):
        self.assertEqual(self.ids(q="denver"), ["s2"])
        self.assertEqual(self.ids(q="  ROBIN "), ["s4", "s1"])
        self.assertEqual(self.ids(q="m5.jpg"), ["s3"])
        self.assertEqual(self.ids(q="back", species=["Blue Jay"]), ["s2"])

    def test_filter_options(self):
        opts = get_filter_options(self.conn)
        self.assertEqual(opts["feeders"], ["Back Yard", "Bird Buddy", "Front Yard"])
        self.assertEqual(opts["species"], ["Blue Jay", "Robin", "Unknown Species"])
        self.assertEqual(opts["date_min"], "2026-10-20T10:00:00+00:00")
        self.assertEqual(opts["date_max"], "2026-10-26T09:00:00+00:00")

    def call_api(self, query: str):
        app = web.Application()
        app["downloader"] = SimpleNamespace(
            conn=self.conn, args=SimpleNamespace(min_bird_confidence=0.25)
        )
        request = make_mocked_request("GET", f"/api/sightings?{query}", app=app)
        resp = asyncio.run(downloader.handle_api_sightings(request))
        return resp.status, json.loads(resp.body)

    def test_api_parses_repeated_and_percent_params(self):
        status, data = self.call_api(
            "days=0&per_page=0&feeder=Front+Yard&feeder=Back+Yard&min_score=50"
        )
        self.assertEqual(status, 200)
        self.assertEqual([s["sighting_id"] for s in data["sightings"]], ["s3", "s1"])
        status, data = self.call_api(
            "days=0&from=2026-10-22T00:00:00.000Z&to=2026-10-23T00:00:00.000Z"
        )
        self.assertEqual([s["sighting_id"] for s in data["sightings"]], ["s2"])

    def test_api_rejects_bad_params(self):
        for query in ("min_score=150", "max_score=abc", "from=yesterday", "type=gif"):
            status, _ = self.call_api(query)
            self.assertEqual(status, 400, query)


if __name__ == "__main__":
    unittest.main()
