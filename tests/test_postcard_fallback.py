import argparse
import asyncio
import os
import shutil
import tempfile
import unittest
from unittest import mock

from birdbuddy.exceptions import GraphqlError
from birdbuddy.feed import Feed
from birdbuddy.sightings import PostcardSighting

import downloader
from downloader import BirdBuddyDownloader, init_db

BROKEN_POSTCARD = "pc-broken"

FEED = Feed(
    {
        "edges": [
            {
                "cursor": "cursor-joined",
                "node": {
                    "__typename": "FeedItemFeederMemberJoined",
                    "id": "joined",
                    "createdAt": "2026-10-15T19:30:00.000Z",
                },
            },
            {
                "cursor": "cursor-broken",
                "node": {
                    "__typename": "FeedItemNewPostcard",
                    "id": BROKEN_POSTCARD,
                    "createdAt": "2026-10-15T18:59:59.526Z",
                },
            },
        ],
        "pageInfo": {"hasNextPage": False, "endCursor": None},
    }
)

# What the feed returns for one item, keyed by the cursor before it.
ITEM_AFTER = {
    None: {"__typename": "FeedItemFeederMemberJoined"},
    "cursor-joined": {
        "__typename": "FeedItemNewPostcard",
        "id": BROKEN_POSTCARD,
        "feeder": {"__typename": "FeederForMember", "id": "f1", "name": "Feeder"},
        "medias": [
            {
                "__typename": "MediaImage",
                "id": "img-1",
                "createdAt": "2026-10-15T18:58:40.000Z",
                "contentUrl": "https://media.example/img-1.jpg",
            },
            {
                "__typename": "MediaVideo",
                "id": "vid-1",
                "createdAt": "2026-10-15T18:58:30.000Z",
                "contentUrl": "https://media.example/vid-1.mp4",
            },
        ],
    },
}


def postcard(postcard_id, created_at, medias):
    return {
        "__typename": "FeedItemNewPostcard",
        "id": postcard_id,
        "createdAt": created_at,
        "feeder": {"__typename": "FeederForMember", "id": "f1", "name": "Feeder"},
        "medias": medias,
    }


def video(media_id):
    return {
        "__typename": "MediaVideo",
        "id": media_id,
        "createdAt": "2026-10-15T18:58:30.000Z",
        "contentUrl": f"https://media.example/{media_id}.mp4",
    }


class FakeBirdBuddy:
    """Postcards not in `opened` fail to open; the feed lists every
    postcard's media item by item."""

    def __init__(self, feed=FEED, item_after=ITEM_AFTER, opened=None):
        self._feed = feed
        self._item_after = item_after
        self._opened = opened or {}

    async def feed(self, first=50, after=None):
        return self._feed

    async def sighting_from_postcard(self, postcard_id):
        if postcard_id in self._opened:
            return self._opened[postcard_id]
        raise GraphqlError(
            {"message": "Internal server error.", "extensions": {"code": "500"}}
        )

    async def _make_request(self, query, variables=None, **kwargs):
        node = self._item_after[variables["after"]]
        return {"me": {"feed": {"edges": [{"node": node}]}}}


# Downloads whose URL or destination contains one of these fail.
UNWRITABLE = ("unwritable", "Tufted Titmouse")


async def fake_download(url, dest_path, stop_checker=None):
    if any(s in url or s in dest_path for s in UNWRITABLE):
        raise PermissionError(f"cannot create {os.path.dirname(dest_path)}")
    os.makedirs(os.path.dirname(dest_path), exist_ok=True)
    with open(dest_path, "wb") as f:
        f.write(b"media")
    return True


class PostcardFallbackTest(unittest.TestCase):
    """A postcard that can't be opened still has its photos and video saved."""

    def setUp(self):
        self.tmp = tempfile.mkdtemp()
        self.conn = init_db(os.path.join(self.tmp, "db.sqlite"))
        self.media_dir = os.path.join(self.tmp, "media")
        args = argparse.Namespace(
            dry_run=False,
            full_sync=True,
            max_pages=0,
            feeder_filter=None,
            no_images=False,
            no_videos=False,
            no_detect=True,
            download_dir=self.media_dir,
            dir_template="{species_name}",
            filename_template="{media_id}.{ext}",
        )
        self.dl = BirdBuddyDownloader(args, self.conn)
        self.dl.bb = FakeBirdBuddy()

    def tearDown(self):
        self.conn.close()
        shutil.rmtree(self.tmp)

    def test_unopenable_postcard_media_downloaded_from_feed(self):
        with mock.patch.object(downloader, "download_file", fake_download):
            downloaded = asyncio.run(self.dl.sync_feed())

        self.assertEqual(downloaded, 2)
        folder = os.path.join(self.media_dir, "Unrecognized Species")
        self.assertEqual(sorted(os.listdir(folder)), ["img-1.jpg", "vid-1.mp4"])
        rows = dict(
            self.conn.execute("SELECT media_id, media_type FROM downloaded_media")
        )
        self.assertEqual(rows, {"img-1": "image", "vid-1": "video"})

    def test_failed_fallback_download_does_not_stop_the_sync(self):
        first = postcard("pc-a", "2026-10-15T19:00:00.000Z", [video("unwritable")])
        second = postcard("pc-b", "2026-10-15T18:00:00.000Z", [video("vid-b")])
        feed = Feed(
            {
                "edges": [
                    {"cursor": "cursor-a", "node": {**first, "medias": None}},
                    {"cursor": "cursor-b", "node": {**second, "medias": None}},
                ],
                "pageInfo": {"hasNextPage": False, "endCursor": None},
            }
        )
        self.dl.bb = FakeBirdBuddy(feed, {None: first, "cursor-a": second})

        with mock.patch.object(downloader, "download_file", fake_download):
            downloaded = asyncio.run(self.dl.sync_feed())

        self.assertEqual(downloaded, 1)
        folder = os.path.join(self.media_dir, "Unrecognized Species")
        self.assertEqual(os.listdir(folder), ["vid-b.mp4"])

    def test_opened_postcard_is_not_refiled_when_saving_fails(self):
        card = postcard("pc-open", "2026-10-15T19:00:00.000Z", [video("vid-t")])
        opened = PostcardSighting(
            {
                "feeder": {"id": "f1", "name": "Feeder"},
                "medias": [],
                "videoMedia": video("vid-t"),
                "sightingReport": {
                    "sightings": [
                        {
                            "id": "s1",
                            "__typename": "SightingRecognizedBird",
                            "species": {"name": "Tufted Titmouse"},
                        }
                    ]
                },
            }
        )
        feed = Feed(
            {
                "edges": [{"cursor": "c1", "node": {**card, "medias": None}}],
                "pageInfo": {"hasNextPage": False, "endCursor": None},
            }
        )
        self.dl.bb = FakeBirdBuddy(feed, {None: card}, opened={"pc-open": opened})

        with mock.patch.object(downloader, "download_file", fake_download):
            downloaded = asyncio.run(self.dl.sync_feed())

        # Its species folder failed; the video must not land elsewhere, or
        # the de-dup record would keep it there for good.
        self.assertEqual(downloaded, 0)
        self.assertEqual(
            os.listdir(self.media_dir) if os.path.isdir(self.media_dir) else [], []
        )
        self.assertEqual(
            self.conn.execute("SELECT COUNT(*) FROM downloaded_media").fetchone()[0], 0
        )


if __name__ == "__main__":
    unittest.main()
