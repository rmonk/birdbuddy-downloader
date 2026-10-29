import asyncio
import types
import unittest

from downloader import BirdBuddyDownloader, me_queries


def page(media_ids, end_cursor=None, has_next=False):
    return {
        "collection": {
            "media": {
                "edges": [
                    {
                        "node": {
                            "feederName": "Backyard",
                            "media": {
                                "id": mid,
                                "contentUrl": f"https://example.invalid/{mid}.jpg",
                                "createdAt": "2026-10-14T15:46:00Z",
                                "__typename": "MediaImage",
                            },
                        }
                    }
                    for mid in media_ids
                ],
                "pageInfo": {"hasNextPage": has_next, "endCursor": end_cursor},
            }
        }
    }


class FakeBirdBuddy:
    def __init__(self, collections, pages):
        self.collections = collections
        self.pages = pages  # {collection_id: [response or Exception, ...]}
        self.calls = []

    async def _make_request(self, query, variables=None):
        if query == me_queries.COLLECTIONS:
            if isinstance(self.collections, Exception):
                raise self.collections
            return {"me": {"collections": self.collections}}
        self.calls.append(dict(variables))
        result = self.pages[variables["collectionId"]].pop(0)
        if isinstance(result, Exception):
            raise result
        return result


class SyncCollectionsTest(unittest.TestCase):
    def make_downloader(self, bb):
        args = types.SimpleNamespace(dry_run=False, feeder_filter=None)
        dl = BirdBuddyDownloader.__new__(BirdBuddyDownloader)
        dl.args = args
        dl.bb = bb
        dl.stop_requested = False
        dl.processed = []

        async def process_media_item(mid, *rest, **kw):
            dl.processed.append(mid)
            return "downloaded"

        dl.process_media_item = process_media_item
        return dl

    def test_first_page_omits_after_and_later_pages_send_cursor(self):
        bb = FakeBirdBuddy(
            [{"id": "c1", "species": {"name": "Robin"}}],
            {"c1": [page(["m1"], "cur1", True), page(["m2"])]},
        )
        dl = self.make_downloader(bb)

        self.assertEqual(asyncio.run(dl.sync_collections()), 2)
        self.assertNotIn("after", bb.calls[0])
        self.assertEqual(bb.calls[1]["after"], "cur1")

    def test_repeated_cursor_stops_paging(self):
        bb = FakeBirdBuddy(
            [{"id": "c1"}],
            {"c1": [page(["m1"], "cur1", True), page(["m2"], "cur1", True)]},
        )
        dl = self.make_downloader(bb)

        self.assertEqual(asyncio.run(dl.sync_collections()), 2)
        self.assertEqual(len(bb.calls), 2)

    def test_failed_collection_is_logged_and_others_continue(self):
        bb = FakeBirdBuddy(
            [{"id": "c1", "species": None}, {"id": "c2"}],
            {"c1": [RuntimeError("boom")], "c2": [page(["m2"])]},
        )
        dl = self.make_downloader(bb)

        with self.assertLogs("birdbuddy-downloader", "WARNING") as logs:
            self.assertEqual(asyncio.run(dl.sync_collections()), 1)
        self.assertEqual(dl.processed, ["m2"])
        self.assertIn("'Unknown Species' (c1) sync failed: boom", logs.output[0])

    def test_collections_list_failure_is_logged_as_error(self):
        dl = self.make_downloader(FakeBirdBuddy(RuntimeError("down"), {}))

        with self.assertLogs("birdbuddy-downloader", "ERROR") as logs:
            self.assertEqual(asyncio.run(dl.sync_collections()), 0)
        self.assertIn("Could not fetch collections list: down", logs.output[0])


if __name__ == "__main__":
    unittest.main()
