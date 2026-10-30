import os
import shutil
import tempfile
import unittest

from downloader import (
    init_db,
    record_media_downloaded,
    restore_media,
    soft_delete_media,
)


class SoftDeleteTrashPathTest(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.mkdtemp()
        self.download_dir = os.path.join(self.tmp, "downloads")
        os.makedirs(os.path.join(self.download_dir, "Feeder"))
        self.conn = init_db(os.path.join(self.tmp, "test.db"))

    def tearDown(self):
        self.conn.close()
        shutil.rmtree(self.tmp)

    def add_media(self, media_id: str) -> str:
        path = os.path.join(self.download_dir, "Feeder", "photo.jpg")
        with open(path, "wb") as f:
            f.write(b"jpeg")
        record_media_downloaded(
            self.conn,
            media_id,
            "fid",
            "Feeder",
            "Robin",
            "image",
            "2026-10-29T10:00:00Z",
            path,
        )
        return path

    def trash_path(self) -> str:
        return self.conn.execute("SELECT trash_path FROM downloaded_media").fetchone()[
            0
        ]

    def test_unsafe_media_id_stays_inside_trash(self):
        path = self.add_media("../../escape/id")
        self.assertTrue(
            soft_delete_media(self.conn, self.download_dir, "../../escape/id")
        )

        trash_dir = os.path.realpath(os.path.join(self.download_dir, ".trash"))
        trash_path = self.trash_path()
        self.assertEqual(os.path.dirname(trash_path), trash_dir)
        self.assertEqual(os.path.basename(trash_path), "______escape_id_photo.jpg")
        self.assertTrue(os.path.exists(trash_path))
        self.assertFalse(os.path.exists(path))
        self.assertEqual(os.listdir(self.tmp).count("escape"), 0)

    def test_unknown_media_id_moves_nothing(self):
        path = self.add_media("m-1")
        self.assertFalse(soft_delete_media(self.conn, self.download_dir, "m-2"))
        self.assertTrue(os.path.exists(path))
        self.assertFalse(os.path.exists(os.path.join(self.download_dir, ".trash")))

    def test_relative_download_dir_round_trips(self):
        path = self.add_media("m-1")
        cwd = os.getcwd()
        os.chdir(self.tmp)
        try:
            self.assertTrue(soft_delete_media(self.conn, "downloads", "m-1"))
            self.assertTrue(os.path.isabs(self.trash_path()))
            self.assertTrue(restore_media(self.conn, "downloads", "m-1"))
        finally:
            os.chdir(cwd)
        self.assertTrue(os.path.exists(path))


if __name__ == "__main__":
    unittest.main()
