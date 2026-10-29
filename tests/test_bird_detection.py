import os
import tempfile
import unittest
from unittest import mock

import cv2
import numpy as np

import downloader
from downloader import YOLO_PAD_VALUE, detect_birds, letterbox_blob

PAD = YOLO_PAD_VALUE / 255.0
# BGR orange; the blob is RGB scaled to 0..1.
COLOR_BGR = (0, 128, 255)
COLOR_RGB = np.array([255, 128, 0]) / 255.0


def solid(h, w):
    return np.full((h, w, 3), COLOR_BGR, dtype=np.uint8)


class LetterboxTest(unittest.TestCase):
    def test_portrait_is_padded_left_and_right_not_stretched(self):
        blob = letterbox_blob(solid(2592, 1944))  # Bird Buddy portrait photo

        self.assertEqual(blob.shape, (1, 3, 640, 640))
        # 1944 * 640 / 2592 = 480 wide, centered: 80 px of padding each side.
        np.testing.assert_allclose(blob[0, :, :, :80], PAD, atol=1e-6)
        np.testing.assert_allclose(blob[0, :, :, 560:], PAD, atol=1e-6)
        content = blob[0, :, :, 80:560]
        for channel, value in enumerate(COLOR_RGB):
            np.testing.assert_allclose(content[channel], value, atol=1 / 255)

    def test_landscape_is_padded_top_and_bottom(self):
        blob = letterbox_blob(solid(480, 640))

        np.testing.assert_allclose(blob[0, :, :80, :], PAD, atol=1e-6)
        np.testing.assert_allclose(blob[0, :, 560:, :], PAD, atol=1e-6)
        np.testing.assert_allclose(blob[0, 0, 80:560, :], COLOR_RGB[0], atol=1 / 255)


class FakeNet:
    def __init__(self, bird_scores):
        self.bird_scores = bird_scores
        self.blob = None

    def setInput(self, blob):
        self.blob = blob

    def forward(self):
        out = np.zeros((1, 84, 8400), dtype=np.float32)
        out[0, 4 + 14, : len(self.bird_scores)] = self.bird_scores
        return out


class DetectBirdsTest(unittest.TestCase):
    def setUp(self):
        fd, self.path = tempfile.mkstemp(suffix=".jpg")
        os.close(fd)
        cv2.imwrite(self.path, solid(2592, 1944))

    def tearDown(self):
        os.remove(self.path)

    def test_scores_bird_class_on_letterboxed_input(self):
        net = FakeNet([0.1, 0.9, 0.3])
        with mock.patch.object(downloader, "get_bird_detector_net", return_value=net):
            score, count = detect_birds(self.path, min_confidence=0.25)

        self.assertAlmostEqual(score, 0.9, places=5)
        self.assertEqual(count, 2)
        self.assertEqual(net.blob.shape, (1, 3, 640, 640))
        np.testing.assert_allclose(net.blob[0, :, :, :80], PAD, atol=1e-6)


if __name__ == "__main__":
    unittest.main()
