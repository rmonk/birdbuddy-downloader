import asyncio
import itertools
import os
import shutil
import stat
import sys
import tempfile
import unittest
from unittest import mock

from birdbuddy.exceptions import AuthenticationFailedError

import downloader


class FakeBirdBuddy:
    """Stands in for pybirdbuddy's client at the API seam.

    Mirrors its auth contract: a refresh token skips the password sign-in and
    signs in as whichever account it belongs to, every refresh rotates the
    token before the feeder data is fetched, and a rejected token raises.
    """

    password_sign_ins = 0
    tokens: dict[str, str] = {}  # refresh token -> account email
    fail_after_rotation = False
    cancel_after_rotation = False
    unreachable_tokens: set[str] = set()  # network fails before the exchange
    cancelled_tokens: set[str] = set()  # cancelled before the exchange
    _ids = itertools.count(1)

    def __init__(self, email=None, password=None, /, refresh_token=None):
        self._email = email
        self._password = password
        self._refresh_token = refresh_token
        self.feeders = {}
        self.user = None

    @classmethod
    def reset(cls):
        cls.password_sign_ins = 0
        cls.tokens = {}
        cls.fail_after_rotation = False
        cls.cancel_after_rotation = False
        cls.unreachable_tokens = set()
        cls.cancelled_tokens = set()

    @classmethod
    def issue(cls, account="me@example.com"):
        token = f"refresh-{next(cls._ids)}"
        cls.tokens[token] = account
        return token

    async def refresh(self):
        if self._refresh_token in self.cancelled_tokens:
            raise asyncio.CancelledError()
        if self._refresh_token in self.unreachable_tokens:
            raise TimeoutError("token exchange timed out")
        if self._refresh_token:
            account = self.tokens.pop(self._refresh_token, None)
            if account is None:
                self._refresh_token = None
                raise AuthenticationFailedError("refresh token rejected")
            self._refresh_token = self.issue(account)
        elif self._email and self._password:
            FakeBirdBuddy.password_sign_ins += 1
            account = self._email
            self._refresh_token = self.issue(account)
        else:
            raise AuthenticationFailedError("email and password are required")
        if self.cancel_after_rotation:
            raise asyncio.CancelledError()
        if self.fail_after_rotation:
            raise TimeoutError("feeder query timed out")
        self.user = {"email": account}
        return True


class SessionPersistenceTest(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.mkdtemp()
        self.db_path = os.path.join(self.tmp, "data", "birdbuddy_downloader.db")
        self.session_path = os.path.join(self.tmp, "data", "session.json")
        FakeBirdBuddy.reset()
        patcher = mock.patch.object(downloader, "BirdBuddy", FakeBirdBuddy)
        patcher.start()
        self.addCleanup(patcher.stop)
        env = mock.patch.dict(os.environ, {}, clear=False)
        env.start()
        self.addCleanup(env.stop)
        for var in ("USERNAME", "PASSWORD", "REFRESH_TOKEN", "SESSION_PATH"):
            os.environ.pop(var, None)

    def tearDown(self):
        shutil.rmtree(self.tmp)

    def start_downloader(self, *cli):
        """Build a downloader the way main() does, as a fresh process would."""
        argv = ["downloader.py", "--env-file", os.devnull, "--db-path", self.db_path]
        with mock.patch.object(sys, "argv", argv + list(cli)):
            args = downloader.parse_args()
        conn = downloader.init_db(args.db_path)
        self.addCleanup(conn.close)
        return downloader.BirdBuddyDownloader(args, conn)

    def authenticate(self, dl):
        return asyncio.run(dl.authenticate())

    def test_restart_reuses_saved_session_instead_of_password(self):
        creds = ("--username", "me@example.com", "--password", "secret")
        self.assertTrue(self.authenticate(self.start_downloader(*creds)))
        self.assertTrue(self.authenticate(self.start_downloader(*creds)))

        self.assertEqual(FakeBirdBuddy.password_sign_ins, 1)

    def test_rejected_saved_session_falls_back_to_password(self):
        creds = ("--username", "me@example.com", "--password", "secret")
        self.assertTrue(self.authenticate(self.start_downloader(*creds)))
        FakeBirdBuddy.tokens.clear()  # server revoked the session

        self.assertTrue(self.authenticate(self.start_downloader(*creds)))
        self.assertEqual(FakeBirdBuddy.password_sign_ins, 2)

    def test_refresh_token_alone_signs_in_without_credentials(self):
        seed = FakeBirdBuddy.issue()

        dl = self.start_downloader("--refresh-token", seed)

        self.assertTrue(self.authenticate(dl))
        self.assertEqual(FakeBirdBuddy.password_sign_ins, 0)

    def test_seed_token_is_only_needed_once(self):
        seed = FakeBirdBuddy.issue()
        self.assertTrue(
            self.authenticate(self.start_downloader("--refresh-token", seed))
        )

        # The seed was rotated away; the restart must use the saved session.
        self.assertTrue(
            self.authenticate(self.start_downloader("--refresh-token", seed))
        )

    def test_token_rotated_before_a_network_error_is_kept(self):
        seed = FakeBirdBuddy.issue()
        FakeBirdBuddy.fail_after_rotation = True
        self.assertFalse(
            self.authenticate(self.start_downloader("--refresh-token", seed))
        )

        # Connectivity is back; the seed was rotated away, so only the
        # session file can sign this restart in.
        FakeBirdBuddy.fail_after_rotation = False
        self.assertTrue(self.authenticate(self.start_downloader()))

    def test_failed_token_only_resume_keeps_the_account_binding(self):
        self.authenticate(
            self.start_downloader("--username", "a@example.com", "--password", "pw")
        )
        FakeBirdBuddy.fail_after_rotation = True
        self.assertFalse(self.authenticate(self.start_downloader()))

        FakeBirdBuddy.fail_after_rotation = False
        dl = self.start_downloader("--username", "b@example.com", "--password", "pw")

        self.assertTrue(self.authenticate(dl))
        self.assertEqual(dl.bb.user["email"], "b@example.com")

    def test_stale_seed_does_not_replace_a_rotated_session(self):
        seed = FakeBirdBuddy.issue()
        self.assertTrue(
            self.authenticate(self.start_downloader("--refresh-token", seed))
        )
        # Next start: the saved session rotates and then times out, and the
        # long-stale seed cannot even reach the server.
        FakeBirdBuddy.fail_after_rotation = True
        FakeBirdBuddy.unreachable_tokens = {seed}
        self.assertFalse(
            self.authenticate(self.start_downloader("--refresh-token", seed))
        )

        FakeBirdBuddy.fail_after_rotation = False
        FakeBirdBuddy.unreachable_tokens = set()
        self.assertTrue(
            self.authenticate(self.start_downloader("--refresh-token", seed))
        )

    def test_token_rotated_before_cancellation_is_kept(self):
        seed = FakeBirdBuddy.issue()
        FakeBirdBuddy.cancel_after_rotation = True
        with self.assertRaises(asyncio.CancelledError):
            self.authenticate(self.start_downloader("--refresh-token", seed))

        FakeBirdBuddy.cancel_after_rotation = False
        self.assertTrue(self.authenticate(self.start_downloader()))

    def test_session_saved_without_an_account_is_checked_after_sign_in(self):
        seed = FakeBirdBuddy.issue("a@example.com")
        FakeBirdBuddy.fail_after_rotation = True
        self.assertFalse(
            self.authenticate(self.start_downloader("--refresh-token", seed))
        )

        FakeBirdBuddy.fail_after_rotation = False
        dl = self.start_downloader("--username", "b@example.com", "--password", "pw")

        self.assertTrue(self.authenticate(dl))
        self.assertEqual(dl.bb.user["email"], "b@example.com")

    def test_cancelled_sync_does_not_save_a_stale_seed(self):
        seed = FakeBirdBuddy.issue()
        self.assertTrue(
            self.authenticate(self.start_downloader("--refresh-token", seed))
        )
        # A sync cycle: the saved session rotates and then times out, and the
        # run is cancelled while the stale seed is being tried.
        FakeBirdBuddy.fail_after_rotation = True
        FakeBirdBuddy.cancelled_tokens = {seed}
        dl = self.start_downloader("--refresh-token", seed)
        with self.assertRaises(asyncio.CancelledError):
            asyncio.run(dl.run_once())

        FakeBirdBuddy.fail_after_rotation = False
        FakeBirdBuddy.cancelled_tokens = set()
        self.assertTrue(
            self.authenticate(self.start_downloader("--refresh-token", seed))
        )

    def test_mismatched_resume_keeps_the_rotated_token_for_its_account(self):
        seed = FakeBirdBuddy.issue("a@example.com")
        FakeBirdBuddy.fail_after_rotation = True
        self.authenticate(self.start_downloader("--refresh-token", seed))
        FakeBirdBuddy.fail_after_rotation = False

        # USERNAME names another account and there is no password to fall back on.
        self.assertFalse(
            self.authenticate(self.start_downloader("--username", "b@example.com"))
        )

        # With USERNAME corrected, the session for a is still usable.
        dl = self.start_downloader("--username", "a@example.com")
        self.assertTrue(self.authenticate(dl))
        self.assertEqual(dl.bb.user["email"], "a@example.com")

    def test_unverified_seed_is_not_bound_to_the_configured_account(self):
        seed = FakeBirdBuddy.issue("a@example.com")
        FakeBirdBuddy.fail_after_rotation = True
        self.authenticate(
            self.start_downloader(
                "--username", "b@example.com", "--refresh-token", seed
            )
        )
        FakeBirdBuddy.fail_after_rotation = False

        dl = self.start_downloader("--username", "a@example.com")
        self.assertTrue(self.authenticate(dl))
        self.assertEqual(dl.bb.user["email"], "a@example.com")

    def test_saved_session_for_another_account_is_not_used(self):
        self.authenticate(
            self.start_downloader("--username", "a@example.com", "--password", "pw")
        )

        dl = self.start_downloader("--username", "b@example.com", "--password", "pw")

        self.assertTrue(self.authenticate(dl))
        self.assertEqual(dl.bb.user["email"], "b@example.com")

    def test_saved_session_matches_account_case_insensitively(self):
        self.authenticate(
            self.start_downloader("--username", "Me@Example.com", "--password", "pw")
        )

        self.authenticate(
            self.start_downloader("--username", "me@example.com", "--password", "pw")
        )

        self.assertEqual(FakeBirdBuddy.password_sign_ins, 1)

    def test_no_credentials_and_no_session_fails(self):
        self.assertFalse(self.authenticate(self.start_downloader()))

    def test_session_file_is_private(self):
        creds = ("--username", "me@example.com", "--password", "secret")
        self.authenticate(self.start_downloader(*creds))

        mode = stat.S_IMODE(os.stat(self.session_path).st_mode)
        self.assertEqual(mode, 0o600)

    def test_session_path_can_be_overridden(self):
        custom = os.path.join(self.tmp, "elsewhere", "bb-session.json")
        creds = ("--username", "me@example.com", "--password", "secret")

        self.authenticate(self.start_downloader(*creds, "--session-path", custom))

        self.assertTrue(os.path.exists(custom))
        self.assertFalse(os.path.exists(self.session_path))


if __name__ == "__main__":
    unittest.main()
