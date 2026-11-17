"""Unit tests for scripts/lt_sql.execute with a fake Statement Execution client."""
import os
import sys
import unittest
from types import SimpleNamespace as NS

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "scripts"))
from databricks.sdk.service.sql import StatementState as S  # noqa: E402
from lt_sql import StatementAborted, StatementFailed, execute  # noqa: E402


def resp(state, msg=None):
    return NS(statement_id="s1", status=NS(state=state, error=NS(message=msg) if msg else None), result=None)


class FakeExec:
    def __init__(self, polls):
        self.polls, self.cancelled = list(polls), False

    def execute_statement(self, **kw):
        return resp(S.PENDING)

    def get_statement(self, sid):
        p = self.polls.pop(0)
        if isinstance(p, Exception):
            raise p
        return p

    def cancel_execution(self, sid):
        self.cancelled = True


def run(polls, **kw):
    fake = FakeExec(polls)
    w = NS(statement_execution=fake)
    return fake, (lambda: execute(w, "SELECT 1", "wh", {"catalog": "c"}, sleep=lambda s: None, **kw))


class ExecuteTest(unittest.TestCase):
    def test_transient_poll_errors_are_retried(self):
        fake, go = run([OSError("blip"), OSError("blip"), resp(S.SUCCEEDED)])
        self.assertEqual(go().status.state, S.SUCCEEDED)
        self.assertFalse(fake.cancelled)

    def test_repeated_poll_errors_cancel(self):
        fake, go = run([OSError("down")] * 5)
        with self.assertRaisesRegex(StatementAborted, "statement s1 cancelled"):
            go()
        self.assertTrue(fake.cancelled)

    def test_failed_statement_raises_with_message(self):
        fake, go = run([resp(S.FAILED, "boom")])
        with self.assertRaisesRegex(StatementFailed, "boom"):
            go()
        self.assertFalse(fake.cancelled)

    def test_failed_cancel_still_reports_statement(self):
        fake, go = run([OSError("down")] * 5)
        def boom(sid):
            raise OSError("cancel failed")
        fake.cancel_execution = boom
        with self.assertRaisesRegex(StatementAborted, "s1 cancel FAILED"):
            go()

    def test_deadline_cancels(self):
        fake, go = run([resp(S.RUNNING)] * 50, timeout_s=-1)
        with self.assertRaisesRegex(StatementAborted, "TimeoutError"):
            go()
        self.assertTrue(fake.cancelled)


if __name__ == "__main__":
    unittest.main()
