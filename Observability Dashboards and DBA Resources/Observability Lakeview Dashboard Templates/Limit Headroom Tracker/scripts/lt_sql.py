"""Shared Statement Execution helper for limit-tracking scripts (submit, poll, cancel)."""
import time

from databricks.sdk.service.sql import StatementParameterListItem, StatementState

POLL_TIMEOUT_S = 1800
POLL_RETRIES = 5  # consecutive transient poll failures tolerated before giving up


class StatementFailed(RuntimeError):
    pass


class StatementAborted(RuntimeError):
    """Raised after cancelling a statement that was still running; message names the statement id."""


def load_sql(path):
    """Read a file as one statement (each sql/ file is a single BEGIN ... END block)."""
    return open(path).read().strip().rstrip(";").strip()


def execute(w, sql, warehouse_id, params, timeout_s=POLL_TIMEOUT_S, sleep=time.sleep):
    """Run `sql` with named string `params`. Cancels the statement if anything goes wrong while it
    may still be running (deadline, repeated poll errors, Ctrl-C). Raises StatementFailed on a
    failed statement; returns the final response on success."""
    r = w.statement_execution.execute_statement(
        statement=sql,
        warehouse_id=warehouse_id,
        parameters=[StatementParameterListItem(name=k, value=v) for k, v in params.items()],
        wait_timeout="0s",  # get the statement_id back before any waiting
    )
    sid, deadline, failures = r.statement_id, time.monotonic() + timeout_s, 0
    try:
        while r.status.state in (StatementState.PENDING, StatementState.RUNNING):
            if time.monotonic() > deadline:
                raise TimeoutError(f"no result after {timeout_s}s")
            sleep(2)
            try:
                r = w.statement_execution.get_statement(sid)
                failures = 0
            except Exception:
                failures += 1
                if failures >= POLL_RETRIES:
                    raise
    except BaseException as e:
        try:
            w.statement_execution.cancel_execution(sid)
            note = "cancelled"
        except Exception as ce:  # still report the original failure and the statement id
            note = f"cancel FAILED ({type(ce).__name__}); it may still be running"
        raise StatementAborted(f"statement {sid} {note}: {type(e).__name__}: {e}") from e
    if r.status.state != StatementState.SUCCEEDED:
        msg = r.status.error.message if r.status.error else str(r.status.state)
        raise StatementFailed(msg)
    return r
