"""End-to-end tests for the local job service."""

import re
import tempfile
import time
import unittest

from jobservice import JobService, State, UnknownCommand, UnknownJob


def wait_for(predicate, timeout=10.0, interval=0.02):
    """Poll until predicate() is truthy; returns the last value."""
    deadline = time.monotonic() + timeout
    value = predicate()
    while not value and time.monotonic() < deadline:
        time.sleep(interval)
        value = predicate()
    return value


def pid_alive(pid):
    """True if the process exists and is not a zombie/dead."""
    try:
        with open(f"/proc/{pid}/stat") as fh:
            state = fh.read().rsplit(")", 1)[1].split()[0]
    except (FileNotFoundError, ProcessLookupError):
        return False
    return state not in ("Z", "X", "x")


class ServiceTestCase(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.service = self.make_service()
        self.addCleanup(self.service.shutdown)

    def make_service(self, **overrides):
        kwargs = dict(max_concurrent=2, run_time_limit=10.0, output_limit=1 << 20)
        kwargs.update(overrides)
        return JobService(self.tmp.name, **kwargs)

    def wait_terminal(self, job_id, timeout=15.0):
        status = self.service.wait(job_id, timeout=timeout)
        self.assertTrue(status.terminal, f"job {job_id} still {status.state}")
        return status


class CommandAllowlistTest(ServiceTestCase):
    def test_unknown_command_rejected(self):
        with self.assertRaises(UnknownCommand):
            self.service.submit("rm -rf /")
        with self.assertRaises(UnknownCommand):
            self.service.submit("slow; echo pwned")  # no shell interpolation
        with self.assertRaises(UnknownCommand):
            self.service.submit("")

    def test_unknown_job_rejected(self):
        with self.assertRaises(UnknownJob):
            self.service.status("no-such-job")
        with self.assertRaises(UnknownJob):
            self.service.cancel("no-such-job")
        with self.assertRaises(UnknownJob):
            self.service.read("no-such-job", "stdout")


class ConcurrencyTest(ServiceTestCase):
    def test_at_most_two_running_and_queued_cancel(self):
        first = self.service.submit("slow")
        second = self.service.submit("slow")
        third = self.service.submit("slow")

        self.assertTrue(
            wait_for(
                lambda: self.service.status(first).state is State.RUNNING
                and self.service.status(second).state is State.RUNNING
            ),
            "first two jobs did not start",
        )
        # Only two slots: the third job must stay queued.
        self.assertIs(self.service.status(third).state, State.QUEUED)
        time.sleep(0.3)
        self.assertIs(self.service.status(third).state, State.QUEUED)

        # Cancelling a queued job works and is stable: it never runs.
        self.assertIs(self.service.cancel(third), State.CANCELED)
        self.assertIs(self.service.cancel(third), State.CANCELED)

        self.service.cancel(first)
        self.service.cancel(second)
        self.wait_terminal(first)
        self.wait_terminal(second)
        final = self.wait_terminal(third)
        self.assertIs(final.state, State.CANCELED)
        self.assertIsNone(final.started_at)
        self.assertEqual(final.stdout_bytes, 0)
        self.assertEqual(final.stderr_bytes, 0)
        # Still the same single result after the others finished.
        self.assertIs(self.service.cancel(third), State.CANCELED)


class CancelTest(ServiceTestCase):
    def test_cancel_running_job_is_idempotent(self):
        job = self.service.submit("slow")
        self.assertTrue(
            wait_for(lambda: self.service.status(job).state is State.RUNNING)
        )
        self.assertIs(self.service.cancel(job), State.CANCELED)
        self.assertIs(self.service.cancel(job), State.CANCELED)
        status = self.wait_terminal(job)
        self.assertIs(status.state, State.CANCELED)
        # Repeated cancels after finalization return the same terminal state.
        self.assertIs(self.service.cancel(job), State.CANCELED)
        self.assertIs(self.service.cancel(job), State.CANCELED)

    def test_exit_vs_cancel_race_has_single_result(self):
        # A command that exits 0 almost immediately, cancelled at the same
        # time: exactly one terminal state must win, and repeated cancels
        # must not change it.
        for _ in range(30):
            job = self.service.submit("exit-race")
            first = self.service.cancel(job)
            status = self.wait_terminal(job)
            self.assertIn(status.state, (State.SUCCEEDED, State.CANCELED))
            self.assertIs(self.service.cancel(job), status.state)
            self.assertIs(self.service.cancel(job), status.state)
            if status.state is State.CANCELED:
                self.assertIs(first, State.CANCELED)


class LimitTest(ServiceTestCase):
    def test_run_time_limit_kills_job(self):
        service = self.make_service(run_time_limit=0.5)
        self.addCleanup(service.shutdown)
        job = service.submit("slow")
        self.assertTrue(wait_for(lambda: service.status(job).state is State.RUNNING))
        status = service.wait(job, timeout=15.0)
        self.assertIs(status.state, State.TIMEOUT)
        self.assertLess(status.ended_at - status.started_at, 5.0)

    def test_output_limit_kills_job_and_keeps_saved_output(self):
        limit = 64 * 1024
        service = self.make_service(run_time_limit=60.0, output_limit=limit)
        self.addCleanup(service.shutdown)
        job = service.submit("spam-stderr")
        status = service.wait(job, timeout=30.0)
        self.assertIs(status.state, State.OUTPUT_LIMIT_EXCEEDED)
        total = status.stdout_bytes + status.stderr_bytes
        self.assertGreater(total, limit)
        self.assertGreater(status.stderr_bytes, 0)

        # Saved output is readable by cursor after the kill.
        result = service.read(job, "stderr", cursor=0, max_bytes=128)
        self.assertEqual(len(result.data), 128)
        self.assertEqual(result.next_cursor, 128)
        self.assertFalse(result.eof)
        self.assertTrue(result.data.startswith(b"error line 0"))
        tail = service.read(job, "stderr", cursor=status.stderr_bytes)
        self.assertEqual(tail.data, b"")
        self.assertTrue(tail.eof)


class ProcessGroupTest(ServiceTestCase):
    def test_cancel_kills_entire_process_group(self):
        job = self.service.submit("spawn-children")

        def child_pids():
            data = self.service.read(job, "stdout", cursor=0, max_bytes=1 << 16).data
            return [int(pid) for pid in re.findall(rb"child (\d+)", data)]

        pids = wait_for(lambda: len(child_pids()) == 3 and child_pids())
        self.assertTrue(pids, "children never reported their pids")
        for pid in pids:
            self.assertTrue(pid_alive(pid), f"child {pid} not running")

        self.assertIs(self.service.cancel(job), State.CANCELED)
        status = self.wait_terminal(job)
        self.assertIs(status.state, State.CANCELED)

        # The whole group — including the spawned children — must be gone.
        gone = wait_for(lambda: all(not pid_alive(pid) for pid in pids), timeout=5)
        self.assertTrue(gone, f"children still alive: {[p for p in pids if pid_alive(p)]}")


class StreamingTest(ServiceTestCase):
    def test_stderr_flood_blocks_neither_stdout_nor_cancel(self):
        service = self.make_service(run_time_limit=60.0, output_limit=1 << 24)
        self.addCleanup(service.shutdown)
        job = service.submit("mixed-output")
        self.assertTrue(
            wait_for(
                lambda: service.status(job).stdout_bytes > 0
                and service.status(job).stderr_bytes > 0
            ),
            "job never produced output on both streams",
        )
        # stderr is flooding; stdout must still make progress.
        before = service.status(job).stdout_bytes
        self.assertTrue(
            wait_for(lambda: service.status(job).stdout_bytes > before, timeout=5),
            "stdout stalled behind flooded stderr",
        )
        # Cancellation must be prompt despite both pipes being busy.
        start = time.monotonic()
        self.assertIs(service.cancel(job), State.CANCELED)
        self.assertLess(time.monotonic() - start, 2.0)
        status = service.wait(job, timeout=15.0)
        self.assertIs(status.state, State.CANCELED)

    def test_cursor_reads_reassemble_stream(self):
        job = self.service.submit("exit-race")
        status = self.wait_terminal(job)
        self.assertIs(status.state, State.SUCCEEDED)

        chunks = []
        cursor = 0
        while True:
            result = self.service.read(job, "stdout", cursor=cursor, max_bytes=3)
            chunks.append(result.data)
            cursor = result.next_cursor
            if result.eof:
                break
        self.assertEqual(b"".join(chunks), b"done\n")

        # A cursor into the middle of the stream works too.
        result = self.service.read(job, "stdout", cursor=1, max_bytes=3)
        self.assertEqual(result.data, b"one")
        # stderr was empty: immediate eof.
        self.assertTrue(self.service.read(job, "stderr", cursor=0).eof)

    def test_read_validation(self):
        job = self.service.submit("exit-race")
        self.wait_terminal(job)
        with self.assertRaises(ValueError):
            self.service.read(job, "combined")
        with self.assertRaises(ValueError):
            self.service.read(job, "stdout", cursor=-1)


class ExitStatusTest(ServiceTestCase):
    def test_success(self):
        job = self.service.submit("exit-race")
        status = self.wait_terminal(job)
        self.assertIs(status.state, State.SUCCEEDED)
        self.assertEqual(status.exit_code, 0)

    def test_nonzero_exit_is_failed(self):
        job = self.service.submit("fail")
        status = self.wait_terminal(job)
        self.assertIs(status.state, State.FAILED)
        self.assertEqual(status.exit_code, 3)
        err = self.service.read(job, "stderr", cursor=0, max_bytes=1 << 16)
        self.assertIn(b"failure details", err.data)


if __name__ == "__main__":
    unittest.main()
