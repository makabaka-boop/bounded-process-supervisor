"""Core job service.

Concurrency model
-----------------
* ``submit`` registers a :class:`_Job` and starts one worker thread per job.
  A ``threading.Semaphore(max_concurrent)`` inside the worker caps the
  number of jobs actually running; extra jobs stay ``QUEUED`` until a slot
  frees up (and can be cancelled while queued).
* Each running job gets two reader threads, one per pipe, so a process
  flooding stderr can never block stdout delivery (or vice versa), and
  neither flood can delay cancellation, which is driven from other threads.
* A ``threading.Timer`` enforces the runtime limit; the reader threads
  enforce the combined output byte limit; ``cancel`` is external. All three
  funnel into :meth:`JobService._request_kill`, which is first-wins under
  the job lock and kills the whole process group (``start_new_session=True``
  + ``os.killpg``), so children spawned by the command die too.
* Exactly one thread (the worker) writes the terminal state, from
  ``kill_reason`` if a kill was requested, else from the exit code. State
  transitions happen under the job lock, so races (e.g. exit vs. cancel)
  resolve to exactly one terminal state and repeated cancels are no-ops.

Output is spooled to ``<data_dir>/<job_id>/{stdout,stderr}`` as it arrives;
clients read it back by byte-offset cursor via :meth:`JobService.read`.
"""

from __future__ import annotations

import os
import signal
import subprocess
import threading
import time
import uuid
from dataclasses import dataclass
from enum import Enum
from pathlib import Path
from typing import Dict, List, Optional

from .commands import resolve

_READ_CHUNK = 65536


class State(str, Enum):
    QUEUED = "QUEUED"
    RUNNING = "RUNNING"
    SUCCEEDED = "SUCCEEDED"
    FAILED = "FAILED"
    CANCELED = "CANCELED"
    TIMEOUT = "TIMEOUT"
    OUTPUT_LIMIT_EXCEEDED = "OUTPUT_LIMIT_EXCEEDED"


TERMINAL_STATES = frozenset(
    {
        State.SUCCEEDED,
        State.FAILED,
        State.CANCELED,
        State.TIMEOUT,
        State.OUTPUT_LIMIT_EXCEEDED,
    }
)

# Terminal states that are reached by killing the process group.
_KILL_STATES = {State.CANCELED, State.TIMEOUT, State.OUTPUT_LIMIT_EXCEEDED}


class UnknownJob(KeyError):
    """Raised when a job id is not known to the service."""


@dataclass(frozen=True)
class Status:
    """Point-in-time snapshot of a job."""

    job_id: str
    command: str
    state: State
    exit_code: Optional[int]
    stdout_bytes: int
    stderr_bytes: int
    queued_at: float
    started_at: Optional[float]
    ended_at: Optional[float]

    @property
    def terminal(self) -> bool:
        return self.state in TERMINAL_STATES


@dataclass(frozen=True)
class ReadResult:
    """Result of a cursor-based read."""

    data: bytes
    next_cursor: int
    # True once the job is terminal and the cursor has reached the end of
    # the stream; no more data will ever appear after this.
    eof: bool


class _Job:
    def __init__(self, command: str, argv: List[str], directory: Path) -> None:
        self.id = uuid.uuid4().hex
        self.command = command
        self.argv = argv
        self.dir = directory / self.id
        self.dir.mkdir(parents=True)
        # Created empty so cursor reads work even before the job starts.
        (self.dir / "stdout").touch()
        (self.dir / "stderr").touch()

        self.lock = threading.Lock()
        self.done = threading.Event()
        self.state = State.QUEUED
        self.exit_code: Optional[int] = None
        # Set (first-wins) by _request_kill; the worker maps it to the
        # terminal state once the process is reaped.
        self.kill_reason: Optional[State] = None
        self.proc: Optional[subprocess.Popen] = None
        self.stdout_bytes = 0
        self.stderr_bytes = 0
        self.queued_at = time.time()
        self.started_at: Optional[float] = None
        self.ended_at: Optional[float] = None

    def snapshot(self) -> Status:
        with self.lock:
            return Status(
                job_id=self.id,
                command=self.command,
                state=self.state,
                exit_code=self.exit_code,
                stdout_bytes=self.stdout_bytes,
                stderr_bytes=self.stderr_bytes,
                queued_at=self.queued_at,
                started_at=self.started_at,
                ended_at=self.ended_at,
            )


class JobService:
    """Runs predefined commands with concurrency, time and output limits."""

    def __init__(
        self,
        data_dir: os.PathLike | str,
        max_concurrent: int = 2,
        run_time_limit: float = 10.0,
        output_limit: int = 1 << 20,
    ) -> None:
        """
        :param data_dir: directory where per-job output files are spooled.
        :param max_concurrent: maximum number of jobs running at once.
        :param run_time_limit: seconds a job may run before its process
            group is killed (terminal state ``TIMEOUT``).
        :param output_limit: combined stdout+stderr bytes a job may produce
            before its process group is killed (terminal state
            ``OUTPUT_LIMIT_EXCEEDED``).
        """
        self._data_dir = Path(data_dir)
        self._data_dir.mkdir(parents=True, exist_ok=True)
        self._slots = threading.Semaphore(max_concurrent)
        self._run_time_limit = run_time_limit
        self._output_limit = output_limit
        self._jobs: Dict[str, _Job] = {}
        self._workers: List[threading.Thread] = []
        self._lock = threading.Lock()
        self._closed = False

    # ------------------------------------------------------------------ API

    def submit(self, command: str) -> str:
        """Queue a predefined command for execution; returns the job id."""
        argv = resolve(command)  # raises UnknownCommand for anything else
        with self._lock:
            if self._closed:
                raise RuntimeError("service is shut down")
            job = _Job(command, argv, self._data_dir)
            self._jobs[job.id] = job
            worker = threading.Thread(
                target=self._run, args=(job,), name=f"job-{job.id[:8]}", daemon=True
            )
            self._workers.append(worker)
            worker.start()
            return job.id

    def cancel(self, job_id: str) -> State:
        """Cancel a job. Idempotent: a job already in a terminal state keeps
        that state and the same state is returned again — a repeated cancel
        never produces a second result."""
        job = self._get(job_id)
        with job.lock:
            if job.state in TERMINAL_STATES:
                return job.state
            if job.state is State.QUEUED:
                job.state = State.CANCELED
                job.ended_at = time.time()
                job.done.set()
                return job.state
            # RUNNING: the kill decision is made under the same lock that
            # the worker uses to finalize, so the returned state is always
            # the state the job will end in.
            self._kill_locked(job, State.CANCELED)
            return State.CANCELED

    def status(self, job_id: str) -> Status:
        return self._get(job_id).snapshot()

    def wait(self, job_id: str, timeout: Optional[float] = None) -> Status:
        """Block until the job reaches a terminal state (or *timeout*)."""
        job = self._get(job_id)
        job.done.wait(timeout)
        return job.snapshot()

    def read(
        self, job_id: str, stream: str, cursor: int = 0, max_bytes: int = 65536
    ) -> ReadResult:
        """Read up to *max_bytes* of a saved stream starting at *cursor*.

        Cursors are byte offsets; output files are append-only, so a cursor
        stays valid for the life of the job. ``eof`` is true once the job is
        terminal and everything has been read.
        """
        if stream not in ("stdout", "stderr"):
            raise ValueError("stream must be 'stdout' or 'stderr'")
        if cursor < 0:
            raise ValueError("cursor must be >= 0")
        job = self._get(job_id)
        with open(job.dir / stream, "rb") as fh:
            fh.seek(cursor)
            data = fh.read(max_bytes)
        next_cursor = cursor + len(data)
        snap = job.snapshot()
        size = snap.stdout_bytes if stream == "stdout" else snap.stderr_bytes
        return ReadResult(
            data=data,
            next_cursor=next_cursor,
            eof=snap.terminal and next_cursor >= size,
        )

    def shutdown(self) -> None:
        """Cancel everything still active and wait for workers to finish."""
        with self._lock:
            self._closed = True
            jobs = list(self._jobs.values())
            workers = list(self._workers)
        for job in jobs:
            self.cancel(job.id)
        for worker in workers:
            worker.join(timeout=10)

    # -------------------------------------------------------------- intern

    def _get(self, job_id: str) -> _Job:
        try:
            return self._jobs[job_id]
        except KeyError:
            raise UnknownJob(job_id) from None

    def _request_kill(self, job: _Job, reason: State) -> None:
        """First-wins kill request from the timer/reader threads."""
        with job.lock:
            self._kill_locked(job, reason)

    def _kill_locked(self, job: _Job, reason: State) -> None:
        """Terminate the whole process group. Caller must hold ``job.lock``;
        only the first reason (while RUNNING) takes effect."""
        assert reason in _KILL_STATES
        if job.state is not State.RUNNING or job.kill_reason is not None:
            return
        job.kill_reason = reason
        proc = job.proc
        if proc is not None:
            try:
                # The child is a session leader (start_new_session=True), so
                # its pid is the process-group id: this hits every process
                # the command spawned, not just the direct child. killpg is
                # a non-blocking syscall, safe to issue under the lock.
                os.killpg(proc.pid, signal.SIGKILL)
            except (ProcessLookupError, PermissionError):
                pass  # already exited; the worker will finalize normally

    def _run(self, job: _Job) -> None:
        with self._slots:
            with job.lock:
                if job.state is not State.QUEUED:
                    return  # cancelled while queued; never starts
            try:
                proc = subprocess.Popen(
                    job.argv,
                    stdin=subprocess.DEVNULL,
                    stdout=subprocess.PIPE,
                    stderr=subprocess.PIPE,
                    start_new_session=True,
                )
            except OSError:
                with job.lock:
                    job.state = State.FAILED
                    job.ended_at = time.time()
                    job.done.set()
                return

            with job.lock:
                if job.state is not State.QUEUED:
                    # Cancelled during spawn: kill the fresh process group
                    # and leave the already-terminal state untouched.
                    proc_to_kill = proc
                else:
                    proc_to_kill = None
                    job.proc = proc
                    job.state = State.RUNNING
                    job.started_at = time.time()
            if proc_to_kill is not None:
                try:
                    os.killpg(proc_to_kill.pid, signal.SIGKILL)
                except (ProcessLookupError, PermissionError):
                    pass
                proc_to_kill.wait()
                proc_to_kill.stdout.close()
                proc_to_kill.stderr.close()
                return

            timer = threading.Timer(
                self._run_time_limit,
                self._request_kill,
                args=(job, State.TIMEOUT),
            )
            timer.start()
            readers = [
                threading.Thread(
                    target=self._reader,
                    args=(job, proc.stdout, "stdout"),
                    name=f"job-{job.id[:8]}-stdout",
                    daemon=True,
                ),
                threading.Thread(
                    target=self._reader,
                    args=(job, proc.stderr, "stderr"),
                    name=f"job-{job.id[:8]}-stderr",
                    daemon=True,
                ),
            ]
            for reader in readers:
                reader.start()

            exit_code = proc.wait()  # reaps the direct child
            timer.cancel()
            for reader in readers:
                reader.join()  # drain both pipes to EOF before finalizing

            with job.lock:
                if job.kill_reason is not None:
                    job.state = job.kill_reason
                elif exit_code == 0:
                    job.state = State.SUCCEEDED
                else:
                    job.state = State.FAILED
                job.exit_code = exit_code
                job.ended_at = time.time()
                job.done.set()

    def _reader(self, job: _Job, pipe, stream: str) -> None:
        """Drain one pipe to its file until EOF. Both pipes are drained
        concurrently, so a flood on one stream can neither block the other
        nor delay a kill requested from another thread."""
        fd = pipe.fileno()
        with open(job.dir / stream, "wb", buffering=0) as out:
            while True:
                try:
                    chunk = os.read(fd, _READ_CHUNK)
                except OSError:
                    break
                if not chunk:
                    break
                out.write(chunk)
                with job.lock:
                    if stream == "stdout":
                        job.stdout_bytes += len(chunk)
                    else:
                        job.stderr_bytes += len(chunk)
                    total = job.stdout_bytes + job.stderr_bytes
                if total > self._output_limit:
                    self._request_kill(job, State.OUTPUT_LIMIT_EXCEEDED)
                    # Keep draining: the dying process must not block on a
                    # full pipe while the kill lands.
        pipe.close()
