"""Local job service for Linux.

Executes only predefined repository commands (never arbitrary shell text),
at most a fixed number concurrently, streaming stdout/stderr to per-job
files with a runtime limit and a combined output byte limit. Cancel,
timeout and output-limit violations kill the whole process group.
"""

from .commands import COMMANDS, UnknownCommand, resolve
from .service import (
    JobService,
    ReadResult,
    State,
    Status,
    TERMINAL_STATES,
    UnknownJob,
)

__all__ = [
    "COMMANDS",
    "JobService",
    "ReadResult",
    "State",
    "Status",
    "TERMINAL_STATES",
    "UnknownCommand",
    "UnknownJob",
    "resolve",
]
