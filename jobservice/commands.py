"""Registry of predefined commands the service is allowed to run.

The service never accepts arbitrary shell text. Callers refer to a command
by name; the registry maps each name to a fixed argv list (no shell
involved, ``shell=False`` semantics). Scripts live in the repository's
top-level ``commands/`` directory so they are versioned with the service.
"""

from __future__ import annotations

import sys
from pathlib import Path
from typing import Callable, Dict, List

SCRIPTS_DIR = Path(__file__).resolve().parent.parent / "commands"


class UnknownCommand(ValueError):
    """Raised when a caller asks for a command that is not registered."""


def _script(filename: str) -> List[str]:
    return [sys.executable, str(SCRIPTS_DIR / filename)]


# name -> factory producing the argv list to execute.
COMMANDS: Dict[str, Callable[[], List[str]]] = {
    # Ticks on stdout for ~60s. Used to occupy worker slots and as a
    # cancel/timeout target.
    "slow": lambda: _script("slow.py"),
    # Floods stderr (with a little stdout). Used to hit the output limit.
    "spam-stderr": lambda: _script("spam_stderr.py"),
    # Spawns children that sleep for a long time and print their pids.
    # Used to verify the whole process group is killed.
    "spawn-children": lambda: _script("spawn_children.py"),
    # Prints one line and exits 0 immediately. Used for the
    # exit-vs-cancel race.
    "exit-race": lambda: _script("exit_race.py"),
    # Writes both stdout and stderr continuously. Used to prove a flooded
    # stream neither blocks the other pipe nor prevents cancellation.
    "mixed-output": lambda: _script("mixed_output.py"),
    # Writes to stderr and exits with a non-zero status.
    "fail": lambda: _script("fail.py"),
}


def resolve(name: str) -> List[str]:
    """Return the argv list for a registered command name."""
    try:
        return COMMANDS[name]()
    except KeyError:
        raise UnknownCommand(
            f"unknown command {name!r}; allowed: {sorted(COMMANDS)}"
        ) from None
