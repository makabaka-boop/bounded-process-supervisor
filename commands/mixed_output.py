"""Writes stdout and stderr continuously until killed.

Used to prove that a flooded stderr does not starve stdout delivery and
that cancellation still works while both pipes are busy.
"""

import sys

for i in range(10_000_000):
    sys.stdout.write(f"out {i}\n")
    sys.stdout.flush()
    sys.stderr.write(f"err {i}\n")
    sys.stderr.flush()

sys.exit(0)
