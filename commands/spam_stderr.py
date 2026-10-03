"""Floods stderr with error lines (and a little stdout) until killed.

Used to exercise the combined output byte limit and to prove that a
saturated stderr pipe neither blocks stdout delivery nor cancellation.
"""

import sys

for i in range(10_000_000):
    sys.stderr.write(f"error line {i}: something went wrong\n")
    if i % 64 == 0:
        sys.stderr.flush()
        sys.stdout.write(f"progress {i}\n")
        sys.stdout.flush()

sys.exit(0)
