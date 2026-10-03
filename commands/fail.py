"""Writes to stderr and exits with a non-zero status."""

import sys

print("about to fail", flush=True)
print("failure details on stderr", file=sys.stderr, flush=True)
sys.exit(3)
