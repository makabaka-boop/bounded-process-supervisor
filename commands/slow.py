"""Ticks on stdout roughly every 100ms for about 60 seconds.

Used to occupy a worker slot and as a cancel/timeout target.
"""

import sys
import time

for i in range(600):
    print(f"tick {i}", flush=True)
    time.sleep(0.1)

sys.exit(0)
