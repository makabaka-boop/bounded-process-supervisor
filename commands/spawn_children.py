"""Spawns several long-lived child processes and reports all pids.

Children inherit the process group, so a group kill must take them down
along with this parent. The pids are printed to stdout so tests can
verify that every spawned process is gone after cancellation.
"""

import os
import subprocess
import sys
import time

CHILD = (
    "import os, time, sys;"
    "print(f'child {os.getpid()}', flush=True);"
    "sys.stdout.flush();"
    "time.sleep(300)"
)

print(f"parent {os.getpid()}", flush=True)
children = []
for _ in range(3):
    children.append(subprocess.Popen([sys.executable, "-c", CHILD]))

# Wait for children to announce themselves, then idle until killed.
time.sleep(300)
