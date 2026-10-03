"""Prints one line and exits 0 immediately.

Used to race a fast successful exit against a concurrent cancel.
"""

print("done", flush=True)
