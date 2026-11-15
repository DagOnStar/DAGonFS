#!/usr/bin/env python3
"""Check that the DAGonFS executables print usage and fail without arguments.

Usage: python3 tests/usage.py [build-dir]
"""

from pathlib import Path
import subprocess
import sys


def main():
    build_dir = Path(sys.argv[1] if len(sys.argv) > 1 else "build")
    failures = 0
    # dagonfs plus its backward-compatible aliases (symbolic links).
    for name in ("dagonfs", "DAGonFS_CS.exe", "DAGonFS_P2P.exe"):
        binary = build_dir / name
        if not binary.exists():
            print(f"skip: {binary} not built")
            continue
        result = subprocess.run([str(binary)], capture_output=True, text=True, timeout=30)
        output = result.stdout + result.stderr
        if "Usage:" not in output or result.returncode == 0:
            print(f"FAIL: {name} returned {result.returncode} with output {output!r}")
            failures += 1
        else:
            print(f"ok: {name}")
    return 1 if failures else 0


if __name__ == "__main__":
    sys.exit(main())
