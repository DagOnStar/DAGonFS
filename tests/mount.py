#!/usr/bin/env python3
"""Mount DAGonFS under MPI, run the POSIX integration suite, and unmount.

Usage::

    python3 tests/mount.py --build-dir build --model client-server --ranks 3
    python3 tests/mount.py --build-dir build --model peer-to-peer --ranks 2

The harness always unmounts every mountpoint it created and terminates the MPI
job, even when the tests fail. Extra arguments after ``--`` are passed to
``mpirun`` (for example ``-- --oversubscribe``).
"""

import argparse
import os
from pathlib import Path
import shutil
import subprocess
import sys
import tempfile
import time

HERE = Path(__file__).resolve().parent


def is_mounted(path):
    path = os.path.realpath(path)
    try:
        with open("/proc/self/mounts", encoding="utf-8") as mounts:
            return any(line.split()[1] == path for line in mounts)
    except FileNotFoundError:  # macOS
        return os.path.ismount(path)


def unmount(path):
    if not is_mounted(path):
        return
    if sys.platform == "darwin":
        command = ["umount", path]
    else:
        command = [shutil.which("fusermount3") or "fusermount", "-u", path]
    subprocess.run(command, check=False)


def wait_for(predicate, timeout):
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if predicate():
            return True
        time.sleep(0.2)
    return predicate()


def peer_to_peer_checks(root, peer):
    """Verify that metadata and data written on rank 0 are visible on another peer.

    ``peer`` must run on the same host as rank 0 (its mount point must be
    visible locally); the data blocks are still spread over every rank.
    """
    first, last = root / "0", root / str(peer)
    (first / "p2p-dir").mkdir()
    payload = bytes(range(256)) * 2048
    (first / "p2p-dir" / "data").write_bytes(payload)
    if not wait_for(lambda: (last / "p2p-dir" / "data").exists(), 10):
        raise AssertionError("file created on rank 0 is not visible on the last peer")
    observed = (last / "p2p-dir" / "data").read_bytes()
    if observed != payload:
        raise AssertionError(f"peer read {len(observed)} bytes, expected {len(payload)}")
    # Reverse direction: overwrite on the last peer, read back on rank 0.
    reverse = bytes(reversed(payload))[:300_001]
    (last / "p2p-dir" / "data").write_bytes(reverse)
    observed = (first / "p2p-dir" / "data").read_bytes()
    if observed != reverse:
        raise AssertionError(f"rank 0 read {len(observed)} bytes after a remote rewrite, expected {len(reverse)}")
    os.rename(first / "p2p-dir", first / "p2p-renamed")
    if not wait_for(lambda: (last / "p2p-renamed" / "data").exists(), 10):
        raise AssertionError("directory rename did not replicate")
    (first / "p2p-renamed" / "data").unlink()
    (first / "p2p-renamed").rmdir()
    if not wait_for(lambda: not (last / "p2p-renamed").exists(), 10):
        raise AssertionError("directory removal did not replicate")
    print("peer-to-peer replication checks: OK")


def main():
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--build-dir", default="build", type=Path)
    parser.add_argument("--model", choices=("client-server", "peer-to-peer"), default="client-server")
    parser.add_argument("--ranks", type=int, default=2)
    parser.add_argument("--root", type=Path, default=None, help="mount root (default: a new temporary directory)")
    parser.add_argument("--mpirun", default=os.environ.get("MPIRUN", "mpirun"))
    parser.add_argument("--timeout", type=float, default=30.0, help="seconds to wait for the mount")
    parser.add_argument("--peer-rank", type=int, default=None,
                        help="peer used for the cross-peer checks (default: last rank; must share rank 0's host)")
    parser.add_argument("mpirun_args", nargs="*")
    args = parser.parse_args()

    executable = "DAGonFS_CS.exe" if args.model == "client-server" else "DAGonFS_P2P.exe"
    binary = (args.build_dir / executable).resolve()
    if not binary.exists():
        parser.error(f"{binary} does not exist; build the project first")

    root = args.root or Path(tempfile.mkdtemp(prefix="dagonfs-mount-"))
    root.mkdir(parents=True, exist_ok=True)
    peer_rank = args.ranks - 1 if args.peer_rank is None else args.peer_rank
    if args.model == "client-server":
        mountpoints = [root]
    else:
        # Only the mount points of ranks on this host can be observed locally.
        mountpoints = [root / "0"] + ([root / str(peer_rank)] if peer_rank > 0 else [])
    command = [args.mpirun, "-np", str(args.ranks), *args.mpirun_args, str(binary), "-f", str(root), "0"]
    print("starting:", " ".join(command), flush=True)
    log = open(root.parent / f"{root.name}.log", "w", encoding="utf-8")
    job = subprocess.Popen(command, stdout=log, stderr=subprocess.STDOUT, cwd=root.parent)

    status = 1
    try:
        if not wait_for(lambda: all(is_mounted(m) for m in mountpoints) or job.poll() is not None, args.timeout):
            raise RuntimeError("DAGonFS did not mount in time")
        if job.poll() is not None:
            raise RuntimeError(f"DAGonFS exited early with status {job.returncode}")
        env = dict(os.environ, DAGONFS_MOUNTPOINT=str(mountpoints[0]))
        result = subprocess.run([sys.executable, str(HERE / "test_python_posix.py"), "-v"], env=env)
        status = result.returncode
        if status == 0 and args.model == "peer-to-peer" and args.ranks > 1:
            peer_to_peer_checks(root, peer_rank)
    except Exception as error:  # report and fall through to cleanup
        print(f"error: {error}", file=sys.stderr)
        status = status or 1
    finally:
        for mountpoint in mountpoints:
            unmount(str(mountpoint))
        try:
            job.wait(timeout=30)
        except subprocess.TimeoutExpired:
            job.terminate()
            try:
                job.wait(timeout=10)
            except subprocess.TimeoutExpired:
                job.kill()
                job.wait()
        for mountpoint in mountpoints:
            unmount(str(mountpoint))
        log.close()
        print(f"DAGonFS job exited with status {job.returncode}; log: {log.name}")
        if job.returncode not in (0, None) and status == 0:
            status = job.returncode
    return status


if __name__ == "__main__":
    sys.exit(main())
