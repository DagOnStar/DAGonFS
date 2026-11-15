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
import subprocess
import sys
import tempfile
import time

HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(HERE.parent / "python"))

from dagonfs import MODELS, Volume  # noqa: E402


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
    concurrent_originators(first, last)
    metadata_replication(first, last)
    print("peer-to-peer replication checks: OK")


def metadata_replication(first, last):
    """chmod, utime, extended attributes and symbolic links made on one peer reach the other."""
    (first / "meta").write_bytes(b"metadata")
    if not wait_for(lambda: (last / "meta").exists(), 10):
        raise AssertionError("file created on rank 0 is not visible on the last peer")
    os.chmod(first / "meta", 0o640)
    os.utime(first / "meta", ns=(1_000_000_000_123_456_789, 1_234_567_890_987_654_321))
    os.symlink("meta", first / "meta-link")
    checks = [
        ("mode", lambda: (os.stat(last / "meta").st_mode & 0o7777) == 0o640),
        ("mtime", lambda: os.stat(last / "meta").st_mtime_ns == 1_234_567_890_987_654_321),
        ("atime", lambda: os.stat(last / "meta").st_atime_ns == 1_000_000_000_123_456_789),
        ("symlink", lambda: os.path.islink(last / "meta-link") and os.readlink(last / "meta-link") == "meta"),
    ]
    if hasattr(os, "setxattr"):
        os.setxattr(first / "meta", "user.dagonfs", b"x" * 1000)
        checks.append(("xattr", lambda: os.getxattr(last / "meta", "user.dagonfs") == b"x" * 1000))
    for name, check in checks:
        if not wait_for(lambda: _quietly(check), 10):
            raise AssertionError(f"{name} change did not replicate")
    if hasattr(os, "removexattr"):
        os.removexattr(first / "meta", "user.dagonfs")
        if not wait_for(lambda: "user.dagonfs" not in os.listxattr(last / "meta"), 10):
            raise AssertionError("xattr removal did not replicate")
    (first / "meta-link").unlink()
    (first / "meta").unlink()
    if not wait_for(lambda: not os.path.lexists(last / "meta-link") and not (last / "meta").exists(), 10):
        raise AssertionError("removal of the symbolic link did not replicate")


def _quietly(check):
    try:
        return check()
    except OSError:
        return False


def concurrent_originators(first, last, files=12):
    """Two peers create, write and read files at the same time.

    Each peer is the root of its own transfers while serving the other's, which
    exercises the point-to-point data plane (concurrent collectives on one
    communicator would be erroneous) and the write acknowledgements.
    """
    import threading

    def payload(tag, index):
        return (f"{tag}-{index}-".encode() * 40_000)[: 150_000 + 9_973 * index]

    errors = []

    def writer(mount, tag):
        try:
            (mount / f"conc-{tag}").mkdir()
            for index in range(files):
                (mount / f"conc-{tag}" / f"f{index}").write_bytes(payload(tag, index))
        except Exception as error:  # reported by the main thread
            errors.append(error)

    threads = [threading.Thread(target=writer, args=(first, "a")),
               threading.Thread(target=writer, args=(last, "b"))]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join(120)
        if thread.is_alive():
            raise AssertionError("concurrent writers did not finish (deadlock?)")
    if errors:
        raise AssertionError(f"concurrent writer failed: {errors[0]!r}")
    # Each peer reads what the other one wrote.
    for mount, tag in ((last, "a"), (first, "b")):
        for index in range(files):
            observed = (mount / f"conc-{tag}" / f"f{index}").read_bytes()
            expected = payload(tag, index)
            if observed != expected:
                first_difference = next((i for i, (x, y) in enumerate(zip(observed, expected)) if x != y),
                                        min(len(observed), len(expected)))
                raise AssertionError(f"conc-{tag}/f{index}: content differs across peers "
                                     f"(read {len(observed)} bytes, expected {len(expected)}; "
                                     f"first difference at byte {first_difference})")
    for tag in ("a", "b"):
        for index in range(files):
            (first / f"conc-{tag}" / f"f{index}").unlink()
        (first / f"conc-{tag}").rmdir()


def main():
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--build-dir", default="build", type=Path)
    parser.add_argument("--model", choices=MODELS, default="client-server")
    parser.add_argument("--ranks", type=int, default=2)
    parser.add_argument("--root", type=Path, default=None, help="mount root (default: a new temporary directory)")
    parser.add_argument("--mpirun", default=os.environ.get("MPIRUN", "mpirun"))
    parser.add_argument("--timeout", type=float, default=30.0, help="seconds to wait for the mount")
    parser.add_argument("--peer-rank", type=int, default=None,
                        help="peer used for the cross-peer checks (default: last rank; must share rank 0's host)")
    parser.add_argument("mpirun_args", nargs="*")
    args = parser.parse_args()

    binary = (args.build_dir / "dagonfs").resolve()
    if not binary.exists():
        parser.error(f"{binary} does not exist; build the project first")

    root = args.root or Path(tempfile.mkdtemp(prefix="dagonfs-mount-"))
    peer_rank = args.ranks - 1 if args.peer_rank is None else args.peer_rank
    # Only the mount points of ranks on this host can be observed locally.
    local_ranks = [0] + ([peer_rank] if args.model == "peer-to-peer" and peer_rank > 0 else [])
    volume = Volume(root, model=args.model, ranks=args.ranks, executable=binary, mpirun=args.mpirun,
                    mpirun_args=args.mpirun_args, local_ranks=local_ranks)
    print("starting:", " ".join(volume.command()), flush=True)

    status = 1
    try:
        volume.start(timeout=args.timeout)
        env = dict(os.environ, DAGONFS_MOUNTPOINT=str(volume.mountpoint(0)))
        result = subprocess.run([sys.executable, str(HERE / "test_python_posix.py"), "-v"], env=env)
        status = result.returncode
        if status == 0 and args.model == "peer-to-peer" and args.ranks > 1:
            peer_to_peer_checks(root, peer_rank)
    except Exception as error:  # report and fall through to cleanup
        print(f"error: {error}", file=sys.stderr)
        status = status or 1
    finally:
        job_status = volume.stop()
        print(f"DAGonFS job exited with status {job_status}; log: {volume.log_path}")
        if job_status not in (0, None) and status == 0:
            status = job_status
    return status


if __name__ == "__main__":
    sys.exit(main())
