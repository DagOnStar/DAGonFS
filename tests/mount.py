#!/usr/bin/env python3
"""Mount DAGonFS under MPI, run the POSIX integration suite, and unmount.

Usage::

    python3 tests/mount.py --build-dir build --model client-server --ranks 3
    python3 tests/mount.py --build-dir build --model peer-to-peer --ranks 2

The harness always unmounts every mountpoint it created and terminates the MPI
job, even when the tests fail. Extra arguments after ``--`` are passed to
``mpirun`` (for example ``-- --oversubscribe``).

After the suite, the harness runs the parallel MPI-IO check
(``dagonfs_mpiio_check``, built from ``tests/mpiio_check.c``) as an MPI job of
its own on this host, once per MPI-IO implementation that Open MPI provides
(OMPIO and ROMIO). In peer-to-peer mode another peer then verifies the shared
file that the MPI-IO ranks wrote through rank 0's mount point, and a last job
exchanges one file per process between ranks that use different peers.
"""

import argparse
import os
from pathlib import Path
import re
import shutil
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
    cross_peer_locks(first, last)
    print("peer-to-peer replication checks: OK")


def _helper(*args):
    return subprocess.Popen([sys.executable, str(HERE / "lock_helper.py")] + [str(a) for a in args],
                            stdin=subprocess.PIPE, stdout=subprocess.PIPE, universal_newlines=True)


def _line(process, timeout=20.0):
    import select
    ready, _, _ = select.select([process.stdout], [], [], timeout)
    return process.stdout.readline().strip() if ready else None


def cross_peer_locks(first, last):
    """Locks taken through one peer's mount exclude processes of another peer.

    Also checks that a FIFO created on one peer is a FIFO on the other, and
    that a counter incremented under a lock by processes on both peers loses
    no update (each holder must observe the previous holder's write-back even
    when it opened the file before acquiring the lock).
    """
    import fcntl
    (first / "locked").write_bytes(b"0")
    os.mkfifo(str(first / "fifo"))
    if not wait_for(lambda: (last / "locked").exists() and (last / "fifo").exists(), 10):
        raise AssertionError("files created on rank 0 are not visible on the last peer")
    if not stat_is_fifo(last / "fifo"):
        raise AssertionError("FIFO did not replicate as a FIFO")
    for kind in ("posix", "flock"):
        with open(str(first / "locked"), "r+b") as stream:
            if kind == "posix":
                fcntl.lockf(stream.fileno(), fcntl.LOCK_EX)
            else:
                fcntl.flock(stream.fileno(), fcntl.LOCK_EX)
            probe = _helper("try", last / "locked", kind, "ex", 0, 0)
            if _line(probe) != "busy":
                raise AssertionError(f"{kind} lock held on rank 0 is not seen by the last peer")
            probe.wait(10)
            if kind == "posix":
                probe = _helper("getlk", last / "locked", 0, 0)
                answer = _line(probe)
                probe.wait(10)
                if not answer or not answer.startswith("wrlck"):
                    raise AssertionError(f"F_GETLK on the last peer reported {answer!r}")
            waiter = _helper("wait", last / "locked", kind, "ex", 0, 0)
            if _line(waiter) != "waiting" or _line(waiter, 0.5) is not None:
                raise AssertionError(f"{kind} waiter on the last peer did not block")
        # Closing on rank 0 grants the waiter on the last peer.
        if _line(waiter) != "granted" or waiter.wait(10) != 0:
            raise AssertionError(f"{kind} waiter on the last peer was not granted after the release")
        (first / "locked").write_bytes(b"0")
        workers = [_helper("count", mount / "locked", 20, kind) for mount in (first, last, first, last)]
        for worker in workers:
            if _line(worker, 180) != "done" or worker.wait(10) != 0:
                raise AssertionError(f"{kind} counter worker failed")
        value = int((first / "locked").read_bytes())
        if value != 80:
            raise AssertionError(f"{kind}: counter is {value} after 80 locked increments on two peers")
    # Unlocking without closing publishes the data written under the lock.
    with open(str(first / "locked"), "r+b") as stream:
        fcntl.lockf(stream.fileno(), fcntl.LOCK_EX)
        stream.write(b"written under the lock")
        stream.flush()
        fcntl.lockf(stream.fileno(), fcntl.LOCK_UN)
        observed = (last / "locked").read_bytes()
        if not observed.startswith(b"written under the lock"):
            raise AssertionError(f"data written under a lock are not visible on the last peer after unlock: {observed!r}")
    (first / "locked").unlink()
    (first / "fifo").unlink()
    if not wait_for(lambda: not (last / "locked").exists() and not (last / "fifo").exists(), 10):
        raise AssertionError("removals did not replicate")


def stat_is_fifo(path):
    import stat
    return stat.S_ISFIFO(os.lstat(str(path)).st_mode)


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


def mpiio_components(mpirun):
    """MPI-IO components to test: Open MPI's (ompio, romio...), or the default one otherwise."""
    info = Path(shutil.which(mpirun) or mpirun).with_name("ompi_info")
    try:
        output = subprocess.run([str(info)], capture_output=True, text=True, timeout=60).stdout
    except (OSError, subprocess.SubprocessError):
        return [None]
    return re.findall(r"MCA io: (\w+)", output) or [None]


def mpiio_checks(args, volume, peer_rank):
    """Parallel MPI-IO on the mounted volume; returns 0 on success.

    The ranks of the MPI-IO job run on this host (where the mount point is),
    in a job separate from the DAGonFS one.
    """
    binary = (args.build_dir / "dagonfs_mpiio_check").resolve()
    if not binary.exists():
        print(f"MPI-IO checks skipped: {binary} was not built")
        return 0
    first = Path(volume.mountpoint(0))
    launch = args.mpiio_args.format(ranks=args.mpiio_ranks).split()
    for component in mpiio_components(args.mpirun):
        directory = first / f"mpiio-{component or 'default'}"
        directory.mkdir()
        mca = ["--mca", "io", component] if component else []
        command = [args.mpirun, "-np", str(args.mpiio_ranks)] + launch + mca + [str(binary), "run", str(directory)]
        print(f"MPI-IO ({component or 'default'}, {args.mpiio_ranks} ranks):", " ".join(command), flush=True)
        if subprocess.run(command, timeout=600).returncode != 0:
            return 1
        if args.model == "peer-to-peer" and peer_rank > 0:
            # The shared file written through rank 0's mount, read through another peer's.
            other = Path(volume.mountpoint(peer_rank)) / directory.name
            if not wait_for(lambda: (other / "independent.dat").exists(), 10):
                print("MPI-IO file written on rank 0 is not visible on the other peer", file=sys.stderr)
                return 1
            command = [args.mpirun, "-np", "2"] + args.mpiio_args.format(ranks=2).split() + mca + [
                str(binary), "verify", str(other), str(args.mpiio_ranks)]
            if subprocess.run(command, timeout=600).returncode != 0:
                return 1
        # One file per process, written and read through every local mount point:
        # in the peer-to-peer model, each rank reads a file written through another peer.
        views = [directory] + ([Path(volume.mountpoint(peer_rank)) / directory.name]
                               if args.model == "peer-to-peer" and peer_rank > 0 else [])
        command = [args.mpirun, "-np", str(args.mpiio_ranks)] + launch + mca + [
            str(binary), "peers"] + [str(view) for view in views]
        if subprocess.run(command, timeout=600).returncode != 0:
            return 1
        shutil.rmtree(directory)
    print("MPI-IO checks: OK")
    return 0


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
    parser.add_argument("--mpiio-ranks", type=int, default=4, help="ranks of the MPI-IO check (0 skips it)")
    parser.add_argument("--mpiio-args", default="--oversubscribe --host localhost:{ranks}",
                        help="mpirun options of the MPI-IO job, which must run on this host ({ranks} is replaced)")
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
        if status == 0 and args.mpiio_ranks > 0:
            status = mpiio_checks(args, volume, peer_rank)
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
