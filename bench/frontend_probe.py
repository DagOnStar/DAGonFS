#!/usr/bin/env python3
"""Positional-I/O probe for the DAGonFS frontends.

Each repeat starts a fresh volume, writes OPS blocks of SIZE bytes at
successive offsets with pwrite, reads them back with pread, closes the file,
shuts the volume down, and records operations per second only after a clean
shutdown. Modes:

  native    C client library (ctypes), client-server volume
  preload   a C program under LD_PRELOAD / DYLD_INSERT_LIBRARIES
  fuse      FUSE3 mount, client-server
  fuse-p2p  FUSE3 mount, peer-to-peer
  local     a temporary file on the local file system (reference)

Example::

    python3 bench/frontend_probe.py --modes native,preload,local --repeats 5 \
        --ops 1000 --output bench/results/frontends-host.json -- --oversubscribe
"""
import argparse
import ctypes
import json
import os
import platform
import shutil
import socket
import subprocess
import sys
import tempfile
import time
from datetime import datetime, timezone
from pathlib import Path

REPO = Path(__file__).resolve().parents[1]
EXTENSION = "dylib" if sys.platform == "darwin" else "so"


def timed_loop(write, read, ops, size):
    start = time.perf_counter()
    for i in range(ops):
        write(i * size)
    writes = ops / (time.perf_counter() - start)
    start = time.perf_counter()
    for i in range(ops):
        read(i * size)
    return {"pwrite": writes, "pread": ops / (time.perf_counter() - start)}


def posix_probe(path, ops, size):
    fd = os.open(path, os.O_CREAT | os.O_RDWR | os.O_TRUNC, 0o600)
    data = b"x" * size
    try:
        def write(offset):
            assert os.pwrite(fd, data, offset) == size

        def read(offset):
            assert len(os.pread(fd, size, offset)) == size
        return timed_loop(write, read, ops, size)
    finally:
        os.close(fd)


class NativeVolume:
    """mpirun ... dagonfs --frontend native SOCKET, stopped with SHUTDOWN."""

    def __init__(self, args, scratch):
        self.socket = Path(scratch) / "v.sock"
        command = [args.mpirun, "-np", str(args.ranks), *args.mpirun_args,
                   str(args.build / "dagonfs"), "--frontend", "native", str(self.socket)]
        self.process = subprocess.Popen(command, stderr=subprocess.PIPE)
        deadline = time.monotonic() + 60
        while not self.socket.exists():
            if self.process.poll() is not None:
                raise RuntimeError(self.process.stderr.read().decode(errors="replace"))
            if time.monotonic() > deadline:
                raise TimeoutError("native socket did not appear")
            time.sleep(0.02)
        self.lib = ctypes.CDLL(str(args.build / f"libdagonfs_preload.{EXTENSION}"), use_errno=True)
        self.lib.dagonfs_client_set_socket.argtypes = [ctypes.c_char_p]
        self.lib.dagonfs_client_set_socket(str(self.socket).encode())

    def close(self):
        try:
            if self.lib.dagonfs_client_shutdown() != 0:
                raise RuntimeError("shutdown failed")
            if self.process.wait(timeout=60) != 0:
                raise RuntimeError(f"volume exited with {self.process.returncode}")
        finally:
            if self.process.poll() is None:
                self.process.kill()
                self.process.wait()


def native_probe(args, scratch):
    volume = NativeVolume(args, scratch)
    try:
        lib = volume.lib
        lib.dagonfs_client_open.argtypes = [ctypes.c_char_p, ctypes.c_int, ctypes.c_uint]
        lib.dagonfs_client_open.restype = ctypes.c_int64
        for name in ("pread", "pwrite"):
            fn = getattr(lib, "dagonfs_client_" + name)
            fn.argtypes = [ctypes.c_int64, ctypes.c_void_p, ctypes.c_size_t, ctypes.c_int64]
            fn.restype = ctypes.c_ssize_t
        lib.dagonfs_client_close.argtypes = [ctypes.c_int64]
        handle = lib.dagonfs_client_open(b"/probe", os.O_CREAT | os.O_RDWR | os.O_TRUNC, 0o600)
        assert handle >= 0, ctypes.get_errno()
        buffer = ctypes.create_string_buffer(b"x" * args.size, args.size)
        size = args.size

        def write(offset):
            assert lib.dagonfs_client_pwrite(handle, buffer, size, offset) == size

        def read(offset):
            assert lib.dagonfs_client_pread(handle, buffer, size, offset) == size
        sample = timed_loop(write, read, args.ops, size)
        assert lib.dagonfs_client_close(handle) == 0
    finally:
        volume.close()
    return sample


def preload_probe(args, scratch):
    binary = Path(scratch) / "preload_probe"
    subprocess.run([os.environ.get("CC", "cc"), "-O2", str(REPO / "bench/preload_probe.c"), "-o", str(binary)],
                   check=True)
    volume = NativeVolume(args, scratch)
    try:
        env = dict(os.environ, DAGONFS_SOCKET=str(volume.socket), DAGONFS_PRELOAD_ROOT="/dagonfs")
        env["DYLD_INSERT_LIBRARIES" if sys.platform == "darwin" else "LD_PRELOAD"] = \
            str(args.build / f"libdagonfs_preload.{EXTENSION}")
        output = subprocess.check_output([str(binary), "/dagonfs/probe", str(args.ops), str(args.size)],
                                         env=env, text=True, timeout=600)
        sample = json.loads(output)
    finally:
        volume.close()
    return sample


def fuse_probe(args, scratch, model):
    sys.path.insert(0, str(REPO / "python"))
    from dagonfs import Volume
    root = Path(scratch) / "mount"
    with Volume(root, model=model, ranks=args.ranks, executable=args.build / "dagonfs",
                mpirun_args=args.mpirun_args):
        path = root / "0" / "probe" if model == "peer-to-peer" else root / "probe"
        return posix_probe(path, args.ops, args.size)


def local_probe(args, scratch):
    return posix_probe(Path(scratch) / "probe", args.ops, args.size)


PROBES = {
    "native": native_probe,
    "preload": preload_probe,
    "fuse": lambda a, s: fuse_probe(a, s, "client-server"),
    "fuse-p2p": lambda a, s: fuse_probe(a, s, "peer-to-peer"),
    "local": local_probe,
}


def main():
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--modes", default="native,preload,local")
    parser.add_argument("--repeats", type=int, default=5)
    parser.add_argument("--ops", type=int, default=1000)
    parser.add_argument("--size", type=int, default=4096)
    parser.add_argument("--ranks", type=int, default=2)
    parser.add_argument("--build-dir", default=str(REPO / "build"))
    parser.add_argument("--mpirun", default="mpirun")
    parser.add_argument("--output")
    parser.add_argument("--host", default=socket.gethostname().split(".")[0])
    parser.add_argument("mpirun_args", nargs="*")
    args = parser.parse_args()
    args.build = Path(args.build_dir).resolve()
    modes = [m for m in args.modes.split(",") if m]
    unknown = set(modes) - PROBES.keys()
    if unknown:
        parser.error(f"unknown modes: {', '.join(sorted(unknown))}")

    result = {
        "host": args.host,
        "system": f"{platform.system()} {platform.machine()} {platform.release()}",
        "date": datetime.now(timezone.utc).isoformat(timespec="seconds"),
        "workload": f"{args.ops} sequential {args.size}-byte pwrite then pread, fresh volume per repeat, "
                    f"{args.ranks} MPI ranks",
        "units": "operations per second",
        "ops": args.ops, "size": args.size, "ranks": args.ranks,
    }
    for mode in modes:
        samples = {"pwrite": [], "pread": []}
        for repeat in range(args.repeats):
            # Unix socket paths are limited to ~104 bytes: keep scratch short.
            scratch = tempfile.mkdtemp(prefix="dgp-", dir="/tmp" if os.path.isdir("/tmp") else None)
            try:
                sample = PROBES[mode](args, scratch)
            finally:
                shutil.rmtree(scratch, ignore_errors=True)
            for key in samples:
                samples[key].append(sample[key])
            print(f"{mode} #{repeat + 1}: pwrite {sample['pwrite']:,.0f}/s  pread {sample['pread']:,.0f}/s",
                  file=sys.stderr, flush=True)
        result[mode] = samples
    text = json.dumps(result, indent=2)
    if args.output:
        Path(args.output).write_text(text + "\n")
    print(text)


if __name__ == "__main__":
    main()
