#!/usr/bin/env python3
"""File-system micro-benchmarks for workflow scratch spaces (standard library only).

Measures, in a fresh directory below ``--dir``:

* metadata: create (open/close), stat and unlink of N empty files; mkdir/rmdir of N directories;
* small files: write and read back N files of 4 KiB (open, transfer, close);
* large files: sequential write (with fsync) and read of one file per size, in 1 MiB requests.

Before every read the file's pages are dropped from the client page cache
(``posix_fadvise(POSIX_FADV_DONTNEED)``), so that an NFS client cannot serve the
read from memory filled by the preceding write. Each measurement is repeated and
reported as the median, minimum and maximum. Results are printed as one line of JSON.
"""

import argparse
import json
import os
from pathlib import Path
import shutil
import statistics
import sys
import time
import uuid

MiB = 1 << 20


def drop_cache(path):
    fd = os.open(path, os.O_RDONLY)
    try:
        if hasattr(os, "posix_fadvise"):
            os.posix_fadvise(fd, 0, 0, os.POSIX_FADV_DONTNEED)
    finally:
        os.close(fd)


def timed(function):
    start = time.perf_counter()
    function()
    return time.perf_counter() - start


def summary(samples, scale=1.0):
    """Median, min and max of ``scale / sample`` (a rate) over the repetitions."""
    rates = sorted(scale / s for s in samples)
    return {"median": statistics.median(rates), "min": rates[0], "max": rates[-1], "samples": len(rates)}


def metadata(work, count, repeats):
    create, stat, unlink, mkdir, rmdir = [], [], [], [], []
    for _ in range(repeats):
        names = [work / f"m{i:06d}" for i in range(count)]

        def do_create():
            for name in names:
                os.close(os.open(name, os.O_CREAT | os.O_WRONLY | os.O_EXCL, 0o644))

        def do_stat():
            for name in names:
                os.stat(name)

        def do_unlink():
            for name in names:
                os.unlink(name)

        create.append(timed(do_create))
        stat.append(timed(do_stat))
        unlink.append(timed(do_unlink))
        dirs = [work / f"d{i:06d}" for i in range(count)]
        mkdir.append(timed(lambda: [os.mkdir(d) for d in dirs]))
        rmdir.append(timed(lambda: [os.rmdir(d) for d in dirs]))
    return {"create": summary(create, count), "stat": summary(stat, count), "unlink": summary(unlink, count),
            "mkdir": summary(mkdir, count), "rmdir": summary(rmdir, count)}


def small_files(work, count, size, repeats):
    payload = os.urandom(size)
    writes, reads = [], []
    for _ in range(repeats):
        names = [work / f"s{i:06d}" for i in range(count)]

        def do_write():
            for name in names:
                with open(name, "wb") as stream:
                    stream.write(payload)

        def do_read():
            for name in names:
                with open(name, "rb") as stream:
                    if len(stream.read()) != size:
                        raise RuntimeError(f"short read of {name}")

        writes.append(timed(do_write))
        for name in names:
            drop_cache(name)
        reads.append(timed(do_read))
        for name in names:
            os.unlink(name)
    return {"size": size, "count": count, "write": summary(writes, count), "read": summary(reads, count)}


def large_file(work, size, repeats, request=MiB):
    chunk = os.urandom(request)
    writes, reads = [], []
    path = work / f"large-{size}"
    for _ in range(repeats):
        def do_write():
            fd = os.open(path, os.O_CREAT | os.O_TRUNC | os.O_WRONLY, 0o644)
            try:
                remaining = size
                while remaining > 0:
                    remaining -= os.write(fd, chunk[: min(request, remaining)])
                os.fsync(fd)
            finally:
                os.close(fd)

        def do_read():
            fd = os.open(path, os.O_RDONLY)
            try:
                total = 0
                while True:
                    data = os.read(fd, request)
                    if not data:
                        break
                    total += len(data)
            finally:
                os.close(fd)
            if total != size:
                raise RuntimeError(f"read {total} of {size} bytes")

        writes.append(timed(do_write))
        drop_cache(path)
        reads.append(timed(do_read))
        os.unlink(path)
    return {"size": size, "write_MiBps": summary(writes, size / MiB), "read_MiBps": summary(reads, size / MiB)}


def main():
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--dir", required=True, type=Path, help="directory on the file system under test")
    parser.add_argument("--repeats", type=int, default=5)
    parser.add_argument("--metadata-files", type=int, default=2000)
    parser.add_argument("--small-files", type=int, default=1000)
    parser.add_argument("--small-size", type=int, default=4096)
    parser.add_argument("--large-sizes", default="1,16,128,512", help="MiB, comma separated")
    args = parser.parse_args()

    work = args.dir / f"fsbench-{uuid.uuid4().hex[:8]}"
    work.mkdir(parents=True)
    try:
        result = {
            "metadata": metadata(work, args.metadata_files, args.repeats),
            "small": small_files(work, args.small_files, args.small_size, args.repeats),
            "large": [large_file(work, int(s) * MiB, args.repeats) for s in args.large_sizes.split(",")],
        }
    finally:
        shutil.rmtree(work, ignore_errors=True)
    json.dump(result, sys.stdout)  # one line: the last line of the output
    print()


if __name__ == "__main__":
    main()
