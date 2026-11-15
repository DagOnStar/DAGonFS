#!/usr/bin/env python3
"""Parallel task (B<i>): verify one part, transform it and record the result.

POSIX calls exercised on the volume: read through a staged copy and through
another peer's view, getxattr, copy_file_range, lseek(SEEK_DATA/SEEK_HOLE),
mmap, mkfifo (the transformed bytes travel through a FIFO), setxattr, and the
shared ledger and counter updated under fcntl and flock locks.
"""

import argparse
import json
import mmap
import os
import shutil
import threading

import common


def copy_with_copy_file_range(source, target):
    with open(source, "rb") as src, open(target, "wb") as dst:
        size = os.fstat(src.fileno()).st_size
        if not hasattr(os, "copy_file_range"):
            shutil.copyfileobj(src, dst)
            return
        copied = 0
        while copied < size:
            count = os.copy_file_range(src.fileno(), dst.fileno(), size - copied, copied, copied)
            common.check(count > 0, "copy_file_range stopped at {} of {} bytes".format(copied, size))
            copied += count


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--index", type=int, required=True)
    parser.add_argument("--input", required=True, help="part staged by DAGonStar (workflow:// reference)")
    parser.add_argument("--manifest", required=True)
    parser.add_argument("--shared", required=True, help="shared directory seen through this task's peer")
    args = parser.parse_args()

    with open(args.manifest) as stream:
        manifest = json.load(stream)
    expected = manifest["sha256"][args.index]

    # The staged input and the producer's file seen through this task's peer.
    common.check(common.sha256_file(args.input) == expected, "staged input differs from the manifest")
    peer_view = os.path.join(args.shared, "inputs", "part{}.bin".format(args.index))
    common.wait_for_path(peer_view)
    common.check(common.sha256_file(peer_view) == expected, "the peer's view of the input differs")
    digest = common.get_digest(peer_view)
    common.check(digest in (None, expected), "the digest xattr did not reach this peer")

    copy_with_copy_file_range(args.input, "copy.bin")
    size = os.stat("copy.bin").st_size
    common.check(size == manifest["size"], "copy has {} bytes".format(size))
    with open("copy.bin", "rb") as stream:
        if hasattr(os, "SEEK_DATA"):
            common.check(os.lseek(stream.fileno(), 0, os.SEEK_DATA) == 0, "SEEK_DATA")
            common.check(os.lseek(stream.fileno(), 0, os.SEEK_HOLE) == size, "SEEK_HOLE")
        with mmap.mmap(stream.fileno(), 0, access=mmap.ACCESS_READ) as mapped:
            original = bytes(mapped)
    common.check(common.sha256_bytes(original) == expected, "mmap view differs from the input")

    # The transformation (byte reversal) is streamed through a FIFO.
    os.mkfifo("pipe")

    def feed():
        with open("pipe", "wb") as fifo:
            fifo.write(original[::-1])

    feeder = threading.Thread(target=feed)
    feeder.start()
    with open("pipe", "rb") as fifo, open("out.bin", "wb") as output:
        shutil.copyfileobj(fifo, output)
    feeder.join()
    os.unlink("pipe")

    result = common.sha256_file("out.bin")
    common.set_digest("out.bin", result)
    common.locked_append(os.path.join(args.shared, "ledger.txt"), "B{} {}".format(args.index, result))
    value = common.locked_increment(os.path.join(args.shared, "counter"))
    print("part {}: transformed, counter now {}".format(args.index, value))


if __name__ == "__main__":
    main()
