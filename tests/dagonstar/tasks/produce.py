#!/usr/bin/env python3
"""Producer task (A): write the input parts and the shared coordination files.

POSIX calls exercised on the volume: mkdir, open(O_CREAT|O_EXCL),
posix_fallocate, pwrite, fsync, setxattr, chmod, utime, rename (atomic
publication), symlink, link and mkfifo-free shared files used later under locks.
"""

import argparse
import json
import os

import common


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--shared", required=True, help="shared directory on this task's peer")
    parser.add_argument("--parts", type=int, required=True)
    parser.add_argument("--size", type=int, required=True)
    args = parser.parse_args()

    os.makedirs("data")
    manifest = {"parts": args.parts, "size": args.size, "sha256": []}
    for index in range(args.parts):
        payload = common.content(index, args.size)
        temporary = os.path.join("data", "part{}.bin.tmp".format(index))
        fd = os.open(temporary, os.O_CREAT | os.O_EXCL | os.O_WRONLY, 0o644)
        try:
            # Preallocate, then fill in two halves written out of order.
            os.posix_fallocate(fd, 0, len(payload))
            half = len(payload) // 2
            os.pwrite(fd, payload[half:], half)
            os.pwrite(fd, payload[:half], 0)
            os.fsync(fd)
        finally:
            os.close(fd)
        digest = common.sha256_bytes(payload)
        common.check(common.sha256_file(temporary) == digest, "part {} reads back differently".format(index))
        common.set_digest(temporary, digest)
        os.chmod(temporary, 0o444)
        os.utime(temporary, ns=(1_700_000_000_000_000_000, 1_700_000_000_000_000_000 + index))
        final = os.path.join("data", "part{}.bin".format(index))
        os.rename(temporary, final)
        manifest["sha256"].append(digest)

    # Publish the manifest atomically.
    with open("manifest.json.tmp", "w") as stream:
        json.dump(manifest, stream)
        stream.flush()
        os.fsync(stream.fileno())
    os.replace("manifest.json.tmp", "manifest.json")
    os.symlink("data", "latest")
    common.check(os.path.realpath("latest/part0.bin") == os.path.realpath("data/part0.bin"), "symlink")

    # Shared area: hard links to the inputs (visible on every peer), the
    # ledger appended under record locks and the counter updated under flock.
    inputs = os.path.join(args.shared, "inputs")
    os.makedirs(inputs)
    for index in range(args.parts):
        os.link(os.path.join("data", "part{}.bin".format(index)), os.path.join(inputs, "part{}.bin".format(index)))
        common.check(os.stat(os.path.join(inputs, "part{}.bin".format(index))).st_nlink == 2, "link count")
    with open(os.path.join(args.shared, "counter"), "wb") as stream:
        stream.write(b"0")
    open(os.path.join(args.shared, "ledger.txt"), "wb").close()
    common.locked_append(os.path.join(args.shared, "ledger.txt"), "A {}".format(args.parts))
    print("produced {} parts of {} bytes".format(args.parts, args.size))


if __name__ == "__main__":
    main()
