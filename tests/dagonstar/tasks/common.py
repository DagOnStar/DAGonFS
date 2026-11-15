"""Helpers shared by the task scripts of the DAGonStar workflow test.

The scripts are copied onto the DAGonFS volume and executed from it, so this
module is also imported from the volume.
"""

import fcntl
import hashlib
import os
import sys
import time

DIGEST_XATTR = "user.dagonfs.sha256"


def fail(message):
    sys.stderr.write("FAILED: " + message + "\n")
    sys.exit(1)


def check(condition, message):
    if not condition:
        fail(message)


def content(index, size):
    """Deterministic, incompressible payload of part ``index``."""
    blocks = []
    produced = 0
    counter = 0
    while produced < size:
        block = hashlib.sha256("{}-{}".format(index, counter).encode()).digest()
        blocks.append(block)
        produced += len(block)
        counter += 1
    return b"".join(blocks)[:size]


def sha256_bytes(data):
    return hashlib.sha256(data).hexdigest()


def sha256_file(path):
    digest = hashlib.sha256()
    with open(path, "rb") as stream:
        for chunk in iter(lambda: stream.read(1 << 20), b""):
            digest.update(chunk)
    return digest.hexdigest()


def wait_for_path(path, timeout=60.0):
    """Peer-to-peer: names created on another peer appear asynchronously."""
    deadline = time.monotonic() + timeout
    while not os.path.exists(path):
        if time.monotonic() > deadline:
            fail("{} did not appear within {} s".format(path, timeout))
        time.sleep(0.05)


def locked_append(path, line):
    """Append one line under an exclusive POSIX record lock (fcntl/lockf)."""
    with open(path, "ab") as stream:
        fcntl.lockf(stream.fileno(), fcntl.LOCK_EX)
        stream.write((line + "\n").encode())
        stream.flush()
    # close() wrote the line back to the volume, then released the lock.


def locked_read(path):
    """Read a whole file under a shared POSIX record lock."""
    with open(path, "rb") as stream:
        fcntl.lockf(stream.fileno(), fcntl.LOCK_SH)
        return stream.read()


def locked_increment(path):
    """Increment a decimal counter under an exclusive flock lock; return the new value."""
    with open(path, "r+b") as stream:
        fcntl.flock(stream.fileno(), fcntl.LOCK_EX)
        value = int(stream.read().decode() or "0") + 1
        stream.seek(0)
        stream.write(str(value).encode())
        stream.truncate()
        stream.flush()
        os.fsync(stream.fileno())
        return value


def set_digest(path, digest):
    if hasattr(os, "setxattr"):
        os.setxattr(path, DIGEST_XATTR, digest.encode())


def get_digest(path):
    if hasattr(os, "getxattr"):
        return os.getxattr(path, DIGEST_XATTR).decode()
    return None
