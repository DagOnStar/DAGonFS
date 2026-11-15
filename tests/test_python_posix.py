#!/usr/bin/env python3
"""Run normal Python filesystem operations against a mounted DAGonFS instance.

Set DAGONFS_MOUNTPOINT to an already-mounted DAGonFS directory. The suite never
starts or stops MPI/FUSE itself, so it is safe to use in CI jobs that own
mounting (see ``tests/mount.py`` for a harness that does both).

Each test documents the defect it guards against, so a failure points directly
at the filesystem behaviour that regressed.
"""

import ctypes
import ctypes.util
import errno
import fcntl
import os
from pathlib import Path
import select
import shutil
import socket
import subprocess
import stat
import struct
import sys
import tempfile
import threading
import time
import unittest


MOUNTPOINT = os.environ.get("DAGONFS_MOUNTPOINT")

# Larger than one DAGonFS data block (128 KiB) times a typical rank count, so
# that the payload is scattered across every storage rank.
MULTI_BLOCK = 128 * 1024 * 9 + 777


def pattern(size, seed=0):
    return bytes((index * 31 + seed) % 251 for index in range(size))


LOCK_HELPER = str(Path(__file__).resolve().parent / "lock_helper.py")


def helper(*args, **kwargs):
    """Run lock_helper.py to completion and return its output lines."""
    result = subprocess.run([sys.executable, LOCK_HELPER] + [str(a) for a in args], stdout=subprocess.PIPE,
                            stderr=subprocess.PIPE, universal_newlines=True, timeout=kwargs.get("timeout", 30))
    if result.returncode != 0:
        raise AssertionError("lock_helper.py {} failed: {}".format(args, result.stderr))
    return result.stdout.split()


def start_helper(*args):
    """Start lock_helper.py in the background (stdin and stdout are pipes)."""
    return subprocess.Popen([sys.executable, LOCK_HELPER] + [str(a) for a in args], stdin=subprocess.PIPE,
                            stdout=subprocess.PIPE, universal_newlines=True)


def finish(process, timeout=10.0):
    """Wait for a helper started by start_helper, close its pipes and return its exit status."""
    try:
        return process.wait(timeout)
    finally:
        for stream in (process.stdin, process.stdout):
            if stream:
                stream.close()


def read_line(process, timeout=10.0):
    """Next stdout line of a helper, or None if it prints nothing within timeout seconds."""
    ready, _, _ = select.select([process.stdout], [], [], timeout)
    return process.stdout.readline().strip() if ready else None


def fallocate(fd, mode, offset, length):
    libc = ctypes.CDLL(ctypes.util.find_library("c"), use_errno=True)
    libc.fallocate.argtypes = [ctypes.c_int, ctypes.c_int, ctypes.c_longlong, ctypes.c_longlong]
    if libc.fallocate(fd, mode, offset, length) != 0:
        code = ctypes.get_errno()
        raise OSError(code, os.strerror(code))


FALLOC_FL_KEEP_SIZE, FALLOC_FL_PUNCH_HOLE, FALLOC_FL_ZERO_RANGE = 0x01, 0x02, 0x10


@unittest.skipUnless(MOUNTPOINT, "set DAGONFS_MOUNTPOINT to run FUSE integration tests")
class PythonPosixIntegrationTests(unittest.TestCase):
    def setUp(self):
        self.mountpoint = Path(MOUNTPOINT)
        if not self.mountpoint.is_dir():
            self.skipTest(f"mountpoint is not available: {self.mountpoint}")
        self.workdir = Path(tempfile.mkdtemp(prefix="dagonfs-python-", dir=self.mountpoint))

    def tearDown(self):
        if hasattr(self, "workdir"):
            shutil.rmtree(self.workdir, ignore_errors=True)

    # ------------------------------------------------------------------ files
    def test_file_lifecycle_and_metadata(self):
        path = self.workdir / "payload.bin"
        payload = bytes(range(256)) * 32
        path.write_bytes(payload)
        with path.open("ab") as handle:
            handle.write(b"tail")
            handle.flush()
            os.fsync(handle.fileno())
        self.assertEqual(path.read_bytes(), payload + b"tail")

        os.chmod(path, 0o640)
        self.assertEqual(stat.S_IMODE(path.stat().st_mode), 0o640)
        self.assertTrue(stat.S_ISREG(path.stat().st_mode))
        with path.open("r+b") as handle:
            handle.truncate(100)
        self.assertEqual(path.stat().st_size, 100)
        self.assertEqual(path.read_bytes(), payload[:100])

    def test_write_only_append_preserves_content(self):
        # O_WRONLY|O_APPEND used to be treated as a truncation.
        path = self.workdir / "append.txt"
        path.write_bytes(b"first\n")
        fd = os.open(path, os.O_WRONLY | os.O_APPEND)
        try:
            os.write(fd, b"second\n")
        finally:
            os.close(fd)
        self.assertEqual(path.read_bytes(), b"first\nsecond\n")

    def test_repeated_multi_block_rewrites(self):
        # Rewriting a distributed file used to free pointers owned by other ranks.
        path = self.workdir / "rewrite.bin"
        for seed, size in enumerate((MULTI_BLOCK, MULTI_BLOCK // 3, MULTI_BLOCK * 2, 17)):
            data = pattern(size, seed)
            path.write_bytes(data)
            self.assertEqual(path.stat().st_size, size)
            self.assertEqual(path.read_bytes(), data)

    def test_truncate_grow_and_shrink_without_open(self):
        # Growing a file beyond its stored blocks used to index past the block list.
        path = self.workdir / "sparse.bin"
        path.write_bytes(b"abc")
        os.truncate(path, MULTI_BLOCK)
        data = path.read_bytes()
        self.assertEqual(len(data), MULTI_BLOCK)
        self.assertEqual(data[:3], b"abc")
        self.assertEqual(data[3:], bytes(MULTI_BLOCK - 3))
        os.truncate(path, 2)
        self.assertEqual(path.read_bytes(), b"ab")
        empty = self.workdir / "grown-from-nothing"
        empty.touch()
        os.truncate(empty, 4096)
        self.assertEqual(empty.read_bytes(), bytes(4096))

    def test_read_past_end_of_file(self):
        # A read starting beyond EOF used to reply twice to the same request.
        path = self.workdir / "short.txt"
        path.write_bytes(b"0123456789")
        fd = os.open(path, os.O_RDONLY)
        try:
            self.assertEqual(os.pread(fd, 10, 100), b"")
            self.assertEqual(os.pread(fd, 100, 5), b"56789")
        finally:
            os.close(fd)

    def test_partial_overwrite_in_place(self):
        path = self.workdir / "inplace.bin"
        original = pattern(MULTI_BLOCK)
        path.write_bytes(original)
        with path.open("r+b") as handle:
            handle.seek(200_000)
            handle.write(b"XYZ")
        expected = original[:200_000] + b"XYZ" + original[200_003:]
        self.assertEqual(path.read_bytes(), expected)

    def test_two_descriptors_on_the_same_file(self):
        # Closing one descriptor used to free the buffer shared by the other.
        path = self.workdir / "shared.bin"
        path.write_bytes(b"0" * 10)
        first = os.open(path, os.O_RDWR)
        second = os.open(path, os.O_RDWR)
        try:
            os.pwrite(first, b"AB", 0)
            os.close(first)
            first = None
            os.pwrite(second, b"CD", 8)
            self.assertEqual(os.pread(second, 10, 0), b"AB000000CD")
        finally:
            if first is not None:
                os.close(first)
            os.close(second)
        self.assertEqual(path.read_bytes(), b"AB000000CD")

    def test_touch_sets_current_time(self):
        # UTIME_NOW used to be ignored and timestamps were reset to the epoch.
        path = self.workdir / "touched"
        path.write_bytes(b"x")
        os.utime(path, (0, 0))
        self.assertEqual(int(path.stat().st_mtime), 0)
        before = time.time() - 5
        os.utime(path)
        self.assertGreater(path.stat().st_mtime, before)
        self.assertGreater(path.stat().st_atime, before)

    def test_mknod_regular_file_keeps_mode_and_owner(self):
        # mknod used to drop permission bits and swap uid/gid.
        path = self.workdir / "node"
        os.mknod(path, stat.S_IFREG | 0o640)
        info = path.stat()
        self.assertTrue(stat.S_ISREG(info.st_mode))
        self.assertEqual(stat.S_IMODE(info.st_mode), 0o640)
        self.assertEqual(info.st_uid, os.getuid())
        self.assertEqual(info.st_gid, os.getgid())
        path.write_bytes(b"content")
        self.assertEqual(path.read_bytes(), b"content")

    # ------------------------------------------------------------ directories
    def test_directory_link_rename_and_scandir(self):
        directory = self.workdir / "nested"
        directory.mkdir(mode=0o750)
        source = directory / "source"
        source.write_text("DAGonFS", encoding="utf-8")
        hard_link = directory / "hard-link"
        symbolic_link = directory / "symbolic-link"
        os.link(source, hard_link)
        # Point the symlink at a name that survives the rename below.
        os.symlink("hard-link", symbolic_link)
        renamed = directory / "renamed"
        source.rename(renamed)

        self.assertEqual(hard_link.read_text(encoding="utf-8"), "DAGonFS")
        self.assertEqual(symbolic_link.read_text(encoding="utf-8"), "DAGonFS")
        self.assertEqual(os.readlink(symbolic_link), "hard-link")
        self.assertEqual(sorted(entry.name for entry in os.scandir(directory)),
                         ["hard-link", "renamed", "symbolic-link"])
        self.assertEqual(os.stat(renamed).st_nlink, 2)

    def test_rename_overwrites_target_and_self_rename_is_noop(self):
        a = self.workdir / "a"
        b = self.workdir / "b"
        a.write_bytes(b"A")
        b.write_bytes(b"B")
        os.rename(a, b)
        self.assertFalse(a.exists())
        self.assertEqual(b.read_bytes(), b"A")
        os.rename(b, b)
        self.assertEqual(b.read_bytes(), b"A")

    def test_directory_rename_updates_link_counts(self):
        left = self.workdir / "left"
        right = self.workdir / "right"
        left.mkdir()
        right.mkdir()
        (left / "child").mkdir()
        self.assertEqual(left.stat().st_nlink, 3)
        os.rename(left / "child", right / "child")
        self.assertEqual(left.stat().st_nlink, 2)
        self.assertEqual(right.stat().st_nlink, 3)
        (right / "child" / "f").write_bytes(b"x")
        self.assertEqual((right / "child" / "f").read_bytes(), b"x")

    def test_posix_errors_for_directory_operations(self):
        directory = self.workdir / "dir"
        directory.mkdir()
        (directory / "file").write_bytes(b"x")
        with self.assertRaises(OSError) as raised:
            os.rmdir(directory)
        self.assertEqual(raised.exception.errno, errno.ENOTEMPTY)
        with self.assertRaises(OSError) as raised:
            os.unlink(directory)
        self.assertIn(raised.exception.errno, (errno.EISDIR, errno.EPERM))
        with self.assertRaises(FileExistsError):
            directory.mkdir()

    def test_large_directory_listing_and_recursive_removal(self):
        # readdir used heap iterators as offsets; deleting entries while
        # listing (rm -rf) dereferenced freed map nodes.
        directory = self.workdir / "many"
        directory.mkdir()
        names = {f"entry-{index:05d}-{'x' * 40}" for index in range(600)}
        for name in names:
            (directory / name).write_bytes(b"")
        self.assertEqual(set(os.listdir(directory)), names)
        shutil.rmtree(directory)
        self.assertFalse(directory.exists())

    # ----------------------------------------------------------------- xattrs
    def test_inode_numbers_identify_files(self):
        # D41: every file reported st_ino 0, so tools that compare (st_dev,
        # st_ino) - cat, cp, tar, find - took distinct files for the same one.
        first, second, alias = self.workdir / "first", self.workdir / "second", self.workdir / "alias"
        first.write_bytes(b"one")
        second.write_bytes(b"two")
        os.link(first, alias)
        self.assertNotEqual(first.stat().st_ino, 0)
        self.assertNotEqual(first.stat().st_ino, second.stat().st_ino)
        self.assertNotEqual(self.workdir.stat().st_ino, first.stat().st_ino)
        self.assertEqual(first.stat().st_ino, alias.stat().st_ino)
        self.assertTrue(os.path.samefile(first, alias))
        self.assertFalse(os.path.samefile(first, second))
        # The shell idiom that exposed the defect.
        result = subprocess.run(["sh", "-c", "cat first first > doubled"], cwd=str(self.workdir),
                                stdout=subprocess.PIPE, stderr=subprocess.PIPE, universal_newlines=True)
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertEqual((self.workdir / "doubled").read_bytes(), b"oneone")

    def test_exclusive_create_and_xattrs(self):
        path = self.workdir / "exclusive"
        path.touch(exist_ok=False)
        with self.assertRaises(FileExistsError):
            path.touch(exist_ok=False)

        try:
            os.setxattr(path, "user.dagonfs", b"value")
        except (AttributeError, OSError) as error:
            if isinstance(error, OSError) and error.errno not in (errno.ENOTSUP, errno.EOPNOTSUPP):
                raise
            self.skipTest("extended attributes are unavailable on this host")
        # getxattr with a buffer larger than the value used to fail with ERANGE.
        self.assertEqual(os.getxattr(path, "user.dagonfs"), b"value")
        self.assertIn("user.dagonfs", os.listxattr(path))
        os.removexattr(path, "user.dagonfs")
        self.assertEqual(os.listxattr(path), [])
        with self.assertRaises(OSError) as raised:
            os.getxattr(path, "user.dagonfs")
        self.assertEqual(raised.exception.errno, errno.ENODATA)
        # POSIX ACLs are not enforced, hence refused rather than silently stored.
        # A valid ACL (user::rw-, group::r--, other::r--): newer kernels turn an
        # ACL without entries into a removal, which never reaches DAGonFS.
        undefined = 0xFFFFFFFF
        acl = struct.pack("<I", 2) + b"".join(struct.pack("<HHI", tag, perm, undefined)
                                               for tag, perm in ((0x01, 6), (0x04, 4), (0x20, 4)))
        with self.assertRaises(OSError) as raised:
            os.setxattr(path, "system.posix_acl_access", acl)
        self.assertIn(raised.exception.errno, (errno.EOPNOTSUPP, errno.ENOTSUP))


    # ------------------------------------------------------------ locks
    def test_posix_record_locks_between_processes(self):
        # fcntl/lockf byte-range locks were not retained (getlk always
        # answered F_UNLCK, setlk was not implemented).
        path = self.workdir / "records"
        path.write_bytes(pattern(4096))
        fd = os.open(str(path), os.O_RDWR)
        try:
            fcntl.lockf(fd, fcntl.LOCK_EX, 100, 0, os.SEEK_SET)          # bytes 0-99
            self.assertEqual(helper("try", path, "posix", "ex", 50, 10), ["busy"])
            self.assertEqual(helper("try", path, "posix", "sh", 0, 1), ["busy"])
            self.assertEqual(helper("try", path, "posix", "ex", 100, 100), ["granted"])
            self.assertEqual(helper("getlk", path, 0, 0), ["wrlck", "0", "100"])
            self.assertEqual(helper("getlk", path, 500, 10)[0], "unlck")
            # Downgrade in place: readers may now share the range.
            fcntl.lockf(fd, fcntl.LOCK_SH, 100, 0, os.SEEK_SET)
            self.assertEqual(helper("try", path, "posix", "sh", 0, 100), ["granted"])
            self.assertEqual(helper("try", path, "posix", "ex", 0, 100), ["busy"])
            # A waiter is granted as soon as the range is unlocked.
            fcntl.lockf(fd, fcntl.LOCK_EX, 100, 0, os.SEEK_SET)
            waiter = start_helper("wait", path, "posix", "ex", 10, 10)
            self.assertEqual(read_line(waiter), "waiting")
            self.assertIsNone(read_line(waiter, 0.5), "the waiter must block while the range is locked")
            fcntl.lockf(fd, fcntl.LOCK_UN, 100, 0, os.SEEK_SET)
            self.assertEqual(read_line(waiter), "granted")
            self.assertEqual(finish(waiter), 0)
        finally:
            os.close(fd)

    def test_closing_any_descriptor_releases_record_locks(self):
        path = self.workdir / "close-release"
        path.write_bytes(b"x" * 10)
        first = os.open(str(path), os.O_RDWR)
        second = os.open(str(path), os.O_RDONLY)
        fcntl.lockf(first, fcntl.LOCK_EX)
        self.assertEqual(helper("try", path, "posix", "ex", 0, 0), ["busy"])
        os.close(second)  # POSIX: any close by the owner releases its locks on the file
        self.assertEqual(helper("try", path, "posix", "ex", 0, 0), ["granted"])
        os.close(first)

    def test_blocked_lock_is_interrupted_by_a_signal(self):
        path = self.workdir / "interrupt"
        path.write_bytes(b"x")
        fd = os.open(str(path), os.O_RDWR)
        try:
            fcntl.lockf(fd, fcntl.LOCK_EX)
            self.assertEqual(helper("interrupt", path, "posix", "ex", 0, 0), ["interrupted"])
            self.assertEqual(helper("interrupt", path, "flock", "ex", 0, 0), ["granted"])
            # The interrupted waiter was withdrawn: unlocking grants nothing to it.
            fcntl.lockf(fd, fcntl.LOCK_UN)
            self.assertEqual(helper("try", path, "posix", "ex", 0, 0), ["granted"])
        finally:
            os.close(fd)

    def test_lock_deadlock_is_detected(self):
        path = self.workdir / "deadlock"
        path.write_bytes(b"x" * 20)
        fd = os.open(str(path), os.O_RDWR)
        try:
            fcntl.lockf(fd, fcntl.LOCK_EX, 1, 0, os.SEEK_SET)
            peer = start_helper("deadlock", path)
            self.assertEqual(read_line(peer), "holding")
            time.sleep(0.5)  # let the peer queue for byte 0
            with self.assertRaises(OSError) as raised:
                fcntl.lockf(fd, fcntl.LOCK_EX, 1, 10, os.SEEK_SET)
            self.assertEqual(raised.exception.errno, errno.EDEADLK)
        finally:
            os.close(fd)
        self.assertEqual(read_line(peer), "granted")
        self.assertEqual(finish(peer), 0)

    def test_flock_between_processes(self):
        path = self.workdir / "flocked"
        path.write_bytes(b"x")
        with open(str(path), "rb") as stream:
            fcntl.flock(stream.fileno(), fcntl.LOCK_SH)
            self.assertEqual(helper("try", path, "flock", "sh", 0, 0), ["granted"])
            self.assertEqual(helper("try", path, "flock", "ex", 0, 0), ["busy"])
            # flock and POSIX locks are independent.
            self.assertEqual(helper("try", path, "posix", "ex", 0, 0), ["granted"])
            fcntl.flock(stream.fileno(), fcntl.LOCK_EX)
            waiter = start_helper("wait", path, "flock", "ex", 0, 0)
            self.assertEqual(read_line(waiter), "waiting")
            self.assertIsNone(read_line(waiter, 0.5))
        # The last close of the open file description releases the lock.
        self.assertEqual(read_line(waiter), "granted")
        self.assertEqual(finish(waiter), 0)

    def test_locked_read_modify_write_counter(self):
        # Four processes increment a counter under a lock: no update may be lost.
        counter = self.workdir / "counter"
        for kind in ("posix", "flock"):
            counter.write_bytes(b"0")
            workers = [start_helper("count", counter, 25, kind) for _ in range(4)]
            for worker in workers:
                self.assertEqual(read_line(worker, 120), "done")
                self.assertEqual(finish(worker), 0)
            self.assertEqual(int(counter.read_bytes()), 100, kind)

    # ------------------------------------------- space, ranges and copies
    def test_fallocate_extends_punches_and_zeroes(self):
        path = self.workdir / "allocated"
        fd = os.open(str(path), os.O_RDWR | os.O_CREAT, 0o644)
        try:
            os.posix_fallocate(fd, 0, MULTI_BLOCK)
            self.assertEqual(os.fstat(fd).st_size, MULTI_BLOCK)
            self.assertEqual(os.pread(fd, MULTI_BLOCK, 0), bytes(MULTI_BLOCK))
            payload = pattern(MULTI_BLOCK, 3)
            os.pwrite(fd, payload, 0)
            # Preallocation never shrinks nor changes content.
            os.posix_fallocate(fd, 10, 100)
            self.assertEqual(os.pread(fd, MULTI_BLOCK, 0), payload)
            if not sys.platform.startswith("linux"):
                return
            fallocate(fd, FALLOC_FL_PUNCH_HOLE | FALLOC_FL_KEEP_SIZE, 1000, 5000)
            expected = payload[:1000] + bytes(5000) + payload[6000:]
            self.assertEqual(os.pread(fd, MULTI_BLOCK, 0), expected)
            size = MULTI_BLOCK
            try:
                fallocate(fd, FALLOC_FL_ZERO_RANGE, MULTI_BLOCK - 10, 30)
            except OSError as error:
                # Older kernels (e.g. RHEL 8, 4.18) do not forward FALLOC_FL_ZERO_RANGE to FUSE.
                if error.errno not in (errno.EOPNOTSUPP, errno.ENOTSUP):
                    raise
            else:
                size = MULTI_BLOCK + 20
                self.assertEqual(os.fstat(fd).st_size, size)
                self.assertEqual(os.pread(fd, 40, MULTI_BLOCK - 20), expected[-20:-10] + bytes(30))
            fallocate(fd, FALLOC_FL_KEEP_SIZE, MULTI_BLOCK * 2, 4096)
            self.assertEqual(os.fstat(fd).st_size, size)
            with self.assertRaises(OSError) as raised:
                fallocate(fd, 0x08, 0, 4096)  # FALLOC_FL_COLLAPSE_RANGE
            self.assertIn(raised.exception.errno, (errno.EOPNOTSUPP, errno.ENOTSUP))
            self.assertEqual(os.pread(fd, 1000, 0), payload[:1000])
        finally:
            os.close(fd)
        self.assertGreaterEqual(path.stat().st_size, MULTI_BLOCK)

    def test_seek_data_and_hole(self):
        if not hasattr(os, "SEEK_DATA"):
            self.skipTest("SEEK_DATA is unavailable")
        path = self.workdir / "sparse-query"
        path.write_bytes(pattern(10000))
        fd = os.open(str(path), os.O_RDONLY)
        try:
            self.assertEqual(os.lseek(fd, 0, os.SEEK_DATA), 0)
            self.assertEqual(os.lseek(fd, 1234, os.SEEK_DATA), 1234)
            self.assertEqual(os.lseek(fd, 0, os.SEEK_HOLE), 10000)
            with self.assertRaises(OSError) as raised:
                os.lseek(fd, 10000, os.SEEK_DATA)
            self.assertEqual(raised.exception.errno, errno.ENXIO)
        finally:
            os.close(fd)

    def test_copy_file_range(self):
        if not hasattr(os, "copy_file_range"):
            self.skipTest("os.copy_file_range needs Python 3.8")
        source, target = self.workdir / "copy-source", self.workdir / "copy-target"
        payload = pattern(MULTI_BLOCK, 5)
        source.write_bytes(payload)
        target.write_bytes(b"head")
        with open(str(source), "rb") as src, open(str(target), "r+b") as dst:
            copied, offset = 0, 4
            while copied < len(payload):
                count = os.copy_file_range(src.fileno(), dst.fileno(), len(payload) - copied, copied, offset)
                self.assertGreater(count, 0)
                copied += count
                offset += count
        self.assertEqual(target.read_bytes(), b"head" + payload)

    def test_poll_and_ioctl_on_regular_files(self):
        path = self.workdir / "polled"
        path.write_bytes(b"ready")
        with open(str(path), "r+b") as stream:
            poller = select.poll()
            poller.register(stream.fileno(), select.POLLIN | select.POLLOUT)
            events = dict(poller.poll(1000))
            self.assertTrue(events[stream.fileno()] & select.POLLIN)
            self.assertTrue(events[stream.fileno()] & select.POLLOUT)
            if sys.platform.startswith("linux"):
                with self.assertRaises(OSError) as raised:
                    fcntl.ioctl(stream.fileno(), 0x80086601, bytes(8))  # FS_IOC_GETFLAGS
                self.assertEqual(raised.exception.errno, errno.ENOTTY)

    # -------------------------------------------------- special files
    def test_fifo(self):
        # mknod of anything but a regular file used to fail with EPERM.
        fifo = self.workdir / "pipe"
        os.mkfifo(str(fifo), 0o640)
        info = os.lstat(str(fifo))
        self.assertTrue(stat.S_ISFIFO(info.st_mode))
        self.assertEqual(stat.S_IMODE(info.st_mode), 0o640 & ~self._umask())
        payload = pattern(100000)

        def produce():
            with open(str(fifo), "wb") as stream:
                stream.write(payload)

        writer = threading.Thread(target=produce)
        writer.start()
        with open(str(fifo), "rb") as stream:
            received = stream.read()
        writer.join(10)
        self.assertEqual(received, payload)
        self.assertIn("pipe", os.listdir(str(self.workdir)))
        os.rename(str(fifo), str(self.workdir / "renamed-pipe"))
        os.unlink(str(self.workdir / "renamed-pipe"))

    def test_unix_domain_socket(self):
        address = str(self.workdir / "s")
        if len(address) >= 100 or not hasattr(socket, "AF_UNIX"):
            self.skipTest("socket path too long for sockaddr_un")
        server = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
        try:
            server.bind(address)
            server.listen(1)
            self.assertTrue(stat.S_ISSOCK(os.lstat(address).st_mode))
            client = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
            client.connect(address)
            connection, _ = server.accept()
            client.sendall(b"over dagonfs")
            self.assertEqual(connection.recv(64), b"over dagonfs")
            connection.close()
            client.close()
        finally:
            server.close()
        os.unlink(address)

    def test_device_nodes_need_privilege(self):
        if os.geteuid() == 0:
            self.skipTest("root may create device nodes")
        with self.assertRaises(PermissionError):
            os.mknod(str(self.workdir / "null"), stat.S_IFCHR | 0o666, os.makedev(1, 3))

    def test_scandir_attributes_match_stat(self):
        # readdirplus returns attributes with the names: they must agree with
        # lookup, and repeated listings must keep the lookup counts balanced.
        for round_ in range(3):
            for index in range(40):
                (self.workdir / "e{}".format(index)).write_bytes(b"x" * (index + round_))
            os.mkdir(str(self.workdir / "sub"))
            os.symlink("e0", str(self.workdir / "link"))
            for entry in os.scandir(str(self.workdir)):
                info = os.lstat(entry.path)
                self.assertEqual(entry.inode(), info.st_ino)
                self.assertEqual(entry.stat(follow_symlinks=False).st_size, info.st_size)
                self.assertEqual(entry.is_dir(follow_symlinks=False), stat.S_ISDIR(info.st_mode))
                self.assertEqual(entry.is_symlink(), stat.S_ISLNK(info.st_mode))
            listing = subprocess.run(["ls", "-l", str(self.workdir)], stdout=subprocess.PIPE,
                                     universal_newlines=True, check=True).stdout
            self.assertIn("link -> e0", listing)
            for index in range(40):
                (self.workdir / "e{}".format(index)).unlink()
            (self.workdir / "link").unlink()
            (self.workdir / "sub").rmdir()
        self.assertEqual(os.listdir(str(self.workdir)), [])

    @staticmethod
    def _umask():
        mask = os.umask(0)
        os.umask(mask)
        return mask

if __name__ == "__main__":
    unittest.main()
