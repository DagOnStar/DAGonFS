#!/usr/bin/env python3
"""Run normal Python filesystem operations against a mounted DAGonFS instance.

Set DAGONFS_MOUNTPOINT to an already-mounted DAGonFS directory. The suite never
starts or stops MPI/FUSE itself, so it is safe to use in CI jobs that own
mounting (see ``tests/mount.py`` for a harness that does both).

Each test documents the defect it guards against, so a failure points directly
at the filesystem behaviour that regressed.
"""

import errno
import os
from pathlib import Path
import shutil
import subprocess
import stat
import tempfile
import time
import unittest


MOUNTPOINT = os.environ.get("DAGONFS_MOUNTPOINT")

# Larger than one DAGonFS data block (128 KiB) times a typical rank count, so
# that the payload is scattered across every storage rank.
MULTI_BLOCK = 128 * 1024 * 9 + 777


def pattern(size, seed=0):
    return bytes((index * 31 + seed) % 251 for index in range(size))


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


if __name__ == "__main__":
    unittest.main()
