#!/usr/bin/env python3
"""Run normal Python filesystem operations against a mounted DAGonFS instance.

Set DAGONFS_MOUNTPOINT to an already-mounted DAGonFS directory. The suite never
starts or stops MPI/FUSE itself, so it is safe to use in CI jobs that own mounting.
"""

import errno
import os
from pathlib import Path
import shutil
import stat
import tempfile
import unittest


MOUNTPOINT = os.environ.get("DAGONFS_MOUNTPOINT")


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
        with path.open("r+b") as handle:
            handle.truncate(100)
        self.assertEqual(path.stat().st_size, 100)

    def test_directory_link_rename_and_scandir(self):
        directory = self.workdir / "nested"
        directory.mkdir(mode=0o750)
        source = directory / "source"
        source.write_text("DAGonFS", encoding="utf-8")
        hard_link = directory / "hard-link"
        symbolic_link = directory / "symbolic-link"
        os.link(source, hard_link)
        os.symlink("source", symbolic_link)
        renamed = directory / "renamed"
        source.rename(renamed)

        self.assertEqual(hard_link.read_text(encoding="utf-8"), "DAGonFS")
        self.assertEqual(symbolic_link.read_text(encoding="utf-8"), "DAGonFS")
        self.assertEqual(sorted(entry.name for entry in os.scandir(directory)),
                         ["hard-link", "renamed", "symbolic-link"])

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
        self.assertEqual(os.getxattr(path, "user.dagonfs"), b"value")
        self.assertIn("user.dagonfs", os.listxattr(path))
        os.removexattr(path, "user.dagonfs")


if __name__ == "__main__":
    unittest.main()
