#!/usr/bin/env python3
"""Dependency-free contract tests for code shared by both FUSE front ends."""

from pathlib import Path
import unittest


ROOT = Path(__file__).resolve().parents[1]
VARIANTS = ("client-server", "peer-to-peer")
REQUIRED_OPERATIONS = (
    "getattr", "lookup", "forget", "setattr", "readlink", "mknod", "mkdir",
    "unlink", "rmdir", "symlink", "rename", "link", "open", "read", "write",
    "flush", "release", "fsync", "opendir", "readdir", "releasedir", "fsyncdir",
    "statfs", "setxattr", "getxattr", "listxattr", "removexattr", "access",
    "create", "getlk",
    "destroy", "forget_multi",
)


class FuseSourceContractTests(unittest.TestCase):
    def test_required_callbacks_are_registered_and_defined(self):
        for variant in VARIANTS:
            source = (ROOT / "src" / variant / "include/ramfs/FileSystem.cpp").read_text()
            for operation in REQUIRED_OPERATIONS:
                with self.subTest(variant=variant, operation=operation):
                    self.assertIn(f"FuseOperations.{operation}", source)

    def test_getlk_always_replies(self):
        for variant in VARIANTS:
            source = (ROOT / "src" / variant / "include/ramfs/FileSystem.cpp").read_text()
            start = source.index("void FileSystem::FuseGetLock")
            body = source[start:source.index("\n/**", start)]
            self.assertIn("fuse_reply_lock(req, lock);", body)
            self.assertNotIn("TODO: implement locking", body)

    def test_batched_forget_replies_once(self):
        for variant in VARIANTS:
            source = (ROOT / "src" / variant / "include/ramfs/FileSystem.cpp").read_text()
            start = source.index("void FileSystem::FuseForgetMulti")
            body = source[start:source.index("\nvoid FileSystem::FuseSetAttr", start)]
            self.assertIn("fuse_reply_none(req);", body)

    def test_directory_creation_can_insert_a_new_name(self):
        for variant in VARIANTS:
            source = (ROOT / "src" / variant / "include/nodes/Directory.cpp").read_text()
            start = source.index("fuse_ino_t Directory::UpdateChild")
            body = source[start:source.index("fuse_ino_t Directory::DeleteChild", start)]
            self.assertIn("m_children.emplace(name, ino);", body)

    def test_creation_handlers_reject_existing_names(self):
        for variant in VARIANTS:
            source = (ROOT / "src" / variant / "include/ramfs/FileSystem.cpp").read_text()
            self.assertGreaterEqual(source.count("fuse_reply_err(req, EEXIST);"), 4)

    def test_block_registry_does_not_create_entries_for_read_only_queries(self):
        for variant in VARIANTS:
            source = (ROOT / "src" / variant / "include/blocks/Blocks.cpp").read_text()
            self.assertIn("return FileSystemDataBlocks.find(inode) != FileSystemDataBlocks.end();", source)
            self.assertIn("blocks == FileSystemDataBlocks.end() ? 0", source)


if __name__ == "__main__":
    unittest.main()
