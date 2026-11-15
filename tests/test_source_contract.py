#!/usr/bin/env python3
"""Dependency-free contract tests for code shared by both FUSE front ends.

These checks run without a compiler, MPI or FUSE, so they are cheap enough for
every CI job. They encode architectural rules from AGENTS.md that a compiler
cannot enforce.
"""

from pathlib import Path
import re
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

# Files that must be byte-identical in both trees: shared filesystem
# behaviour is implemented once and specialised with DAGONFS_P2P.
SHARED_FILES = (
    "ramfs/FileSystem.cpp",
    "ramfs/FileSystem.hpp",
    "nodes/File.cpp",
    "nodes/File.hpp",
    "nodes/Directory.cpp",
    "nodes/Directory.hpp",
    "nodes/INode.cpp",
    "nodes/INode.hpp",
    "nodes/Nodes.cpp",
    "nodes/Nodes.hpp",
    "blocks/Blocks.cpp",
    "blocks/Blocks.hpp",
    "blocks/DataBlock.cpp",
    "blocks/DataBlock.hpp",
    "mpi/DataBlockManager.cpp",
    "mpi/DataBlockManager.hpp",
    "utils/ArgumentParser.cpp",
    "utils/ArgumentParser.hpp",
)


def source(variant, relative):
    return (ROOT / "src" / variant / "include" / relative).read_text()


def handler_bodies(text):
    """Yield (name, body) for every FileSystem::Fuse* handler definition."""
    for match in re.finditer(r"\nvoid FileSystem::(Fuse\w+)\([^)]*\)[^{]*\{", text):
        start = match.end()
        depth, index = 1, start
        while depth:
            depth += {"{": 1, "}": -1}.get(text[index], 0)
            index += 1
        yield match.group(1), text[start:index - 1]


class FuseSourceContractTests(unittest.TestCase):
    def test_shared_sources_are_identical_in_both_trees(self):
        for relative in SHARED_FILES:
            with self.subTest(file=relative):
                self.assertEqual(source(VARIANTS[0], relative), source(VARIANTS[1], relative),
                                 f"{relative} diverged between the two trees")

    def test_required_callbacks_are_registered_and_defined(self):
        text = source(VARIANTS[0], "ramfs/FileSystem.cpp")
        for operation in REQUIRED_OPERATIONS:
            with self.subTest(operation=operation):
                self.assertIn(f"FuseOperations.{operation}", text)

    def test_every_error_reply_is_followed_by_return(self):
        # AGENTS.md: after fuse_reply_* on an error path, return immediately.
        text = source(VARIANTS[0], "ramfs/FileSystem.cpp")
        for name, body in handler_bodies(text):
            lines = [line.strip() for line in body.splitlines()]
            for index, line in enumerate(lines):
                if line.startswith("fuse_reply_") and index + 1 < len(lines):
                    following = next((l for l in lines[index + 1:] if l), "}")
                    if following in ("return;", "}", "#endif", "#else", "#ifdef DAGONFS_P2P"):
                        continue
                    with self.subTest(handler=name, line=line):
                        self.fail(f"{name}: '{line}' is followed by '{following}'")

    def test_getlk_always_replies(self):
        text = source(VARIANTS[0], "ramfs/FileSystem.cpp")
        start = text.index("void FileSystem::FuseGetLock")
        body = text[start:text.index("\n}\n", start)]
        self.assertIn("fuse_reply_lock(req, lock);", body)

    def test_batched_forget_replies_once(self):
        text = source(VARIANTS[0], "ramfs/FileSystem.cpp")
        start = text.index("void FileSystem::FuseForgetMulti")
        body = text[start:text.index("\n}\n", start)]
        self.assertEqual(body.count("fuse_reply_none(req);"), 1)

    def test_directory_creation_can_insert_a_new_name(self):
        text = source(VARIANTS[0], "nodes/Directory.cpp")
        start = text.index("fuse_ino_t Directory::UpdateChild")
        body = text[start:text.index("fuse_ino_t Directory::DeleteChild", start)]
        self.assertIn("m_children.emplace(name, ino);", body)

    def test_creation_handlers_reject_existing_names(self):
        text = source(VARIANTS[0], "ramfs/FileSystem.cpp")
        self.assertGreaterEqual(text.count("fuse_reply_err(req, EEXIST);"), 5)

    def test_readdir_offsets_are_not_pointers(self):
        text = source(VARIANTS[0], "ramfs/FileSystem.cpp")
        start = text.index("void FileSystem::FuseReadDir")
        body = text[start:text.index("\n}\n", start)]
        self.assertNotIn("const_iterator *", body)
        self.assertNotIn("(off_t) childIterator", body)

    def test_no_process_local_pointers_in_mpi_packets(self):
        # AGENTS.md: never transmit raw process-local pointers.
        for variant in VARIANTS:
            text = source(variant, "mpi/mpi_data.hpp")
            with self.subTest(variant=variant):
                self.assertNotIn("PointerPacket", text)
                self.assertIsNone(re.search(r"void\s*\*", text))

    def test_peer_requests_use_bounded_paths(self):
        text = source("peer-to-peer", "mpi/mpi_data.hpp")
        self.assertIn("DAGONFS_MAX_REQUEST_PATH = 4096", text)
        self.assertNotIn("[256]", text)
        sender = source("peer-to-peer", "mpi/RequestSender.cpp")
        self.assertIn("DAGONFS_MAX_REQUEST_PATH - 1", sender)
        listener = source("peer-to-peer", "mpi/DistributedCode.cpp")
        self.assertIn("firstLength >= DAGONFS_MAX_REQUEST_PATH", listener)
        self.assertIn("sizeof(PeerRequest) + firstLength + secondLength != static_cast<size_t>(count)", listener)

    def test_listener_receives_each_request_as_one_probed_message(self):
        text = source("peer-to-peer", "mpi/DistributedCode.cpp")
        start = text.index("void DistributedCode::start()")
        body = text[start:text.index("\n}\n", start)]
        self.assertEqual(body.count("MPI_ANY_SOURCE"), 1, "only the probe may match any source")
        self.assertIn("MPI_Get_count", body)
        self.assertIn("MPI_Recv(message.data(), count, MPI_BYTE, source", body)

    def test_block_registry_does_not_create_entries_for_read_only_queries(self):
        text = source(VARIANTS[0], "blocks/Blocks.cpp")
        self.assertIn("return FileSystemDataBlocks.find(inode) != FileSystemDataBlocks.end();", text)
        self.assertIn("blocks == FileSystemDataBlocks.end() ? 0", text)

    def test_data_blocks_own_their_memory(self):
        header = source(VARIANTS[0], "blocks/DataBlock.hpp")
        self.assertNotIn("setData", header, "blocks must not adopt foreign addresses")
        self.assertIn("DataBlock(const DataBlock &) = delete;", header)

    def test_launcher_does_not_use_a_shell(self):
        text = (ROOT / "src" / "DAGonFS_Launcher.cpp").read_text()
        self.assertNotIn("system(", text)
        self.assertIn("execvp(", text)

    def test_p2p_build_defines_the_model_macro(self):
        text = (ROOT / "CMakeLists.txt").read_text()
        self.assertRegex(text, r"target_compile_definitions\(\$\{PROJECT_NAME\}_P2P\.exe PRIVATE [^)]*DAGONFS_P2P")
        self.assertIn('add_dependencies("${PROJECT_NAME}_CS.exe" log4cplus)', text)


if __name__ == "__main__":
    unittest.main()
