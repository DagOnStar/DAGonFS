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
SRC = ROOT / "src"
REQUIRED_OPERATIONS = (
    "getattr", "lookup", "forget", "setattr", "readlink", "mknod", "mkdir",
    "unlink", "rmdir", "symlink", "rename", "link", "open", "read", "write",
    "flush", "release", "fsync", "opendir", "readdir", "releasedir", "fsyncdir",
    "statfs", "setxattr", "getxattr", "listxattr", "removexattr", "access",
    "create", "getlk", "setlk", "flock", "fallocate", "readdirplus", "poll",
    "ioctl", "copy_file_range", "lseek",
    "destroy", "forget_multi",
)


def source(relative):
    return (SRC / relative).read_text()


def all_sources():
    return sorted(p for p in SRC.rglob("*") if p.suffix in (".cpp", ".hpp"))


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
    def test_single_code_base_without_model_specific_trees(self):
        # Both deployment models are built from one tree and chosen at run time.
        self.assertFalse((SRC / "client-server").exists())
        self.assertFalse((SRC / "peer-to-peer").exists())
        for path in all_sources():
            with self.subTest(file=str(path.relative_to(ROOT))):
                self.assertNotIn("DAGONFS_P2P", path.read_text(), "the model is a run-time choice")

    def test_model_is_selected_at_run_time(self):
        options = source("volume/Options.cpp")
        self.assertIn('"peer-to-peer"', options)
        self.assertIn('"dagonfs_model"', options)
        self.assertIn('"--model"', options)
        volume = source("volume/Volume.cpp")
        self.assertIn("MPI_THREAD_MULTIPLE", volume)

    def test_required_callbacks_are_registered_and_defined(self):
        text = source("ramfs/FileSystem.cpp")
        for operation in REQUIRED_OPERATIONS:
            with self.subTest(operation=operation):
                self.assertIn(f"FuseOperations.{operation}", text)

    def test_every_error_reply_is_followed_by_return(self):
        # AGENTS.md: after fuse_reply_* on an error path, return immediately.
        text = source("ramfs/FileSystem.cpp")
        for name, body in handler_bodies(text):
            lines = [line.strip() for line in body.splitlines()]
            for index, line in enumerate(lines):
                if line.startswith("fuse_reply_") and index + 1 < len(lines):
                    following = next((l for l in lines[index + 1:] if l), "}")
                    if following in ("return;", "}", "#endif", "#else"):
                        continue
                    is_error = line.startswith("fuse_reply_err(") and not line.startswith("fuse_reply_err(req, 0)")
                    if following.startswith("if (peerToPeer") and not is_error:
                        # Replication to the other peers after a successful reply.
                        continue
                    with self.subTest(handler=name, line=line):
                        self.fail(f"{name}: '{line}' is followed by '{following}'")

    def test_locks_are_served_by_the_lock_table(self):
        # getlk used to answer F_UNLCK unconditionally and setlk/flock were
        # missing: every lock request now goes through the volume's table.
        text = source("ramfs/FileSystem.cpp")
        for handler in ("FuseGetLock", "FuseSetLock", "FuseFlock"):
            start = text.index("void FileSystem::" + handler)
            body = text[start:text.index("\n}\n", start)]
            with self.subTest(handler=handler):
                self.assertIn("SubmitLock(req, ", body)
        # A deferred reply is sent exactly once, by CompleteLockRequest.
        start = text.index("void FileSystem::CompleteLockRequest")
        body = text[start:text.index("\n}\n", start)]
        self.assertIn("pendingLocks.erase(entry);", body)
        # An interrupt never replies from inside libfuse's callback.
        start = text.index("void FileSystem::InterruptLock")
        body = text[start:text.index("\n}\n", start)]
        self.assertNotIn("fuse_reply", body)
        self.assertIn(".detach()", body)
        # close() releases record locks (flush) and flock locks (release).
        for handler, call in (("FuseFlush", "LOCK_RELEASE"), ("FuseRelease", "flock_release")):
            start = text.index("void FileSystem::" + handler)
            body = text[start:text.index("\n}\n", start)]
            with self.subTest(handler=handler):
                self.assertIn(call, body)
        self.assertIn("FUSE_CAP_POSIX_LOCKS", text)
        self.assertIn("FUSE_CAP_FLOCK_LOCKS", text)

    def test_lock_requests_are_served_by_the_listener_without_system_calls(self):
        listener = source("mpi/DistributedCode.cpp")
        start = listener.index("void DistributedCode::start()")
        body = listener[start:listener.index("\n}\n", start)]
        self.assertIn("FileSystem::ServeLockRequest(source, request);", body)
        self.assertIn("FileSystem::CompleteLockRequest(request.lock);", body)
        table = source("locks/LockTable.cpp")
        for call in ("fuse_", "MPI_", "open(", "fcntl("):
            with self.subTest(call=call):
                self.assertNotIn(call, table, "the lock table is pure bookkeeping")

    def test_special_files_are_created_and_replicated(self):
        text = source("ramfs/FileSystem.cpp")
        start = text.index("void FileSystem::FuseMknod")
        body = text[start:text.index("\n}\n", start)]
        self.assertIn("S_ISFIFO(mode)", body)
        self.assertIn("sendMknodRequest(", body)
        self.assertNotIn("EPERM", body)

    def test_batched_forget_replies_once(self):
        text = source("ramfs/FileSystem.cpp")
        start = text.index("void FileSystem::FuseForgetMulti")
        body = text[start:text.index("\n}\n", start)]
        self.assertEqual(body.count("fuse_reply_none(req);"), 1)

    def test_directory_creation_can_insert_a_new_name(self):
        text = source("nodes/Directory.cpp")
        start = text.index("fuse_ino_t Directory::UpdateChild")
        body = text[start:text.index("fuse_ino_t Directory::DeleteChild", start)]
        self.assertIn("m_children.emplace(name, ino);", body)

    def test_creation_handlers_reject_existing_names(self):
        text = source("ramfs/FileSystem.cpp")
        self.assertGreaterEqual(text.count("fuse_reply_err(req, EEXIST);"), 5)

    def test_readdir_offsets_are_not_pointers(self):
        text = source("ramfs/FileSystem.cpp")
        start = text.index("void FileSystem::FuseReadDir")
        body = text[start:text.index("\n}\n", start)]
        self.assertNotIn("const_iterator *", body)
        self.assertNotIn("(off_t) childIterator", body)

    def test_no_process_local_pointers_in_mpi_packets(self):
        # AGENTS.md: never transmit raw process-local pointers.
        text = source("mpi/mpi_data.hpp")
        self.assertNotIn("PointerPacket", text)
        self.assertIsNone(re.search(r"void\s*\*", text))

    def test_peer_requests_use_bounded_paths(self):
        text = source("mpi/mpi_data.hpp")
        self.assertIn("DAGONFS_MAX_REQUEST_PATH = 4096", text)
        self.assertNotIn("[256]", text)
        sender = source("mpi/RequestSender.cpp")
        self.assertIn("DAGONFS_MAX_REQUEST_PATH - 1", sender)
        listener = source("mpi/DistributedCode.cpp")
        self.assertIn("firstLength >= DAGONFS_MAX_REQUEST_PATH", listener)
        self.assertIn("sizeof(PeerRequest) + firstLength + secondLength + valueLength != static_cast<size_t>(count)", listener)

    def test_listener_receives_each_request_as_one_probed_message(self):
        text = source("mpi/DistributedCode.cpp")
        start = text.index("void DistributedCode::start()")
        body = text[start:text.index("\n}\n", start)]
        self.assertIn("waitForRequest(status);", body)
        self.assertNotIn("MPI_Probe(", text, "a blocking probe starves the FUSE thread under MPI_THREAD_MULTIPLE")
        self.assertEqual(text.count("MPI_ANY_SOURCE"), 1, "only the probe may match any source")
        self.assertIn("MPI_Get_count", body)
        self.assertIn("MPI_Recv(message.data(), count, MPI_BYTE, source", body)

    def test_replays_are_identified_per_request_and_never_block_the_listener(self):
        text = source("ramfs/FileSystem.cpp")
        self.assertEqual(text.count("const bool replay = IsReplay(req, "), 11)
        self.assertIn("fuse_req_ctx(req)->pid", text)
        listener = source("mpi/DistributedCode.cpp")
        start = listener.index("void DistributedCode::start()")
        body = listener[start:listener.index("\n}\n", start)]
        for call in ("createFile(", "deleteFile(", "createDir(", "deleteDir(", "renameHandler(", "linkHandler(",
                     "symlinkHandler(", "setAttrHandler(", "setXAttrHandler(", "removeXAttrHandler(",
                     "mknodHandler("):
            with self.subTest(call=call):
                self.assertNotIn(call, body, "the listener must queue replays, not perform them")

    def test_peer_to_peer_replicates_metadata(self):
        # Symbolic links, setattr and extended attributes are replayed on every
        # peer, with bounded values and absolute (originator-resolved) times.
        text = source("ramfs/FileSystem.cpp")
        for call in ("sendSymlinkRequest(", "sendSetAttrRequest(", "sendSetXAttrRequest(", "sendRemoveXAttrRequest("):
            with self.subTest(call=call):
                self.assertIn(call, text)
        listener = source("mpi/DistributedCode.cpp")
        self.assertIn("valueLength > DAGONFS_MAX_REQUEST_VALUE", listener)
        self.assertIn("DAGONFS_MAX_REQUEST_VALUE = 65536", source("mpi/mpi_data.hpp"))

    def test_data_plane_uses_no_collectives_on_the_shared_communicator(self):
        # Peers originate transfers concurrently; concurrent collectives on one
        # communicator are erroneous MPI, so the data plane is point-to-point.
        for relative in ("mpi/DataBlockManager.cpp", "mpi/DistributedCode.cpp", "mpi/RequestSender.cpp"):
            text = source(relative)
            with self.subTest(file=relative):
                self.assertIsNone(re.search(r"MPI_(Scatterv|Gatherv|Bcast|Barrier|Allreduce)\(", text))
        self.assertIn("DAGONFS_TAG_ACK", source("mpi/DistributedCode.cpp"))

    def test_collective_library_is_disabled_before_mpi_init(self):
        # D47: HCOLL's teardown deadlocked MPI_Finalize; DAGonFS uses no collectives.
        text = source("main.cpp")
        setting = text.find('setenv("OMPI_MCA_coll_hcoll_enable", "0", 0)')
        self.assertGreater(setting, 0)
        self.assertLess(setting, text.find("MPI_Init_thread("))

    def test_block_registry_does_not_create_entries_for_read_only_queries(self):
        text = source("blocks/Blocks.cpp")
        self.assertIn("return FileSystemDataBlocks.find(inode) != FileSystemDataBlocks.end();", text)
        self.assertIn("blocks == FileSystemDataBlocks.end() ? 0", text)

    def test_data_blocks_own_their_memory(self):
        header = source("blocks/DataBlock.hpp")
        self.assertNotIn("setData", header, "blocks must not adopt foreign addresses")
        self.assertIn("DataBlock(const DataBlock &) = delete;", header)

    def test_launcher_does_not_use_a_shell(self):
        text = (ROOT / "src" / "DAGonFS_Launcher.cpp").read_text()
        self.assertNotIn("system(", text)
        self.assertIn("execvp(", text)

    def test_build_produces_one_executable_and_waits_for_log4cplus(self):
        text = (ROOT / "CMakeLists.txt").read_text()
        self.assertIn("add_executable(dagonfs src/main.cpp)", text)
        self.assertIn("add_dependencies(dagonfs_core log4cplus)", text)
        self.assertNotIn("DAGONFS_P2P", text)

    def test_native_frontend_survives_dead_clients_and_keeps_connections(self):
        server = source("frontends/native/NativeFrontend.cpp")
        client = (SRC / "frontends/native/Client.c").read_text()
        # A client that disappears must not raise SIGPIPE in the volume or the
        # application, and a connection carries many requests.
        self.assertIn("signal(SIGPIPE, SIG_IGN)", server)
        self.assertIn("MSG_NOSIGNAL", server)
        self.assertIn("MSG_NOSIGNAL", client)
        self.assertIn("poll(", server)
        self.assertNotRegex(server, r"\bwrite\(client")
        # Handles opened on a connection are written back when it ends.
        self.assertIn("connection == client", server)
        # A forked child never shares its parent's stream.
        self.assertIn("connection_owner == getpid()", client)
        # Native and preload volumes are client-server only.
        self.assertIn("frontend != Frontend::Fuse && options.model == Model::PeerToPeer",
                      source("volume/Volume.cpp"))

    def test_preload_descriptors_are_real_and_interposition_is_portable(self):
        preload = (SRC / "frontends/preload/dagonfs_preload.c").read_text()
        # Placeholder descriptors come from the kernel, so they cannot collide.
        self.assertIn('"/dev/null"', preload)
        self.assertIn("__DATA,__interpose", preload)
        self.assertIn("RTLD_NEXT", preload)
        for name in ("open", "close", "read", "write", "pread", "pwrite", "lseek"):
            self.assertIn(f"INTERPOSE({name});", preload)

    def test_main_treats_argv_as_read_only(self):
        # AGENTS.md: argv is immutable; FUSE receives an owned copy.
        self.assertIsNone(re.search(r"argv\[[^\]]*\]\s*=[^=]", source("main.cpp")))
        self.assertIn("const char *const argv[]", source("volume/Options.hpp"))
        self.assertIn("parser.copy_args", source("ramfs/FileSystem.cpp"))


if __name__ == "__main__":
    unittest.main()
