#!/usr/bin/env python3
"""Functional test of the native and LD_PRELOAD frontends.

Starts ``mpirun -np RANKS dagonfs --frontend native SOCKET`` (no FUSE needed),
drives the volume through the C client library with ctypes, runs a C program
under the preload interposer, then shuts the volume down and checks that every
rank exits cleanly. Extra arguments after ``--`` are passed to ``mpirun``::

    python3 tests/native_frontend.py --build-dir build --ranks 2 -- --oversubscribe
"""
import argparse
import ctypes
import errno
import os
import shutil
import subprocess
import sys
import tempfile
import time
from pathlib import Path

PRELOAD_PROGRAM = r"""
#include <errno.h>
#include <fcntl.h>
#include <stdio.h>
#include <string.h>
#include <sys/stat.h>
#include <unistd.h>
#define CHECK(c) do { if (!(c)) { fprintf(stderr, "line %d: %s (errno %d)\n", __LINE__, #c, errno); return 1; } } while (0)
int main(int argc, char **argv) {
    char path[512], moved[512], dir[512], buffer[64];
    snprintf(dir, sizeof dir, "%s/pre", argv[1]);
    snprintf(path, sizeof path, "%s/pre/a.txt", argv[1]);
    snprintf(moved, sizeof moved, "%s/pre/b.txt", argv[1]);
    CHECK(mkdir(dir, 0755) == 0);
    int fd = open(path, O_CREAT | O_RDWR | O_TRUNC, 0640);
    CHECK(fd >= 0);
    CHECK(write(fd, "hello ", 6) == 6);
    CHECK(write(fd, "world", 5) == 5);
    CHECK(lseek(fd, 0, SEEK_CUR) == 11);
    CHECK(lseek(fd, 0, SEEK_END) == 11);
    CHECK(lseek(fd, 0, SEEK_SET) == 0);
    memset(buffer, 0, sizeof buffer);
    CHECK(read(fd, buffer, 5) == 5 && memcmp(buffer, "hello", 5) == 0);
    CHECK(read(fd, buffer, sizeof buffer) == 6 && memcmp(buffer, " world", 6) == 0);
    CHECK(read(fd, buffer, sizeof buffer) == 0);
    CHECK(pwrite(fd, "W", 1, 6) == 1);
    CHECK(pread(fd, buffer, 11, 0) == 11 && memcmp(buffer, "hello World", 11) == 0);
    CHECK(fsync(fd) == 0);
    CHECK(close(fd) == 0);
    fd = open(path, O_WRONLY | O_APPEND);
    CHECK(fd >= 0);
    CHECK(write(fd, "!", 1) == 1);
    CHECK(close(fd) == 0);
    CHECK(rename(path, moved) == 0);
    CHECK(open(path, O_RDONLY) < 0 && errno == ENOENT);
    fd = open(moved, O_RDONLY);
    CHECK(fd >= 0);
    CHECK(read(fd, buffer, sizeof buffer) == 12 && memcmp(buffer, "hello World!", 12) == 0);
    CHECK(write(fd, "x", 1) < 0 && errno == EBADF);
    CHECK(close(fd) == 0);
    CHECK(rmdir(dir) < 0 && errno == ENOTEMPTY);
    CHECK(unlink(moved) == 0);
    CHECK(rmdir(dir) == 0);
    /* Paths outside the root still reach the operating system. */
    fd = open(argv[2], O_CREAT | O_WRONLY | O_TRUNC, 0600);
    CHECK(fd >= 0 && write(fd, "local", 5) == 5 && close(fd) == 0);
    puts("preload ok");
    return 0;
}
"""


class Client:
    def __init__(self, library):
        lib = ctypes.CDLL(str(library), use_errno=True)
        lib.dagonfs_client_set_socket.argtypes = [ctypes.c_char_p]
        lib.dagonfs_client_open.argtypes = [ctypes.c_char_p, ctypes.c_int, ctypes.c_uint]
        lib.dagonfs_client_open.restype = ctypes.c_int64
        for name in ("pread", "pwrite"):
            fn = getattr(lib, "dagonfs_client_" + name)
            fn.argtypes = [ctypes.c_int64, ctypes.c_void_p, ctypes.c_size_t, ctypes.c_int64]
            fn.restype = ctypes.c_ssize_t
        for name in ("close", "fsync"):
            getattr(lib, "dagonfs_client_" + name).argtypes = [ctypes.c_int64]
        for name in ("unlink", "rmdir"):
            getattr(lib, "dagonfs_client_" + name).argtypes = [ctypes.c_char_p]
        lib.dagonfs_client_mkdir.argtypes = [ctypes.c_char_p, ctypes.c_uint]
        lib.dagonfs_client_rename.argtypes = [ctypes.c_char_p, ctypes.c_char_p]
        lib.dagonfs_client_list.argtypes = [ctypes.c_char_p, ctypes.c_char_p, ctypes.c_size_t]
        lib.dagonfs_client_list.restype = ctypes.c_ssize_t
        self.lib = lib

    def call(self, name, *args):
        result = getattr(self.lib, "dagonfs_client_" + name)(*args)
        if result < 0:
            raise OSError(ctypes.get_errno(), f"{name}{args!r}")
        return result

    def expect_error(self, code, name, *args):
        try:
            self.call(name, *args)
        except OSError as error:
            assert error.errno == code, (name, args, error)
            return
        raise AssertionError(f"{name}{args!r} succeeded, expected {errno.errorcode[code]}")

    def write(self, handle, data, offset):
        buffer = ctypes.create_string_buffer(data, len(data))
        return self.call("pwrite", handle, buffer, len(data), offset)

    def read(self, handle, size, offset):
        buffer = ctypes.create_string_buffer(size)
        count = self.call("pread", handle, buffer, size, offset)
        return buffer.raw[:count]

    def list(self, path):
        buffer = ctypes.create_string_buffer(4096)
        count = self.call("list", path, buffer, len(buffer))
        return sorted(name for name in buffer.raw[:count].decode().split("\n") if name)


def native_checks(client):
    c = client
    RW = os.O_RDWR
    h = c.call("open", b"/f", os.O_CREAT | RW, 0o644)
    assert c.write(h, b"abcdef", 0) == 6
    assert c.write(h, b"XY", 2) == 2
    assert c.read(h, 100, 0) == b"abXYef"
    assert c.read(h, 4, 4) == b"ef"
    assert c.read(h, 4, 100) == b""
    c.expect_error(errno.EINVAL, "pread", h, ctypes.create_string_buffer(1), 1, -1)
    c.call("fsync", h)
    c.call("close", h)
    c.expect_error(errno.EBADF, "close", h)
    c.expect_error(errno.EEXIST, "open", b"/f", os.O_CREAT | os.O_EXCL | RW, 0o644)
    c.expect_error(errno.ENOENT, "open", b"/missing", RW, 0)
    # Access modes are enforced per handle.
    h = c.call("open", b"/f", os.O_RDONLY, 0)
    c.expect_error(errno.EBADF, "pwrite", h, b"z", 1, 0)
    c.call("close", h)
    h = c.call("open", b"/f", os.O_WRONLY, 0)
    c.expect_error(errno.EBADF, "pread", h, ctypes.create_string_buffer(1), 1, 0)
    c.call("close", h)
    # Data survive close and reopen (written back to the storage ranks).
    h = c.call("open", b"/f", os.O_RDONLY, 0)
    assert c.read(h, 100, 0) == b"abXYef"
    c.call("close", h)
    # O_TRUNC and O_APPEND.
    h = c.call("open", b"/f", os.O_TRUNC | RW, 0)
    assert c.read(h, 10, 0) == b""
    c.write(h, b"one", 0)
    c.call("close", h)
    h = c.call("open", b"/f", os.O_APPEND | os.O_WRONLY, 0)
    c.write(h, b"two", 0)
    c.call("close", h)
    h = c.call("open", b"/f", os.O_RDONLY, 0)
    assert c.read(h, 10, 0) == b"onetwo"
    c.call("close", h)
    # A larger, multi-block file.
    payload = os.urandom(3 * 1024 * 1024 + 17)
    h = c.call("open", b"/big", os.O_CREAT | RW, 0o600)
    for offset in range(0, len(payload), 1 << 20):
        c.write(h, payload[offset:offset + (1 << 20)], offset)
    c.call("close", h)
    h = c.call("open", b"/big", os.O_RDONLY, 0)
    data = b"".join(c.read(h, 1 << 20, offset) for offset in range(0, len(payload), 1 << 20))
    assert data == payload
    c.call("close", h)
    # Directories.
    c.call("mkdir", b"/d", 0o755)
    c.expect_error(errno.EEXIST, "mkdir", b"/d", 0o755)
    c.expect_error(errno.ENOENT, "mkdir", b"/no/such", 0o755)
    c.expect_error(errno.EINVAL, "mkdir", b"/d/..", 0o755)
    c.expect_error(errno.EISDIR, "open", b"/d", os.O_RDONLY, 0)
    c.expect_error(errno.ENOTDIR, "open", b"/f/x", os.O_CREAT | RW, 0o644)
    c.call("mkdir", b"/d/sub", 0o755)
    assert c.list(b"/") == ["big", "d", "f"], c.list(b"/")
    assert c.list(b"/d") == ["sub"]
    small = ctypes.create_string_buffer(2)
    assert c.lib.dagonfs_client_list(b"/", small, 2) == len("big\nd\nf\n")
    # Rename: same name is a no-op, a directory cannot move below itself,
    # type mismatches are rejected, and a file replaces a file.
    c.call("rename", b"/f", b"/f")
    h = c.call("open", b"/f", os.O_RDONLY, 0)
    assert c.read(h, 10, 0) == b"onetwo"
    c.call("close", h)
    c.expect_error(errno.EINVAL, "rename", b"/d", b"/d/sub/d")
    c.expect_error(errno.EISDIR, "rename", b"/f", b"/d")
    c.expect_error(errno.ENOTDIR, "rename", b"/d", b"/f")
    c.call("rename", b"/f", b"/big")
    h = c.call("open", b"/big", os.O_RDONLY, 0)
    assert c.read(h, 10, 0) == b"onetwo"
    c.call("close", h)
    c.call("rename", b"/d", b"/e")
    h = c.call("open", b"/e/sub/g", os.O_CREAT | RW, 0o644)
    c.write(h, b"deep", 0)
    c.call("close", h)
    c.expect_error(errno.ENOTEMPTY, "rmdir", b"/e")
    c.expect_error(errno.ENOTDIR, "rmdir", b"/big")
    c.expect_error(errno.EISDIR, "unlink", b"/e")
    c.call("unlink", b"/e/sub/g")
    c.call("rmdir", b"/e/sub")
    c.call("rmdir", b"/e")
    c.call("unlink", b"/big")
    c.expect_error(errno.ENOENT, "unlink", b"/big")
    assert c.list(b"/") == []
    # A forked child opens its own connection and sees the same volume.
    h = c.call("open", b"/forked", os.O_CREAT | RW, 0o644)
    pid = os.fork()
    if pid == 0:
        try:
            ok = c.write(h, b"child", 0) == 5
        except BaseException:
            ok = False
        os._exit(0 if ok else 1)
    _, status = os.waitpid(pid, 0)
    assert os.waitstatus_to_exitcode(status) == 0
    assert c.read(h, 10, 0) == b"child"
    c.call("close", h)
    c.call("unlink", b"/forked")


def preload_checks(build, socket, scratch):
    compiler = shutil.which("cc") or shutil.which("gcc") or shutil.which("clang")
    if compiler is None:
        print("preload: SKIPPED (no C compiler)")
        return
    source = scratch / "preload_check.c"
    binary = scratch / "preload_check"
    source.write_text(PRELOAD_PROGRAM)
    subprocess.run([compiler, "-O1", str(source), "-o", str(binary)], check=True)
    extension = "dylib" if sys.platform == "darwin" else "so"
    library = build / f"libdagonfs_preload.{extension}"
    env = dict(os.environ, DAGONFS_SOCKET=str(socket), DAGONFS_PRELOAD_ROOT="/dagonfs")
    env["DYLD_INSERT_LIBRARIES" if sys.platform == "darwin" else "LD_PRELOAD"] = str(library)
    local = scratch / "local.txt"
    subprocess.run([str(binary), "/dagonfs", str(local)], env=env, check=True, timeout=60)
    assert local.read_text() == "local"


def main():
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--build-dir", default="build")
    parser.add_argument("--ranks", type=int, default=2)
    parser.add_argument("--mpirun", default="mpirun")
    parser.add_argument("mpirun_args", nargs="*")
    args = parser.parse_args()
    build = Path(args.build_dir).resolve()
    extension = "dylib" if sys.platform == "darwin" else "so"
    client = Client(build / f"libdagonfs_preload.{extension}")

    # Unix socket paths are short (104 bytes on macOS): use /tmp, not $TMPDIR.
    scratch = Path(tempfile.mkdtemp(prefix="dgn-", dir="/tmp" if os.path.isdir("/tmp") else None))
    socket = scratch / "v.sock"
    command = [args.mpirun, "-np", str(args.ranks), *args.mpirun_args,
               str(build / "dagonfs"), "--frontend", "native", str(socket)]
    volume = subprocess.Popen(command)
    try:
        deadline = time.monotonic() + 60
        while not socket.exists():
            if volume.poll() is not None:
                raise RuntimeError(f"volume exited with {volume.returncode}")
            if time.monotonic() > deadline:
                raise TimeoutError("native socket did not appear")
            time.sleep(0.05)
        client.call("set_socket", str(socket).encode())
        native_checks(client)
        print("native: ok")
        preload_checks(build, socket, scratch)
        print("preload: ok")
        assert client.lib.dagonfs_client_shutdown() == 0
        assert volume.wait(timeout=60) == 0, volume.returncode
        print("shutdown: ok")
    finally:
        if volume.poll() is None:
            volume.terminate()
            try:
                volume.wait(timeout=10)
            except subprocess.TimeoutExpired:
                volume.kill()
        shutil.rmtree(scratch, ignore_errors=True)


if __name__ == "__main__":
    main()
