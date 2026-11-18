"""Pure-Python client of the DAGonFS native protocol (standard library only).

The native and preload frontends serve a volume on a Unix-domain socket
(``dagonfs --frontend native SOCKET``).  This module speaks the same version-2
protocol as ``src/frontends/native/Client.c``, so tools and workflow engines can
check readiness, move files in and out of a FUSE-free volume, and shut it down
without compiling or preloading anything::

    from dagonfs.native import Client

    with Client("/tmp/dagonfs.sock") as volume:
        volume.write_bytes("/results/a.txt", b"hello")
        print(volume.listdir("/results"))
        volume.shutdown()

The wire layout mirrors ``src/frontends/native/Protocol.h`` with the natural
C alignment of both supported platforms (LP64 Linux and macOS):
``tests/test_source_contract.py`` keeps the two in step.
"""

import errno
import os
import socket
import stat as stat_module
import struct
import time

MAGIC = 0x44474653
VERSION = 2
MAX_PATH = 4096
MAX_IO = 8 * 1024 * 1024

OPEN, CLOSE, READ, WRITE, FSYNC, STAT, MKDIR, UNLINK, RMDIR, RENAME, LIST, SHUTDOWN, FSTAT = range(1, 14)

# struct dagonfs_native_request: magic, version, opcode, path_length,
# second_path_length, data_length, flags, mode, <4 bytes padding>, handle, offset.
REQUEST = struct.Struct("=IHHIIIiI4xqq")
# struct dagonfs_native_reply: magic, version, reserved, error, data_length,
# value, inode, size, mode, nlink, uid, gid, atime, mtime, ctime.
REPLY = struct.Struct("=IHHiIqQQIIIIqqq")
assert REQUEST.size == 48 and REPLY.size == 80


class NativeStat:
    """The subset of ``struct stat`` that the protocol transports."""

    __slots__ = ("st_ino", "st_size", "st_mode", "st_nlink", "st_uid", "st_gid",
                 "st_atime", "st_mtime", "st_ctime")

    def __init__(self, reply):
        (self.st_ino, self.st_size, self.st_mode, self.st_nlink, self.st_uid, self.st_gid,
         self.st_atime, self.st_mtime, self.st_ctime) = reply[6:15]

    def is_dir(self):
        return stat_module.S_ISDIR(self.st_mode)

    def is_file(self):
        return stat_module.S_ISREG(self.st_mode)

    def __repr__(self):
        return "NativeStat(st_ino=%d, st_size=%d, st_mode=0o%o)" % (self.st_ino, self.st_size, self.st_mode)


def _path(value):
    encoded = os.fsencode(value)
    if not encoded.startswith(b"/"):
        raise ValueError("DAGonFS native paths are absolute volume paths: %r" % (value,))
    if len(encoded) >= MAX_PATH:
        raise OSError(errno.ENAMETOOLONG, os.strerror(errno.ENAMETOOLONG), value)
    return encoded


class Client:
    """One persistent connection to a native DAGonFS volume.

    :param socket_path: the socket given to ``dagonfs --frontend native``
        (default: ``$DAGONFS_SOCKET``).
    :param timeout: seconds allowed for one request (``None`` blocks).
    """

    def __init__(self, socket_path=None, timeout=60.0):
        socket_path = socket_path or os.environ.get("DAGONFS_SOCKET")
        if not socket_path:
            raise ValueError("no socket path given and DAGONFS_SOCKET is not set")
        self.socket_path = os.fspath(socket_path)
        self.timeout = timeout
        self._socket = None

    # -- connection -------------------------------------------------------
    def connect(self):
        if self._socket is None:
            connection = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
            connection.settimeout(self.timeout)
            try:
                connection.connect(self.socket_path)
            except OSError:
                connection.close()
                raise
            self._socket = connection
        return self

    def close(self):
        """Close the connection; the server closes (and writes back) its open handles."""
        if self._socket is not None:
            self._socket.close()
            self._socket = None

    def __enter__(self):
        return self.connect()

    def __exit__(self, *exc):
        self.close()
        return False

    def _receive(self, size):
        chunks, remaining = [], size
        while remaining:
            chunk = self._socket.recv(remaining)
            if not chunk:
                raise OSError(errno.EPROTO, "DAGonFS closed the connection")
            chunks.append(chunk)
            remaining -= len(chunk)
        return b"".join(chunks)

    def _call(self, opcode, path=None, second=None, data=b"", flags=0, mode=0, handle=0, offset=0):
        first = _path(path) if path is not None else b""
        other = _path(second) if second is not None else b""
        if len(data) > MAX_IO:
            raise ValueError("a single request carries at most %d bytes" % MAX_IO)
        self.connect()
        header = REQUEST.pack(MAGIC, VERSION, opcode, len(first), len(other), len(data),
                              flags, mode, handle, offset)
        try:
            self._socket.sendall(header + first + other + bytes(data))
            reply = REPLY.unpack(self._receive(REPLY.size))
            if reply[0] != MAGIC or reply[1] != VERSION or reply[4] > MAX_IO:
                raise OSError(errno.EPROTO, "unexpected DAGonFS native reply")
            payload = self._receive(reply[4]) if reply[4] and not reply[3] else b""
        except BaseException:
            # The stream position is unknown: start over on a new connection.
            self.close()
            raise
        if reply[3]:
            raise OSError(reply[3], os.strerror(reply[3]), path)
        return reply, payload

    # -- namespace --------------------------------------------------------
    def stat(self, path):
        return NativeStat(self._call(STAT, path)[0])

    def exists(self, path):
        try:
            self.stat(path)
        except FileNotFoundError:
            return False
        return True

    def listdir(self, path="/"):
        capacity = 1 << 16
        while True:
            try:
                _, payload = self._call(LIST, path, flags=capacity)
            except OSError as error:
                if error.errno != errno.ERANGE or capacity >= MAX_IO:
                    raise
                capacity = min(MAX_IO, capacity * 4)
                continue
            return sorted(os.fsdecode(name) for name in payload.split(b"\n") if name)

    def mkdir(self, path, mode=0o755):
        self._call(MKDIR, path, mode=mode)

    def makedirs(self, path, mode=0o755):
        current = ""
        for part in [p for p in os.fspath(path).split("/") if p]:
            current += "/" + part
            try:
                self.mkdir(current, mode)
            except FileExistsError:
                if not self.stat(current).is_dir():
                    raise

    def unlink(self, path):
        self._call(UNLINK, path)

    def rmdir(self, path):
        self._call(RMDIR, path)

    def rename(self, old, new):
        self._call(RENAME, old, new)

    # -- data -------------------------------------------------------------
    def open(self, path, flags=os.O_RDONLY, mode=0o644):
        return self._call(OPEN, path, flags=flags, mode=mode)[0][5]

    def close_handle(self, handle):
        self._call(CLOSE, handle=handle)

    def pread(self, handle, size, offset):
        return self._call(READ, flags=min(size, MAX_IO), handle=handle, offset=offset)[1]

    def pwrite(self, handle, data, offset):
        return self._call(WRITE, data=data, handle=handle, offset=offset)[0][5]

    def fsync(self, handle):
        self._call(FSYNC, handle=handle)

    def write_bytes(self, path, data):
        handle = self.open(path, os.O_WRONLY | os.O_CREAT | os.O_TRUNC)
        try:
            view, offset = memoryview(data), 0
            while offset < len(view):
                offset += self.pwrite(handle, view[offset:offset + MAX_IO], offset)
        finally:
            self.close_handle(handle)
        return len(data)

    def read_bytes(self, path):
        handle = self.open(path)
        try:
            chunks, offset = [], 0
            while True:
                chunk = self.pread(handle, MAX_IO, offset)
                if not chunk:
                    return b"".join(chunks)
                chunks.append(chunk)
                offset += len(chunk)
        finally:
            self.close_handle(handle)

    def put(self, local, remote):
        """Copy a local file or directory tree into the volume; returns bytes copied."""
        if os.path.isdir(local):
            self.makedirs(remote)
            total = 0
            for name in sorted(os.listdir(local)):
                total += self.put(os.path.join(local, name), remote.rstrip("/") + "/" + name)
            return total
        with open(local, "rb") as source:
            return self.write_bytes(remote, source.read())

    def get(self, remote, local):
        """Copy a volume file or directory tree to the local file system; returns bytes copied."""
        if self.stat(remote).is_dir():
            os.makedirs(local, exist_ok=True)
            return sum(self.get(remote.rstrip("/") + "/" + name, os.path.join(local, name))
                       for name in self.listdir(remote))
        data = self.read_bytes(remote)
        with open(local, "wb") as target:
            target.write(data)
        return len(data)

    # -- life cycle -------------------------------------------------------
    def ping(self):
        """True if the volume answers a ``STAT /`` request."""
        try:
            return self.stat("/").is_dir()
        except OSError:
            self.close()
            return False

    def shutdown(self):
        """Ask rank 0 to write back every handle, terminate the storage ranks and exit."""
        try:
            self._call(SHUTDOWN)
        finally:
            self.close()


def wait_until_ready(socket_path, timeout=30.0, alive=lambda: True):
    """Wait until the volume at ``socket_path`` answers; ``alive`` aborts early."""
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline and alive():
        if answers(socket_path):
            return True
        time.sleep(0.1)
    return answers(socket_path)


def answers(socket_path):
    """True if a native volume is serving ``socket_path``."""
    if not os.path.exists(socket_path):
        return False
    try:
        with Client(socket_path, timeout=5.0) as client:
            return client.ping()
    except OSError:
        return False
