# DAGonFS frontends

DAGonFS exposes the same distributed volume through selectable frontends:

| Frontend | Selector | Kernel FUSE required | Models | Intended use |
|---|---|---:|---|---|
| FUSE3 | `--frontend fuse3` (default) | Yes | client-server, peer-to-peer | Transparent POSIX paths for existing applications |
| Native | `--frontend native` | No | client-server | FUSE-free C clients and MPI-IO-aware applications |
| LD_PRELOAD | `--frontend preload` | No | client-server | Opt-in interception of file calls below a configured path |

Native and preload modes start rank 0 as a server on a Unix-domain socket; the
other ranks are storage ranks, exactly as in a client-server FUSE volume. Pass
the socket path where a FUSE volume takes its mount root. The preload library
(`libdagonfs_preload.so`, `.dylib` on macOS) contains both the C client
(`src/frontends/native/Client.h`) and the interposer.

```sh
mpirun -np 4 dagonfs --frontend native /tmp/dagonfs.sock

# C client (link against libdagonfs_preload) or interposer:
export DAGONFS_SOCKET=/tmp/dagonfs.sock
export DAGONFS_PRELOAD_ROOT=/dagonfs
export LD_PRELOAD=/path/to/libdagonfs_preload.so              # Linux
export DYLD_INSERT_LIBRARIES=/path/to/libdagonfs_preload.dylib # macOS
```

A client stops the volume with `dagonfs_client_shutdown()`; rank 0 then closes
every remaining handle (writing its data back), sends `TERMINATE` to the
storage ranks and exits. Peer-to-peer native volumes are rejected at start-up,
because a single rank-0 socket would bypass the namespace replication of
peer-to-peer mode.

## Native protocol (version 2)

Every request is a fixed `dagonfs_native_request` header, followed by the path,
an optional second path (rename) and the write payload; every reply is a fixed
`dagonfs_native_reply` header followed by the read payload or directory listing
(`src/frontends/native/Protocol.h`). Paths are below 4 KiB and payloads at most
8 MiB; larger reads and writes are shortened, as POSIX allows.

- **Persistent connections.** A client process opens one connection and sends
  all its requests on it, serialised by a mutex across threads. Version 1
  opened a connection per request; reusing it doubles the request rate on
  macOS and on PurpleJeans (see [PERFORMANCE-FRONTENDS.md](PERFORMANCE-FRONTENDS.md)).
- **Fork safety.** The connection records its owner's process id. A child
  created by `fork()` opens its own connection, so parent and child never
  interleave a stream; handles are volume-wide, so the child can keep using the
  parent's handles.
- **Clean failure.** Neither side can be killed by `SIGPIPE`: the server
  ignores it and both sides send with `MSG_NOSIGNAL` (`SO_NOSIGPIPE` on macOS).
  A malformed header gets an `EPROTO` reply and the connection is closed; the
  client then reconnects on the next call.
- **Handle lifetime.** Handles opened on a connection are closed when the
  connection ends, so a client that crashes still has its data written back to
  the storage ranks and its open counts released.
- **One server thread.** Rank 0 serves the connections with `poll()`, one
  request at a time, because client-server rank 0 runs at `MPI_THREAD_SINGLE`.
  The volume is therefore serialised at rank 0 in this mode.

Operations: `OPEN` (with `O_CREAT`, `O_EXCL`, `O_TRUNC`, `O_APPEND`, and the
access mode enforced per handle: `EBADF` for a write on a read-only handle and
vice versa), `CLOSE`, `READ`, `WRITE`, `FSYNC`, `STAT`, `FSTAT`, `MKDIR`,
`UNLINK`, `RMDIR`, `RENAME`, `LIST` and `SHUTDOWN`. Namespace operations follow
the FUSE handlers: `.` and `..` cannot be created, a rename onto the same name
is a no-op, a directory cannot be moved below itself (`EINVAL`), and type
mismatches return `EISDIR`, `ENOTDIR` or `ENOTEMPTY`. `dagonfs_client_list`
returns the required length with `errno = ERANGE` when the buffer is too small.

## Python client and command line

`python/dagonfs/native.py` implements the version-2 protocol with the Python
standard library only, so a workflow engine can drive a FUSE-free volume
without compiling or preloading anything. It keeps one connection per
`Client`, raises `OSError` with the server's `errno`, rejects relative paths,
and splits transfers into requests of at most 8 MiB:

```python
from dagonfs import Volume

with Volume("/tmp/v.sock", ranks=4, frontend="native") as volume:
    with volume.client() as client:
        client.put("inputs", "/run/inputs")         # file or directory tree
        client.get("/run/results", "results")
        print(client.listdir("/run"))
# leaving the block sends SHUTDOWN and waits for every rank
```

`Volume(frontend="native" | "preload")` waits until the socket answers a
`STAT /` request (not merely until the socket file exists), stops the volume
with `SHUTDOWN`, and reports `environment()`: `DAGONFS_SOCKET`, plus
`DAGONFS_PRELOAD_ROOT` and the platform's preload variable for the `preload`
frontend. The same operations are available to shell tasks:

```sh
python -m dagonfs --socket /tmp/v.sock wait --timeout 30
python -m dagonfs --socket /tmp/v.sock put inputs /run/inputs
python -m dagonfs --socket /tmp/v.sock ls /run
python -m dagonfs --socket /tmp/v.sock get /run/results results
python -m dagonfs --socket /tmp/v.sock shutdown
```

`tests/test_source_contract.py` checks the Python constants, opcodes and
structure sizes against `Protocol.h`; `tests/native_frontend.py` exercises the
client against a running volume (CI runs it with three ranks).

## Interposer coverage

The interposer handles absolute paths below `DAGONFS_PRELOAD_ROOT` and passes
everything else to the C library. It intercepts `open`, `openat` (absolute
paths), `close`, `read`, `write`, `pread`, `pwrite`, `lseek`, `fsync`,
`unlink`, `mkdir`, `rmdir` and `rename` (plus the `*64` names on Linux; a
rename between DAGonFS and another file system fails with `EXDEV`). A DAGonFS
file is represented by a real descriptor (a duplicate of `/dev/null`), so its
number never collides with descriptors the C library hands out; the
interposer keeps the file offset for `read`, `write` and `lseek`.

Linux resolves the real functions with `dlsym(RTLD_NEXT)`. macOS uses dyld's
`__interpose` section, which works for ordinary binaries; binaries protected
by System Integrity Protection ignore `DYLD_INSERT_LIBRARIES`.

Not covered: `stat`/`fstat`/`access` on DAGonFS paths (`fstat` on a DAGonFS
descriptor describes `/dev/null`), `opendir`/`readdir`, `dup`/`dup2`/`fcntl`,
`mmap`, `chdir` into the volume, relative paths, and the C library's internal
calls (for example `fopen` in glibc uses non-interposable entry points).
Applications that need complete POSIX semantics should use FUSE3.
