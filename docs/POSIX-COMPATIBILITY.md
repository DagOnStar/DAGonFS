# POSIX and FUSE operation coverage

## Scope

DAGonFS uses the FUSE3 low-level API. It implements every operation of that API that has a POSIX (or common Linux) counterpart, in both deployment models:

| Family | FUSE operations | System calls served |
| --- | --- | --- |
| Namespace | `lookup`, `mknod`, `mkdir`, `unlink`, `rmdir`, `rename`, `link`, `symlink`, `readlink` | `open(O_CREAT)`, `mknod`, `mkfifo`, `bind(AF_UNIX)`, `mkdir`, `unlink`, `rmdir`, `rename`/`renameat2(RENAME_NOREPLACE)`, `link`, `symlink`, `readlink` |
| Metadata | `getattr`, `setattr`, `access`, `statfs` | `stat`, `chmod`, `chown`, `utimensat`, `truncate`, `access`, `statvfs` |
| File I/O | `create`, `open`, `read`, `write`, `flush`, `release`, `fsync` | `open`, `read`/`pread`, `write`/`pwrite`, `close`, `fsync`/`fdatasync`, `mmap` (through the kernel page cache) |
| Space and ranges | `fallocate`, `lseek`, `copy_file_range` | `posix_fallocate`, `fallocate` (allocate, `KEEP_SIZE`, `PUNCH_HOLE`, `ZERO_RANGE`), `lseek(SEEK_DATA/SEEK_HOLE)`, `copy_file_range` |
| Locks | `getlk`, `setlk`, `flock` | `fcntl(F_GETLK/F_SETLK/F_SETLKW)`, `lockf`, `flock` |
| Directories | `opendir`, `readdir`, `readdirplus`, `releasedir`, `fsyncdir` | `getdents`, `scandir`, `ls -l` |
| Extended attributes | `setxattr`, `getxattr`, `listxattr`, `removexattr` | the `*xattr` family |
| Readiness and control | `poll`, `ioctl` | `poll`/`select`/`epoll` (always ready), `ioctl` (`ENOTTY`) |
| Lifecycle | `init`, `destroy`, `forget`, `forget_multi` | — |

`lseek` and `copy_file_range` callbacks exist from libfuse 3.8 and 3.4. When DAGonFS is built against an older libfuse (for example libfuse 3.3 on RHEL 8), they are not registered and the kernel performs both operations itself with identical results: every byte of a file is data, and a copy is a read followed by a write.

The remaining callbacks of the API have no POSIX meaning for this file system and are intentionally not registered; FUSE answers them with `ENOSYS`, and the kernel maps that to the documented behaviour of the system call:

* `bmap` applies only to block-device-backed (`fuseblk`) mounts;
* `write_buf` and `retrieve_reply` are zero-copy and cache-retrieval optimisations of `write` and of kernel notifications that DAGonFS does not use;
* `tmpfile` (`O_TMPFILE`, Linux-specific, libfuse 3.15+): `open(O_TMPFILE)` fails with `EOPNOTSUPP`, which `tempfile.TemporaryFile` and similar libraries handle by creating and unlinking a named file;
* POSIX ACLs are not supported: setting a `system.posix_acl_*` attribute fails with `EOPNOTSUPP` (so `setfacl` reports the failure instead of storing an ACL that would be ignored); the permission bits apply.

Every registered request handler replies exactly once. Error paths reply with a POSIX errno and return immediately; this is essential because an unanswered FUSE request appears to applications as a hang. Lock requests that must wait are the only replies deferred to another thread, and each is answered exactly once, by the lock table.

## Semantics relevant to applications

`create`, `mknod`, `mkdir`, `symlink`, and `link` return `EEXIST` when the destination name already exists. `rename` follows POSIX: renaming a name onto itself is a no-op, an existing target is replaced atomically, a directory may replace only an empty directory (`ENOTEMPTY`, `EISDIR`, `ENOTDIR`), a directory cannot be moved below itself (`EINVAL`), and `RENAME_NOREPLACE` is honoured (`RENAME_EXCHANGE` and `RENAME_WHITEOUT` fail with `EINVAL`). `unlink` of a directory fails with `EISDIR`; `rmdir` of a non-empty directory with `ENOTEMPTY`. `getxattr`/`listxattr` with a zero size return the required size, and `ERANGE` only when the supplied buffer is too small. `setattr` is the FUSE entry point used by common POSIX metadata operations such as `chmod`, `chown`, `utime`, and `truncate`. Directory enumeration is implemented through `readdir` and `readdirplus`; callers should not depend on a stable enumeration order across concurrent namespace changes.

### Special files

`mkfifo`, `mknod(S_IFIFO | S_IFSOCK | S_IFCHR | S_IFBLK)` and `bind` of a Unix-domain socket create a node that carries only metadata (type, permissions, owner, times and, for devices, `st_rdev`). Once the kernel knows the node's type, it serves `open`, `read`, `write` and `connect` on it itself, exactly as on a local file system: a FIFO connects processes of the same node. The kernel allows only privileged processes to create device nodes (`EPERM` otherwise). Any other file type fails with `EINVAL`.

### Space allocation, holes and copies

Memory is the backing store, so `posix_fallocate` and `fallocate` without flags only extend the file (the new range reads as zeros); `FALLOC_FL_KEEP_SIZE` alone succeeds without effect. `FALLOC_FL_PUNCH_HOLE | FALLOC_FL_KEEP_SIZE` and `FALLOC_FL_ZERO_RANGE` write zeros into the range (`ZERO_RANGE` also extends the file unless `KEEP_SIZE` is given; some kernels, such as RHEL 8's 4.18, do not forward `ZERO_RANGE` to FUSE and fail it with `EOPNOTSUPP`). `COLLAPSE_RANGE`, `INSERT_RANGE` and `UNSHARE_RANGE` fail with `EOPNOTSUPP`. Files are never sparse: `SEEK_DATA` returns the offset itself and `SEEK_HOLE` the file size, and both fail with `ENXIO` at or past the end. `copy_file_range` copies between the front-end buffers of the two files; overlapping ranges of the same file fail with `EINVAL`, and an `O_APPEND` destination with `EBADF`.

### Advisory locks

POSIX record locks (`fcntl`, `lockf`) and BSD `flock` locks are held in one **lock table per volume**, so they exclude processes of every node:

* In the client-server model, the table is in the front end (rank 0), through which every request passes.
* In the peer-to-peer model, the table is on rank 0, the *lock coordinator*. The other peers send their lock requests to it through the control plane and receive one reply per request ([PROTOCOL.md](PROTOCOL.md) §3.4).

The semantics are those of Linux:

* Record locks belong to a process (FUSE's lock owner), cover byte ranges, and may be shared (`F_RDLCK`) or exclusive (`F_WRLCK`). A new lock by the same owner converts, splits or merges its existing locks; `F_UNLCK` releases a range.
* `F_SETLK` fails with `EAGAIN` on a conflict. `F_SETLKW` waits without blocking the file system: the request stays pending until the conflicting locks are released, and it fails with `EDEADLK` if waiting would close a cycle of owners waiting for each other (detected across files and nodes), or with `EINTR` if a signal interrupts it.
* `F_GETLK` reports the first conflicting lock (`l_pid` identifies the holder only when it runs on the same node: the kernel translates it in the caller's process namespace), or `F_UNLCK`.
* Closing any descriptor of a file releases every record lock that the process holds on that file, after the file's dirty data have been written back, so the next holder reads them.
* `flock` locks belong to the open file description, cover the whole file, are released by its last close, and are independent of record locks. Converting a `flock` lock is not atomic, as on Linux.
* Open-file-description locks (`F_OFD_SETLK`) are kept until they are explicitly unlocked: the kernel does not tell a FUSE file system when the last descriptor of such a description is closed.
* Locks are advisory, are not inherited across `fork` (record locks), and disappear with the volume. `fcntl` locks taken on another file system are unrelated.

A lock therefore also orders data: unlocking, downgrading or closing first writes the file's dirty data back, so a process that acquires the lock afterwards, on any node, reads the data written under it (see the consistency model below).

## Consistency model

DAGonFS provides **close-to-open consistency**, the model of NFS and of most workflow scratch file systems.

* While a file is open on a front end, reads and writes operate on an in-memory whole-file buffer held by that front end; several descriptors on the same file share it.
* In the peer-to-peer model, when another peer writes the file back, an open buffer without unflushed local writes is reloaded before the next read or write. A reader that synchronises with the writer (through the writer's `close`, a lock, or the workflow's dependencies) therefore sees the new content even if it opened the file earlier.
* The content is distributed to the storage ranks (write-back) on `fsync`, `flush` of a dirty file, and when the last descriptor is released. `truncate` on a closed file is applied and written back immediately.
* An `open` that follows a completed `close` (on any peer, in the peer-to-peer model) observes the closed content: in the peer-to-peer model, `close`/`fsync` returns only after every peer has stored its slice and updated its size. Concurrent writers of the same file on different peers are not serialised: the last write-back wins.
* `O_TRUNC` truncates; `O_APPEND` appends; `O_WRONLY` alone preserves existing content.
* `st_blocks` is reported in 512-byte units as POSIX specifies; `statfs` reports a writable file system.

## Peer-to-peer specifics

Namespace and metadata operations (`create`, `mknod` of regular and special files, `mkdir`, `unlink`, `rmdir`, `rename`, `link`, `symlink`, `chmod`, `chown`, `utimensat`, `setxattr`, `removexattr`) are replicated to every peer in origination order, asynchronously: a name created on one peer appears on the others shortly afterwards (applications that hand a path from one node to another should do so after the producer has closed the file, as DAG-structured workflows naturally do). The kernel's name and attribute caches are disabled in this model, so a replicated change is visible as soon as it has been applied. Timestamps are replicated as applied on the originating peer, so `touch` sets the same time on every peer. Extended-attribute values are limited to 64 KiB (`E2BIG` beyond), and the macOS-only `position` argument is not supported (`ENOTSUP`). Paths longer than 4095 bytes fail with `ENAMETOOLONG` on the originating peer. Lock requests are not replicated: they go to the lock coordinator. A FIFO created on one peer is a FIFO on every peer, but each peer's kernel serves its own pipe, so a writer and a reader must use the same node. See [PROTOCOL.md](PROTOCOL.md).

## Python example

`tests/test_python_posix.py` exercises this interface through Python's `pathlib`, `os`, `fcntl`, `select` and `socket` modules: binary read/write, `fsync`, metadata changes, directory traversal, links, rename, exclusive creation, xattrs, record and `flock` locks (waiting, interruption, deadlock detection, locked read-modify-write), `fallocate`, `SEEK_DATA`/`SEEK_HOLE`, `copy_file_range`, `poll`, `ioctl`, FIFOs and Unix-domain sockets. `tests/dagonstar/run_workflow.py` runs the same interface inside a DAGonStar workflow whose tasks are Python scripts (see [TESTING.md](TESTING.md) §2.2). Run it against a mount you already control:

```bash
DAGONFS_MOUNTPOINT=/path/to/mount python3 tests/test_python_posix.py
```

The following self-contained example performs a normal Python file lifecycle:

```python
from pathlib import Path
import os

root = Path(os.environ["DAGONFS_MOUNTPOINT"])
work = root / "python-example"
work.mkdir(exist_ok=True)
payload = work / "payload.bin"

with payload.open("wb") as stream:
    stream.write(bytes(range(256)) * 16)
    stream.flush()
    os.fsync(stream.fileno())

assert payload.stat().st_size == 4096
payload.rename(work / "payload-renamed.bin")
```

Run this only after the mount is available. The example validates namespace and I/O behavior; it does not assert persistence after unmounting.
