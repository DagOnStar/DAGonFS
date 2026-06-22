# POSIX and FUSE operation coverage

## Scope

DAGonFS uses the FUSE3 low-level API. It implements the following operation families in both deployment models:

- Namespace: lookup, `mknod`, `mkdir`, `unlink`, `rmdir`, `rename`, hard links, and symbolic links.
- Metadata: `getattr`, `setattr` (including chmod/chown/truncate requests sent through FUSE), `access`, and `statfs`.
- File I/O: create, open, read, write, flush, release, and `fsync`.
- Directory I/O: open, readdir, release, and `fsyncdir`.
- Extended attributes: set, get, list, and remove.
- Lifecycle: init, destroy, forget, and batched forget (`forget_multi`).
- Lock query: `getlk` always reports `F_UNLCK`, because DAGonFS does not retain byte-range locks.

The following FUSE extensions are intentionally not registered: `setlk`/`flock`, `ioctl`, `poll`, `fallocate`, `copy_file_range`, `lseek`, `readdirplus`, POSIX ACL operations, and Linux-specific notifications. FUSE returns `ENOSYS` for an unregistered operation. Applications requiring durable storage, retained byte-range locks, or those extensions must use a backing filesystem instead.

Every registered request handler must reply exactly once. Error paths reply with a POSIX errno and return immediately; this is essential because an unanswered FUSE request appears to applications as a hang.

## Semantics relevant to applications

`create`, `mknod`, `mkdir`, `symlink`, and `link` return `EEXIST` when the destination name already exists. `setattr` is the FUSE entry point used by common POSIX metadata operations such as `chmod`, `chown`, `utime`, and `truncate`. Directory enumeration is implemented through `readdir`; callers should not depend on a stable enumeration order across concurrent namespace changes.

Byte-range locks are local-kernel behavior only. `getlk` reports an unlocked range and DAGonFS does not distribute or persist locks among ranks. Python programs that need inter-process mutual exclusion across nodes should use an MPI synchronization primitive or a separate distributed lock service rather than `fcntl.lockf`.

## Python example

`tests/test_python_posix.py` exercises this interface through Python's `pathlib` and `os` modules: binary read/write, `fsync`, metadata changes, directory traversal, links, rename, exclusive creation, and xattrs. Run it against a mount you already control:

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
