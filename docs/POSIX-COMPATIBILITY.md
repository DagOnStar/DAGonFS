# POSIX and FUSE operation coverage

DAGonFS uses the FUSE3 low-level API. It implements the following operation families in both deployment models:

- Namespace: lookup, `mknod`, `mkdir`, `unlink`, `rmdir`, `rename`, hard links, and symbolic links.
- Metadata: `getattr`, `setattr` (including chmod/chown/truncate requests sent through FUSE), `access`, and `statfs`.
- File I/O: create, open, read, write, flush, release, and `fsync`.
- Directory I/O: open, readdir, release, and `fsyncdir`.
- Extended attributes: set, get, list, and remove.
- Lifecycle: init and forget.
- Lock query: `getlk` always reports `F_UNLCK`, because DAGonFS does not retain byte-range locks.

The following FUSE extensions are intentionally not registered: `setlk`/`flock`, `ioctl`, `poll`, `fallocate`, `copy_file_range`, `lseek`, `readdirplus`, POSIX ACL operations, and Linux-specific notifications. FUSE returns `ENOSYS` for an unregistered operation. Applications requiring durable storage, retained byte-range locks, or those extensions must use a backing filesystem instead.

Every registered request handler must reply exactly once. Error paths reply with a POSIX errno and return immediately; this is essential because an unanswered FUSE request appears to applications as a hang.
