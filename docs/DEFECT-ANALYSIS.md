# Defect analysis and remediation

This document records the systematic assessment of DAGonFS carried out in November 2026, the defects it uncovered, their root causes, and the remedies adopted. It is written as an engineering case study: for each class of defect it states the observable failure, the mechanism, and the invariant that now prevents recurrence.

## 1. Method

The assessment combined three techniques.

1. **Static review** of every FUSE handler against the libfuse low-level contract (exactly one reply per request; `fuse_reply_none` only for `forget`) and of every MPI exchange against the MPI standard's matching and ownership rules.
2. **Differential review** of the two source trees, which had been maintained by hand as near-copies and had drifted.
3. **Dynamic testing** on a production HPC system (Rocky Linux 8.10, GCC 12.2, Open MPI 4.1.4, libfuse 3.3.0): a regression suite of fifteen POSIX scenarios, written *before* the fixes so that each scenario reproduces a suspected defect, executed against both models with 1, 2 and 4 ranks on one node and with 4 ranks across two nodes under Slurm.

Against the unmodified code, the client-server file system terminated with a segmentation fault (exit status 139) during the suite; once the daemon had died, every remaining scenario failed with a disconnected mount (3 failures, 12 errors out of 15). After remediation all fifteen scenarios pass on both models and in every configuration, and the cross-peer replication checks pass in both directions.

## 2. Memory-safety defects in the data plane

**D1 — Cross-address-space `free()`.** Ranks returned the *addresses* of the blocks they had allocated to the root, which stored them in its `DataBlock` objects. `DataBlock::setData` freed the previous address before adopting a new one. On the first rewrite of any file spanning more than one rank, the root therefore called `free()` on pointers that belong to other processes: undefined behaviour that corrupts the root's heap and was the cause of the observed crash. *Remedy:* blocks never leave the process that allocated them; reads gather data, not addresses (data-plane invariants I1–I3).

**D2 — Unbounded growth on rewrite.** Storage ranks appended new blocks on every write without releasing the old ones, so repeatedly rewriting a file leaked memory proportional to the number of writes. *Remedy:* each write atomically replaces the local slice.

**D3 — Allocator mismatch.** Buffers obtained with `malloc` were released with `delete[]`. *Remedy:* consistent ownership through RAII (`DataBlock`) and `std::vector`.

**D4 — Out-of-range block access.** A read requested as many blocks as the *logical* size required, but `ftruncate`/`truncate` can grow the logical size without storing blocks, and `mknod`-created files had no block list at all. The root indexed past the end of the block vector. *Remedy:* reads are driven by the stored size; the logical tail is materialised as zeros.

**D5 — Heap overflow on write.** The writable capacity of the front-end buffer was inferred from `st_blocks`, which other code paths reset to zero or left stale, so a write could copy past the end of the allocation. *Remedy:* explicit `m_capacity`, grown by `File::ensureCapacity`; `st_blocks` now reports 512-byte units as POSIX requires.

## 3. FUSE protocol defects

**D6 — Double replies.** `read` with an offset past end-of-file, `write` after a failed `realloc`, and `listxattr` with a zero-size probe each replied and then continued, replying a second time. libfuse treats this as a programming error and the request state is corrupted. *Remedy:* every reply on an error path is immediately followed by `return`; a source-contract test checks this mechanically.

**D7 — `readdir` offsets as heap pointers.** Offsets passed to the kernel were addresses of heap-allocated map iterators, never freed (a leak per listing) and dereferenced on the next call even if the directory had changed meanwhile. Removing entries during a listing (`rm -rf` on a large directory) therefore dereferenced iterators to erased map nodes. *Remedy:* offsets are ordinal positions in the ordered child map.

**D8 — Shared buffer released by any descriptor.** The front-end buffer is per inode, but `flush`/`release` of *any* descriptor freed it, so a second descriptor on the same file read freed memory. *Remedy:* an open count; the buffer is released by the last release only.

## 4. POSIX-semantics defects

| Id | Operation | Observed behaviour | Correct behaviour |
| --- | --- | --- | --- |
| D9 | `open(O_WRONLY \| O_APPEND)` | Content silently discarded | Content preserved; only `O_TRUNC` truncates |
| D10 | `utimensat(UTIME_NOW)` (`touch`) | Timestamps reset to the epoch | Current time (`FUSE_SET_ATTR_*_NOW`) |
| D11 | `getxattr` with a large enough buffer | `ERANGE` | Value returned; `ERANGE` only if the buffer is too small |
| D12 | `mknod(S_IFREG)` | Permission bits dropped, uid/gid swapped | Mode and caller's credentials preserved |
| D13 | `rename(a, a)` | Entry deleted | Successful no-op |
| D14 | `rename` over an existing file | Target's link count not decremented (leak); directory link counts adjusted on the wrong condition | Target unlinked; `..` links moved only when a directory changes parent; `ENOTEMPTY`/`EISDIR`/`ENOTDIR`/`EINVAL` checks; `RENAME_NOREPLACE` honoured |
| D15 | `rmdir` of a non-empty directory | `EPERM` | `ENOTEMPTY` |
| D16 | `unlink` of a directory | Directory removed | `EISDIR` |
| D17 | `chmod` | File-type bits could be overwritten | Only permission bits change |
| D18 | `statfs` | `f_flag = 0777` (includes `ST_RDONLY`) | `f_flag = 0` |
| D19 | `access` | Group/other bits checked cumulatively | Owner, group or other triplet selected as the kernel does |

## 5. Distributed-protocol defects (peer-to-peer)

**D20 — Inode-number coupling.** File data were keyed by local inode numbers, which coincide across peers only if every peer allocates inodes in exactly the same order. Any non-replicated allocation (e.g. a symbolic link) or inode reclamation desynchronised the peers silently: a file written on one peer read back empty on another. *Remedy:* a cluster-wide file identity assigned at creation and carried in `CREATE_FILE`; inode reclamation is disabled in the peer-to-peer build.

**D21 — Stale kernel caches.** Remote writes changed sizes behind the local kernel's back; the kernel clamped reads to the cached size. *Remedy:* `fuse_lowlevel_notify_inval_inode` after every remote write.

**D22 — Payload source mismatch.** The listener received the header from any source and then the payload *also* from any source, so two concurrent originators could have their payloads swapped. *Remedy:* a request is now a single message, received from the source returned by `MPI_Probe`.

**D23 — Silent path truncation.** Paths were copied into 256-byte arrays; longer paths were truncated and replayed on the wrong object. *Remedy:* paths of up to 4095 bytes, explicit lengths validated by the receiver, and `ENAMETOOLONG` at the originator.

**D24 — Replay flags.** Plain `bool`s shared between threads (a data race), cleared only on the success path. *Remedy:* `std::atomic<bool>` consumed first in each handler.

**D25 — Missing replication.** `link` and `mknod` were not replicated. *Remedy:* `mknod` replays as an exclusive create; a new `LINK` message replays hard links.

**D26 — Shell-based unmount.** Termination executed `<root>/unmount.sh` through `system()`, a script that the project did not provide and that would run with the user's privileges from a shared directory. *Remedy:* `fusermount3 -u` through `posix_spawnp`, without a shell.

**D33 — Per-operation latency across nodes.** Each replicated operation was sent as a header plus a fixed-size payload (up to 8 KiB for `RENAME`) in two blocking sends per peer. On two nodes over TCP every metadata operation took roughly 200 ms, as the payload crossed the rendezvous threshold. *Remedy:* one variable-length message per request carrying only the bytes in use (see [PROTOCOL.md](PROTOCOL.md) §2).

## 6. Lifecycle, launcher and build defects

**D27 — Workers left blocked.** If the front end failed before mounting, it returned without sending `TERMINATE`, leaving every other rank blocked forever. *Remedy:* `FileSystem::start` always notifies termination.

**D28 — Single-level mount point creation.** The mount point was created with one `mkdir`; on a node where the parent did not exist the rank exited with `ENOENT`, and the error was logged through a logger that is disabled by default. Discovered by the two-node Slurm test. *Remedy:* recursive creation and unconditional diagnostics on `stderr`.

**D29 — Launcher command.** The launcher produced `mpirun -np 8--hostfile …` (missing separator), passed configuration values through `/bin/sh`, and crashed on a missing `num_processes`. *Remedy:* argument vector + `execvp`, validation of every key, `--dry-run`.

**D30 — Build ordering.** The executables did not depend on the log4cplus external project, so parallel builds failed on a clean tree. *Remedy:* explicit `add_dependencies`, `BUILD_BYPRODUCTS`, and installation of the library with a relative RPATH.

**D31 — Argument ownership.** The copy of `argv` handed to libfuse was never freed and was not NULL-terminated. *Remedy:* `ArgumentParser` owns a NULL-terminated copy and releases it after the session is destroyed.

**D32 — Integer overflow in MPI counts.** Byte counts per rank were `int`, overflowing above 2 GiB per rank. *Remedy:* counts in units of a block-sized MPI datatype.

## 7. Structural remedy

The two trees previously duplicated `FileSystem.cpp` with ad-hoc divergence. All shared sources are now byte-identical across the trees and specialised at compile time (`DAGONFS_P2P`). `tests/test_source_contract.py` enforces the identity, the single-reply rule, the absence of pointers in MPI packets, bounded peer paths, and the build-ordering constraint, so the most consequential classes of defect above are checked on every CI run without a FUSE-capable host.
