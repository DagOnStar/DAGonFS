# Defect analysis and remediation

This document records the systematic assessment of DAGonFS carried out in November 2026, the defects it uncovered, their root causes, and the remedies adopted. It is written as an engineering case study: for each class of defect it states the observable failure, the mechanism, and the invariant that now prevents recurrence.

## 1. Method

The assessment combined three techniques.

1. **Static review** of every FUSE handler against the libfuse low-level contract (exactly one reply per request; `fuse_reply_none` only for `forget`) and of every MPI exchange against the MPI standard's matching and ownership rules.
2. **Differential review** of the two source trees, which had been maintained by hand as near-copies and had drifted.
3. **Dynamic testing** on a production HPC system (Rocky Linux 8.10, GCC 12.2, Open MPI 4.1.4, libfuse 3.3.0): a regression suite of POSIX scenarios (fifteen at the start of the assessment, sixteen after D41), written *before* the fixes so that each scenario reproduces a suspected defect, executed against both models with 1, 2 and 4 ranks on one node and with 4 ranks across two nodes under Slurm. It was complemented by a per-operation latency micro-benchmark, a concurrent two-peer workload, and repeated stress runs to expose timing-dependent failures.

Against the unmodified code, the client-server file system terminated with a segmentation fault (exit status 139) during the suite; once the daemon had died, every remaining scenario failed with a disconnected mount (3 failures, 12 errors out of 15). After remediation, all fifteen scenarios pass on both models in every configuration, as do the cross-peer replication and concurrency checks, including 20 consecutive stress runs.

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

**D24 — Replay flags.** Plain `bool`s shared between threads (a data race), cleared only on the success path. *Remedy:* replays are identified by the requesting thread (D36); the flags remain only as a fallback and are always consumed first.

**D25 — Missing replication.** `link` and `mknod` were not replicated. *Remedy:* `mknod` replays as an exclusive create; a new `LINK` message replays hard links.

**D26 — Shell-based unmount.** Termination executed `<root>/unmount.sh` through `system()`, a script that the project did not provide and that would run with the user's privileges from a shared directory. *Remedy:* `fusermount3 -u` through `posix_spawnp`, without a shell.

**D33 — Per-operation latency across nodes.** On two nodes every replicated operation took 360–480 ms. This held even for `mkdir`, which sends one small message and waits for nothing, and the fifteen-scenario suite took 571 s against 0.45 s on one node. Shrinking the messages (one variable-length message per request instead of a header plus fixed 4 KiB path buffers) did not help. The cause was the listener's blocking `MPI_Probe`. Under `MPI_THREAD_MULTIPLE` it holds the MPI library's progress lock while it waits, starving the FUSE thread's own MPI calls in the same process. *Remedy:* `MPI_Iprobe` with a back-off bounded at 0.5 ms. Per-operation latency dropped to 0.03–0.4 ms (Table 2 of [TESTING.md](TESTING.md)).

The remaining defects of this section were latent behind D33: the 400 ms serialisation made the interleavings that trigger them improbable. They surfaced as soon as D33 was fixed, through a new test in which two peers create and write files simultaneously.

**D34 — Concurrent collectives on one communicator.** Each peer ran `MPI_Scatterv`/`MPI_Gatherv` rooted at itself on `MPI_COMM_WORLD`. Its FUSE thread acted as root, while its listener thread took part in other peers' collectives. When two peers' operations overlapped, collectives ran concurrently on the same communicator, which the MPI standard declares erroneous. *Observed:* a file rewritten on one peer read back on another with the right size but a mixture of old and new blocks. *Remedy:* a point-to-point data plane on dedicated tags, with the originator posting all transfers before waiting (invariant I6 of [DATA-PLANE.md](DATA-PLANE.md)).

**D35 — Distributed deadlock between replays and transfers.** The listener replayed namespace operations by issuing system calls on the local mount, which needs the peer's own FUSE thread. With two writers, peer A's FUSE thread waited for B's listener, B's listener was blocked in a replay needing B's FUSE thread, and B's FUSE thread waited for A's listener, itself blocked in a replay. *Observed:* both ranks spinning, the volume frozen. *Remedy:* replays run on a dedicated replayer thread fed by a FIFO, so the listener never depends on the local kernel. Remote writes that overtake their replayed create are kept as pending sizes.

**D36 — Replay misattribution.** The "this request is a replay" signal was a single global flag set by the listener just before its system call. A local request queued in the kernel immediately before the replayed one consumed the flag, so the local operation was never replicated and the replay was echoed back. *Remedy:* a replay is recognised by the requesting thread (`fuse_req_ctx(req)->pid` equals the replayer's thread id).

**D37 — Inode table read across threads.** To mirror a remote write, the listener looked up the file through the inode table, a `std::vector` that the FUSE thread grows (and may reallocate) concurrently. A missed lookup parked the new size as pending for a create that had already happened, so the size was never applied. *Observed:* a remotely written file read back as 0 bytes. *Remedy:* the global-id index holds `File` pointers, which stay valid because peer-to-peer volumes never reclaim inode objects.

**D38 — Kernel cache populated after its invalidation.** The listener could update a replica's size and invalidate the kernel's cache while the reply to the replayed create, carrying size 0, was still on its way to the kernel. The invalidation hit an inode the kernel did not know yet, and the late reply re-populated the cache with the stale size. *Observed:* `stat` reported 0 bytes for up to the 1 s attribute timeout, then the correct size. *Remedy:* in the peer-to-peer model, names and attributes are not cached by the kernel (timeouts 0). Every `stat` and `lookup` is answered from local memory without MPI, so the cost is one user-space round trip.

**D39 — Reads bounded by a size that another thread changes.** A descriptor that was already open kept its buffer while a remote write enlarged the file, and `read` was bounded only by `st_size`. *Remedy:* reads are bounded by the buffer capacity as well, and an open file's buffer grows when the reported size does.

**D40 — Metadata not replicated.** Earlier versions of this document listed this defect as a design limitation. `chmod`, `chown`, `utimensat`, extended attributes and symbolic links changed only the peer on which they were made. A workflow that marked a script executable on one node, or that relied on modification times or xattrs, therefore saw different files on different nodes. *Remedy:* four new requests: `SYMLINK`, `SET_ATTR`, `SET_XATTR` and `REMOVE_XATTR`. They are replayed by the replayer like the namespace requests (`symlink`, `chmod`/`lchown`/`utimensat`, `lsetxattr`, `lremovexattr`). `SET_ATTR` carries the attribute values as applied by the originator, and a "now" timestamp is resolved once there, so every peer stores the same time. An attribute value travels in the same message as the request, bounded at 64 KiB (Linux's `XATTR_SIZE_MAX`).

## 6. Defects found by the performance study

The benchmarks of [PERFORMANCE.md](PERFORMANCE.md) §4 run real DAGonStar workflows, whose tasks are ordinary shell tools. They found two more defects that the Python suite had not exercised.

**D41 — Every file reported inode number 0.** The FUSE entry carried the inode number, but the `st_ino` field of the attributes was never set. Every `stat` therefore returned `st_ino = 0`. Tools that identify a file by `(st_dev, st_ino)` take distinct files for the same one: `cat a a > b` failed with "input file is output file", `cp` and `tar` treat files as hard links of each other, and `find` and `rsync` can skip files. *Observed:* every DAGonStar task that concatenated its inputs failed on DAGonFS. *Remedy:* `st_ino` is set when an inode is initialised. A new scenario (`test_inode_numbers_identify_files`) checks distinct and shared inode numbers and the `cat` idiom.

**D42 — Python volume API and relative executables.** `dagonfs.Volume` starts `mpirun` from the parent of the mount root, so a relative `executable` path such as `build/dagonfs` could not be found. *Remedy:* paths containing a directory separator are made absolute when the `Volume` is created.

### 6.1 Defects found by the POSIX coverage review

A review of the FUSE low-level API against POSIX found four more defects: operations that applications use were missing or answered incorrectly. Each was reproduced by a new scenario before it was fixed ([TESTING.md](TESTING.md) §2), and the whole set was then exercised by a DAGonStar workflow whose tasks are Python scripts (§2.2 there).

**D43 — Locks were silently not provided.** `getlk` always answered `F_UNLCK`, and `setlk` and `flock` were not registered. Because libfuse requests kernel lock delegation only when both `getlk` and `setlk` exist, the kernel kept record locks locally, so they worked on one node and silently failed to exclude processes on other peers, while `F_GETLK` could contradict `F_SETLK`. Workflow tasks that serialise updates of a shared file with `fcntl.lockf` or `flock` could therefore lose updates in the peer-to-peer model. *Remedy:* one lock table per volume (`dagonfs::LockTable`), on rank 0, with POSIX semantics (ranges, conversion, `EAGAIN`, waiting, `EDEADLK`, `EINTR`, release on any close of the owner) and `flock` semantics. Waiting requests are answered later, so the single-threaded FUSE loop never blocks; peers reach the table through five new messages, `LOCK_SET`, `LOCK_GET`, `LOCK_RELEASE`, `LOCK_CANCEL` and `LOCK_REPLY` ([PROTOCOL.md](PROTOCOL.md) §3.4).

**D44 — Space and range operations missing.** `fallocate`, `lseek(SEEK_DATA/SEEK_HOLE)`, `copy_file_range`, `poll` and `ioctl` were not registered. `posix_fallocate` then fell back to writing zeros through the C library, and `fallocate(PUNCH_HOLE)` failed. *Remedy:* `fallocate` extends, punches and zeroes ranges in the front-end buffer (`EOPNOTSUPP` for collapse, insert and unshare); `SEEK_DATA`/`SEEK_HOLE` describe a non-sparse file; `copy_file_range` copies between buffers; `poll` reports a regular file always ready; `ioctl` fails with `ENOTTY`. `readdirplus` was added as well, so that `ls -l` and `os.scandir` obtain attributes with the names.

**D45 — FIFOs, sockets and device nodes could not be created.** `mknod` rejected every type but regular files with `EPERM`, so `mkfifo` and `bind` of a Unix-domain socket failed. *Remedy:* such nodes are created as metadata-only inodes (`SPECIAL_FILE`, with `st_rdev`) whose I/O the kernel serves itself, and a new `MKNOD` message replicates them to every peer. `mknod` became a replicated handler that recognises its replays.

**D46 — A buffer opened before a remote write-back stayed stale.** In the peer-to-peer model, a file that was open on a peer kept the content loaded at open time, even after another peer had written the file back. A process that opened a shared file, then waited for a lock, read the previous holder's data only if no other descriptor of the file was open on its peer. Similarly, data written under a lock and unlocked without `close` stayed on the writer's peer. *Remedy:* a remote write-back marks the local file as changed, and a buffer without unflushed local writes is reloaded before its next use; unlocking or downgrading a lock writes the file back first, as `close` does.

### 6.2 Defect found by the multi-node regression runs

**D47 — An MPI collective library could deadlock the shutdown.** About one client-server volume in twenty spread over two nodes never exited after a clean unmount: the harness waited 30 s, then terminated `mpirun`, which reported status 1. Stack traces showed every rank in `MPI_Finalize`: one in the teardown of Mellanox HCOLL (`hcoll_context_free`, disconnecting its UCX endpoints), the others waiting for it in UCX's `del_procs` and the PMIx fence. Open MPI loads HCOLL on `MPI_COMM_WORLD` whenever it is installed, although DAGonFS performs no collective operation. The failure appeared only when other MPI jobs loaded the node, which slowed the teardown. *Remedy:* `dagonfs` sets `OMPI_MCA_coll_hcoll_enable=0` before `MPI_Init` unless the environment already sets it. A contract test checks the order. Twenty-four concurrent two-node shutdowns then completed (§4.0b of [TESTING.md](TESTING.md)).

## 7. Lifecycle, launcher and build defects

**D27 — Workers left blocked.** If the front end failed before mounting, it returned without sending `TERMINATE`, leaving every other rank blocked forever. *Remedy:* `FileSystem::start` always notifies termination.

**D28 — Single-level mount point creation.** The mount point was created with one `mkdir`; on a node where the parent did not exist the rank exited with `ENOENT`, and the error was logged through a logger that is disabled by default. Discovered by the two-node Slurm test. *Remedy:* recursive creation and unconditional diagnostics on `stderr`.

**D29 — Launcher command.** The launcher produced `mpirun -np 8--hostfile …` (missing separator), passed configuration values through `/bin/sh`, and crashed on a missing `num_processes`. *Remedy:* argument vector + `execvp`, validation of every key, `--dry-run`.

**D30 — Build ordering.** The executables did not depend on the log4cplus external project, so parallel builds failed on a clean tree. *Remedy:* explicit `add_dependencies`, `BUILD_BYPRODUCTS`, and installation of the library with a relative RPATH.

**D31 — Argument ownership.** The copy of `argv` handed to libfuse was never freed and was not NULL-terminated. *Remedy:* `ArgumentParser` owns a NULL-terminated copy and releases it after the session is destroyed.

**D32 — Integer overflow in MPI counts.** Byte counts per rank were `int`, overflowing above 2 GiB per rank. *Remedy:* counts in units of a block-sized MPI datatype.

## 8. Structural remedy

The client-server and peer-to-peer front ends were originally two hand-maintained copies of the code in `src/client-server` and `src/peer-to-peer`, and their divergence caused several of the defects above. They are now **one code base**:

* There is a single `src/` tree and a single executable, `dagonfs`. The deployment model is a run-time property of each volume: `--model`, `dagonfs_model` in `DAGonFS.ini`, or `dagonfs::VolumeOptions` in the C++ and Python APIs.
* Client-server storage ranks run the same listener as peers, and both models share one wire protocol and one point-to-point data plane.
* The few behaviours intrinsic to a model (namespace replication, inode reclamation, kernel cache timeouts, MPI thread level) are run-time branches on the model.
* `DAGonFS_CS.exe` and `DAGonFS_P2P.exe` survive only as symbolic links, for backward compatibility.

`tests/test_source_contract.py` enforces the absence of model-specific trees and compile-time switches. It also enforces the single-reply rule, the absence of pointers in MPI messages, bounded paths, the absence of collectives in the data plane, and listener-independent replays, so the most consequential classes of defect above are checked on every CI run without a FUSE-capable host. `tests/mount.py` adds a concurrent two-peer workload that reproduces D34–D38 on the old code.
