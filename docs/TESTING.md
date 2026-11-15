# Testing and validation

DAGonFS is validated at three levels of increasing cost and fidelity. The lower levels run on any machine and in every CI job; the upper levels need a FUSE-capable host with MPI.

| Level | What it checks | Requirements | Command |
| --- | --- | --- | --- |
| Source contract | Architectural invariants (shared files identical, single-reply handlers, no pointers in MPI packets, bounded peer paths, build ordering) | Python 3 | `python3 tests/test_source_contract.py` |
| Build and smoke | Both executables and the launcher compile with `-Wall -Wextra`; usage and launcher dry-run behave | CMake, C++23 compiler, MPI, FUSE3 headers | `cmake -S . -B build && cmake --build build && ctest --test-dir build` |
| Mounted integration | Fifteen POSIX scenarios through the kernel, plus cross-peer replication | Linux, `/dev/fuse`, `fusermount3`, MPI | `python3 tests/mount.py --model … --ranks …` |
| Multi-node | Mounted integration with ranks spread over several hosts | Slurm cluster whose nodes provide FUSE | `sbatch … tests/slurm/multinode.sbatch` |

## 1. Mounted integration harness

`tests/mount.py` creates a volume through the Python API (`python/dagonfs`). It waits until the mount point(s) appear in `/proc/self/mounts`, runs `tests/test_python_posix.py` against rank 0's mount and, for peer-to-peer volumes, runs the cross-peer checks of §2.1. It **always** unmounts every mount point it created and waits for the MPI job (or terminates it), even when a test fails.

```bash
python3 tests/mount.py --build-dir build --model client-server --ranks 4 -- --oversubscribe
python3 tests/mount.py --build-dir build --model peer-to-peer  --ranks 4 -- --oversubscribe
```

Arguments after `--` are passed to `mpirun`. The job's output is written next to the mount root (`<root>.log`).

## 2. POSIX scenarios

Each scenario in `tests/test_python_posix.py` is named after the behaviour it protects; most reproduce a defect documented in [DEFECT-ANALYSIS.md](DEFECT-ANALYSIS.md).

| Scenario | Guards against |
| --- | --- |
| `test_file_lifecycle_and_metadata` | append, `fsync`, `chmod`, `ftruncate` shrink |
| `test_write_only_append_preserves_content` | D9 |
| `test_repeated_multi_block_rewrites` | D1, D2 (multi-rank rewrite with shrinking and growing sizes) |
| `test_truncate_grow_and_shrink_without_open` | D4 |
| `test_read_past_end_of_file` | D6 |
| `test_partial_overwrite_in_place` | read-modify-write of a multi-block file |
| `test_two_descriptors_on_the_same_file` | D8 |
| `test_touch_sets_current_time` | D10 |
| `test_mknod_regular_file_keeps_mode_and_owner` | D12, D25 |
| `test_directory_link_rename_and_scandir` | hard and symbolic links, rename, enumeration |
| `test_rename_overwrites_target_and_self_rename_is_noop` | D13, D14 |
| `test_directory_rename_updates_link_counts` | D14 |
| `test_posix_errors_for_directory_operations` | D15, D16 |
| `test_large_directory_listing_and_recursive_removal` | D7 (600 entries, multi-buffer `readdir`, `rm -rf`) |
| `test_inode_numbers_identify_files` | D41 (distinct `st_ino`, hard links share it, `cat f f > g`) |
| `test_exclusive_create_and_xattrs` | D11, `O_EXCL`, xattr lifecycle |

The suite can also be pointed at any existing mount: `DAGONFS_MOUNTPOINT=/path python3 tests/test_python_posix.py`.

### 2.1 Peer-to-peer checks

| Check | Guards against |
| --- | --- |
| Write on rank 0, read on peer *p*; rewrite on *p*, read on rank 0 | D20, D21, D34 |
| Directory rename and removal replicate | D14, D23, D25 |
| Two peers create and write 12 files each **simultaneously**, then read each other's files | D34–D39 (concurrent originators) |
| `chmod`, `utime` (nanosecond times), `setxattr`/`removexattr` and `symlink` on rank 0 are observed on peer *p*; removal of the link replicates | D40 |

## 3. Multi-node validation with Slurm

`tests/slurm/multinode.sbatch` runs both models with the ranks of the allocation. It places ranks 0 and 1 on the first node (`--map-by slot`), so the harness can observe both mount points locally while data blocks are stored on every node.

```bash
sbatch -p <partition> -N 2 --ntasks-per-node=2 tests/slurm/multinode.sbatch build
```

Every allocated node needs `/dev/fuse`, a set-uid `fusermount3` and the libfuse3 runtime. On clusters where only some node classes provide FUSE, select them with `-w`/`--constraint`.

## 4. Recorded results

### 4.0 Metadata replication and D41 (15 November 2026)

After D40–D42 were fixed, the sixteen-scenario suite was run on the login node with both models at 1, 2, 3 and 4 ranks. All scenarios passed, the peer-to-peer replication checks (including metadata replication) passed, and every job shut down cleanly. The performance study of [PERFORMANCE.md](PERFORMANCE.md) §4 then ran six real DAGonStar workflows on each volume, and every task's output was checked.


Validation of the unified code base on 15 November 2026: a single `dagonfs` executable, with the model selected by `--model`.

**Table 1 — functional validation.**

| Environment | Model | Ranks | Result |
| --- | --- | --- | --- |
| Rocky Linux 8.10 login node, GCC 12.2.1, Open MPI 4.1.4 (UCX 1.15), libfuse 3.3.0 | client-server | 1, 2, 4 | 15/15 pass, clean shutdown |
| same | peer-to-peer | 1, 2, 4 | 15/15 pass, replication and concurrency checks pass, clean shutdown |
| same | peer-to-peer | 2 and 4, alternating | 20/20 consecutive stress runs pass |
| 2 × GPU nodes (Slurm, `--map-by slot`) | client-server | 4 (2 per node) | 15/15 pass in 4/4 runs (suite: 0.69 s) |
| same | peer-to-peer | 4 (2 per node) | 15/15 + replication + concurrency pass in 4/4 runs (suite: 0.84 s) |
| same | legacy alias `DAGonFS_P2P.exe -f <root> 0`, launcher with `--model` | 2, 3 | mount, cross-peer read, shutdown from any peer |

The unmodified code base, run with the same suite on the same login node (client-server, 3 ranks), terminated with a segmentation fault: 3 failures and 12 errors out of 15.

### 4.1 Peer-to-peer across nodes

**Table 2 — mean latency per operation from rank 0's mount, 2 nodes × 2 ranks, peer-to-peer (50 operations each).**

| Operation | Blocking probe, collectives (before D33) | Final design |
| --- | --- | --- |
| `mkdir` | 362.9 ms | 0.43 ms |
| `rmdir` | 450.1 ms | 0.03 ms |
| create (empty file) | 427.0 ms | 0.07 ms |
| write 4 KiB (open, write, close) | 372.7 ms | 0.26 ms |
| read 4 KiB (open, read, close) | 475.9 ms | 0.20 ms |
| `unlink` | 415.3 ms | 0.03 ms |

The whole fifteen-scenario suite on two nodes took 571 s before and 0.84 s after the fix. Removing the latency then exposed the concurrency defects D34–D39, which the 400 ms serialisation had masked. The concurrent-originator check was added to catch them, and the stress runs in Table 1 were used to confirm their absence.
