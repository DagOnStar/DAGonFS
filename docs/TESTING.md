# Testing and validation

DAGonFS is validated at three levels of increasing cost and fidelity. The lower levels run on any machine and in every CI job; the upper levels need a FUSE-capable host with MPI.

| Level | What it checks | Requirements | Command |
| --- | --- | --- | --- |
| Source contract | Architectural invariants (shared files identical, single-reply handlers, no pointers in MPI packets, bounded peer paths, build ordering) | Python 3 | `python3 tests/test_source_contract.py` |
| Build and smoke | Both executables and the launcher compile with `-Wall -Wextra`; usage and launcher dry-run behave | CMake, C++23 compiler, MPI, FUSE3 headers | `cmake -S . -B build && cmake --build build && ctest --test-dir build` |
| Mounted integration | Fifteen POSIX scenarios through the kernel, plus cross-peer replication | Linux, `/dev/fuse`, `fusermount3`, MPI | `python3 tests/mount.py --model … --ranks …` |
| Multi-node | Mounted integration with ranks spread over several hosts | Slurm cluster whose nodes provide FUSE | `sbatch … tests/slurm/multinode.sbatch` |

## 1. Mounted integration harness

`tests/mount.py` starts the selected model under `mpirun`, waits until the mount point(s) appear in `/proc/self/mounts`, runs `tests/test_python_posix.py` against rank 0's mount, and in peer-to-peer mode verifies replication between rank 0 and a second peer. It **always** unmounts every mount point it created and waits for (or terminates) the MPI job, even when a test fails.

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
| `test_exclusive_create_and_xattrs` | D11, `O_EXCL`, xattr lifecycle |

The suite can also be pointed at any existing mount: `DAGONFS_MOUNTPOINT=/path python3 tests/test_python_posix.py`.

## 3. Multi-node validation with Slurm

`tests/slurm/multinode.sbatch` runs both models with the ranks of the allocation, placing ranks 0 and 1 on the first node (`--map-by slot`) so that the harness can observe both mount points locally while data blocks are stored on every node.

```bash
sbatch -p <partition> -N 2 --ntasks-per-node=2 tests/slurm/multinode.sbatch build
```

Every allocated node needs `/dev/fuse`, a set-uid `fusermount3` and the libfuse3 runtime; on clusters where only some node classes provide FUSE, select them with `-w`/`--constraint`.

## 4. Recorded results

Validation of the remediated code base on 15 November 2026:

| Environment | Model | Ranks | Result |
| --- | --- | --- | --- |
| Rocky Linux 8.10 login node, GCC 12.2.1, Open MPI 4.1.4, libfuse 3.3.0 | client-server | 1, 2, 3, 4 | 15/15 pass, clean shutdown |
| same | peer-to-peer | 1, 2, 4 | 15/15 pass, replication checks pass, clean shutdown |
| 2 × GPU nodes (Slurm, `--map-by slot`) | client-server | 4 (2 per node) | 15/15 pass, clean shutdown |
| same | peer-to-peer | 4 (2 per node) | 15/15 pass, replication checks pass, clean shutdown; slow (see §4.1) |

The unmodified code base, run with the same suite on the same login node (client-server, 3 ranks), terminated with a segmentation fault: 3 failures and 12 errors out of 15.

### 4.1 Peer-to-peer across nodes

The first two-node run exposed defect D28 (the mount root's parent did not exist on the second node). After the fix the suite passes, but replicated operations are slow across nodes: the suite takes minutes instead of under one second on a single node. Per-operation latency is under investigation; see [PERFORMANCE.md](PERFORMANCE.md).
