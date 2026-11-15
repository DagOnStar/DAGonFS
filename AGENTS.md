# DAGonFS contributor guide

## Project layout

- `src/` contains the single implementation, built as one executable, `dagonfs`:
  - `ramfs/FileSystem.*`: the FUSE low-level handlers;
  - `nodes/`: the namespace (`INode`, `Directory`, `File`, `SymbolicLink`);
  - `blocks/` and `mpi/DataBlockManager.*`: the data plane;
  - `mpi/RequestSender.*`, `mpi/DistributedCode.*` and `mpi/mpi_data.hpp`: the control plane;
  - `volume/`: `Options` (model, ini and command line, with no MPI or FUSE dependency) and `Volume` (`runVolume`);
  - `main.cpp`: a thin wrapper around `runVolume`;
  - `DAGonFS_Launcher.cpp`: the ini-driven launcher.
- `python/dagonfs/` contains the Python volume API (standard library only).
- `docs/` contains all documentation: the Markdown documents and the manual page (`docs/man/dagonfs.1`, installed by `docs/CMakeLists.txt`). Keep the manual page consistent with `docs/CONFIGURATION.md` and `dagonfs --help`.
- `bench/` contains the benchmark (`fsbench.py`, `workflow.py`, `run.py`, `plot.py`, `slurm/bench.sbatch`). Its charts go to `figures/bench-*.png`, and its results are recorded in `docs/PERFORMANCE.md`.
- `tests/` contains the source-contract tests, the mounted suite and the harness. Do not require a mounted filesystem in unit tests.

### Deployment models

- The client-server and peer-to-peer models are a **run-time** property of a volume (`dagonfs::Model`, selected with `--model`, `dagonfs_model` or the APIs). Do not reintroduce per-model source trees, executables or preprocessor switches. Branch on `FileSystem::peerToPeer`, and only where the behaviour is intrinsic to the model.
- `DAGonFS_CS.exe` and `DAGonFS_P2P.exe` are symbolic links kept for backward compatibility. The program name only sets the default model.
- `MPI_THREAD_MULTIPLE` is requested only for peer-to-peer volumes (`dagonfs::requiredThreadLevel`).

### Protocol and threading invariants

- Every request is one message (`PeerRequest` header + path bytes) on `DAGONFS_TAG_REQUEST`; see `docs/PROTOCOL.md` before changing the wire format, and update the contract tests with it.
- The data plane is point-to-point: post every `MPI_Isend`/`MPI_Irecv` before waiting. Never use collectives, because several peers originate transfers concurrently. A `WRITE` completes only after an ACK from every rank.
- The listener waits with `waitForRequest` (an `MPI_Iprobe` back-off), never with a blocking `MPI_Probe`.
- The listener never issues a system call on a mount point. Namespace replays go to the replayer thread's FIFO.
- Every change to the namespace or to metadata (names, links, symbolic links, mode, ownership, times, xattrs) must be replicated in peer-to-peer mode. Replicated FUSE handlers call `IsReplay(req, ...)` as their first statement. A replay is identified by the replayer's thread id and is never re-broadcast.
- The data plane addresses files by their cluster-wide `m_globalId`, never by local inode numbers. The listener reaches files only through the global-id index, never through the inode table.

## Engineering rules

- Keep FUSE request handlers single-reply: after `fuse_reply_*` on an error path, return immediately.
- Validate inode numbers, parent types, names, offsets, and user-provided buffer sizes before dereferencing or copying.
- Use bounded, null-terminating copies for data sent through MPI packets. Never transmit raw process-local pointers as persistent data.
- Preserve POSIX error conventions (`ENOENT`, `ENOTDIR`, `EEXIST`, and so on). Unsupported FUSE operations must fail predictably with `ENOSYS`; they must not leave a request unanswered.
- Treat the contents of `argv` as immutable. Create an owned copy before FUSE or MPI modifies command-line data.
- Avoid manual ownership ambiguity. Every `malloc`/`calloc` allocation needs one clear `free`, and array allocations use `delete[]`.

## Validation

Run the checks appropriate to the change:

```bash
cmake -S . -B build -DUSE_MPI=ON
cmake --build build
python3 -m py_compile tests/*.py python/dagonfs/*.py bench/*.py
python3 tests/test_source_contract.py
# Requires a running DAGonFS mount:
DAGONFS_MOUNTPOINT=/path/to/mount python3 tests/test_python_posix.py
git diff --check
```

Mounted tests are driven by `tests/mount.py`, which starts the job, waits for the mounts, runs the suite (and the peer-to-peer replication checks) and always unmounts:

```bash
python3 tests/mount.py --build-dir build --model client-server --ranks 4 -- --oversubscribe
python3 tests/mount.py --build-dir build --model peer-to-peer  --ranks 4 -- --oversubscribe
sbatch -p <partition> -N 2 --ntasks-per-node=2 tests/slurm/multinode.sbatch build   # multi-node
```

A new regression scenario belongs in `tests/test_python_posix.py`; a new architectural invariant belongs in `tests/test_source_contract.py`. Record validation results in `docs/TESTING.md`.

The build requires FUSE3, MPI, CMake 3.20+, a C++23 compiler, and network access on the first build for log4cplus. Mount tests require a FUSE-capable host and must always unmount the test mountpoint during cleanup. On clusters, run multi-node tests with Slurm only on nodes that provide `/dev/fuse`, and never cancel jobs you did not submit.
