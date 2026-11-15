# DAGonFS contributor guide

## Project layout

- `src/client-server/` contains the single FUSE front end and MPI storage workers.
- `src/peer-to-peer/` contains the distributed FUSE front end used by every peer.
- `doc/` contains the installed manual page; `docs/` contains maintained project documentation.
- `tests/` contains smoke-test helpers. Do not require a mounted filesystem in unit tests.

The two implementations intentionally mirror each other. Changes to shared filesystem behaviour must be applied and reviewed in both trees unless the difference is intrinsic to the distribution model.

- The following files are **byte-identical** in both trees and must stay so (enforced by `tests/test_source_contract.py`): `blocks/*`, `mpi/DataBlockManager.*`, `nodes/*`, `ramfs/FileSystem.*`, `utils/ArgumentParser.*`, `utils/fuse_headers.hpp`. Edit one copy, then copy it to the other tree.
- Model-specific behaviour inside shared files is selected with `#ifdef DAGONFS_P2P`, defined only for the peer-to-peer target in `CMakeLists.txt`.
- Model-specific code lives in `mpi/` only: `MasterProcessCode`/`NodeProcessCode` (client-server) and `DistributedCode`/`RequestSender` (peer-to-peer).
- Peer-to-peer requests are one message each (`PeerRequest` header + path bytes); see `docs/PROTOCOL.md` before changing the wire format, and update the contract tests with it.
- The data plane addresses peer-to-peer files by their cluster-wide `m_globalId`, never by local inode numbers. Replicated FUSE handlers consume their replay flag as the first statement.

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
python3 -m py_compile tests/mount.py tests/usage.py
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

The build requires FUSE3, MPI, CMake 3.20+, a C++23 compiler, and network access on the first build for log4cplus. Mount tests require a FUSE-capable host and must always unmount the test mountpoint during cleanup.
