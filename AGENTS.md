# DAGonFS contributor guide

## Project layout

- `src/client-server/` contains the single FUSE front end and MPI storage workers.
- `src/peer-to-peer/` contains the distributed FUSE front end used by every peer.
- `doc/` contains the installed manual page; `docs/` contains maintained project documentation.
- `tests/` contains smoke-test helpers. Do not require a mounted filesystem in unit tests.

The two implementations intentionally mirror each other. Changes to shared filesystem behaviour must be applied and reviewed in both trees unless the difference is intrinsic to the distribution model.

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

The build requires FUSE3, MPI, CMake 3.20+, a C++23 compiler, and network access on the first build for log4cplus. Mount tests require a FUSE-capable host and must always unmount the test mountpoint during cleanup.
