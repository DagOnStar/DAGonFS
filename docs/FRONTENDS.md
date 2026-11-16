# DAGonFS frontends

DAGonFS exposes the same distributed volume through selectable frontends:

| Frontend | Selector | Kernel FUSE required | Intended use |
|---|---|---:|---|
| FUSE3 | `--frontend fuse3` (default) | Yes | Transparent POSIX paths for existing applications |
| Native | `--frontend native` | No | FUSE-free clients and MPI-IO-aware applications |
| LD_PRELOAD | `--frontend preload` | No | Opt-in interception of file calls below a configured path |

Native and preload modes use a versioned Unix-domain socket. Start the volume with
the socket path as `root`, then set `DAGONFS_SOCKET` for the C client or
`DAGONFS_PRELOAD_ROOT` and `DAGONFS_SOCKET` for the interposer. The interposer is
deliberately opt-in and delegates paths outside `DAGONFS_PRELOAD_ROOT` to libc.
It currently covers open/close/read/write/pread/pwrite and a conservative file
operation subset; applications requiring complete POSIX semantics should use FUSE3.

Example:

```sh
mpirun -np 4 dagonfs --frontend native /tmp/dagonfs.sock
export DAGONFS_SOCKET=/tmp/dagonfs.sock
export DAGONFS_PRELOAD_ROOT=/dagonfs
export DYLD_INSERT_LIBRARIES=/path/to/libdagonfs_preload.dylib # macOS
export LD_PRELOAD=/path/to/libdagonfs_preload.so              # Linux
```

Native mode is a protocol/API frontend, not a transparent mounted filesystem.
It is therefore suitable for MPI-IO integration and controlled workflows, but
does not promise that arbitrary binaries behave exactly as they do on FUSE.
