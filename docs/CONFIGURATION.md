# Configuration and operation

## Prerequisites

Build hosts need CMake 3.20 or newer, a C++23 compiler, FUSE3 development headers and libraries, and an MPI implementation compatible with the target cluster. The first CMake build also downloads and builds log4cplus. On Linux, users normally need permission to mount FUSE filesystems; verify the site policy before running on a shared cluster.

`DAGonFS_Launcher` reads `DAGonFS.ini` from its current directory. Values accept either `key=value` or `key = value` syntax; blank values are allowed for `machinefile`.

```ini
[DAGonFS_config]
num_processes = 8
machinefile =
dagonfs_model = client-server
fuse_mode = f
dagonfs_root_dir = /tmp/DAGonFS
enable_logging = true
```

- `num_processes`: number of MPI ranks.
- `machinefile`: optional Open MPI hostfile.
- `dagonfs_model`: `client-server` or `peer-to-peer`.
- `fuse_mode`: FUSE mode argument, normally `f` for foreground execution.
- `dagonfs_root_dir`: mountpoint parent. Peer-to-peer creates a directory per rank below it.
- `enable_logging`: enables verbose log4cplus output when `true`.

## Reproducible local example

From the repository root, configure and build both models:

```bash
cmake -S . -B build -DUSE_MPI=ON -DCMAKE_BUILD_TYPE=Release
cmake --build build --parallel
mkdir -p run
cp src/DAGonFS.ini build/DAGonFS_CS.exe build/DAGonFS_P2P.exe build/DAGonFS_Launcher run/
cd run
```

Edit `DAGonFS.ini` so that `num_processes` is suitable for the available machine and `dagonfs_root_dir` is an empty, writable path. Start the launcher in foreground mode:

```bash
./DAGonFS_Launcher
```

Unmount with the platform’s FUSE tool (for example, `fusermount3 -u /tmp/DAGonFS` on Linux) before terminating MPI processes. In peer-to-peer mode, unmount each rank-specific mountpoint created below `dagonfs_root_dir`.

## Cluster example

Set `machinefile` to an Open MPI hostfile and choose `peer-to-peer` when every allocated node should expose a mountpoint. The launcher builds an `mpirun -np <num_processes>` command, adding `--hostfile <machinefile>` when the field is non-empty. Ensure the three executables and configuration file are visible at the same path on all hosts, or stage them with the scheduler before launch.

Do not place a DAGonFS mount inside a directory managed by a cleanup job while it is active. An external removal of the mountpoint is a service failure, not a supported shutdown protocol.
