# Configuration and operation

## 1. Prerequisites

| Component | Requirement | Notes |
| --- | --- | --- |
| Build system | CMake ≥ 3.20 | The first configure downloads and builds log4cplus (network access required once). |
| Compiler | C++23 (GCC ≥ 12, Clang ≥ 16) | Both executables build with `-Wall -Wextra`. |
| MPI | Any MPI-3 implementation; the peer-to-peer model requires `MPI_THREAD_MULTIPLE` | Tested with Open MPI 4.1. |
| FUSE | libfuse ≥ 3.3 headers and runtime, `/dev/fuse`, set-uid `fusermount3` | On macOS, macFUSE provides the headers; mounting is untested there. |

## 2. Building

```bash
cmake -S . -B build -DUSE_MPI=ON -DCMAKE_BUILD_TYPE=Release
cmake --build build --parallel
ctest --test-dir build --output-on-failure
cmake --install build --prefix "$HOME/opt/dagonfs"   # optional
```

The build produces `DAGonFS_CS.exe` (client-server), `DAGonFS_P2P.exe` (peer-to-peer, compiled with `-DDAGONFS_P2P`) and `DAGonFS_Launcher`. Installation places the log4cplus shared library under `lib/` and sets a relative RPATH, so the installed tree can be relocated or staged to compute nodes as a unit.

## 3. Running an executable directly

```bash
mpirun -np <P> [--hostfile <file>] ./DAGonFS_CS.exe  -f <mountpoint> [0|1]
mpirun -np <P> [--hostfile <file>] ./DAGonFS_P2P.exe -f <root>       [0|1]
```

* `-f` runs FUSE in the foreground, which is required under MPI because the rank must stay alive inside `mpirun`; `-d` additionally prints the FUSE request trace.
* The trailing `1` enables log4cplus logging; `0` or nothing disables it.
* **Client-server:** rank 0 mounts `<mountpoint>`; ranks 1…P−1 are storage workers.
* **Peer-to-peer:** rank *r* mounts `<root>/<r>`. Each peer's mount shows the same namespace; files written on any peer are readable on every other peer after `close`.

Mount points, and any missing parent directories, are created on each node. Startup failures (for instance an existing non-empty mount point or a missing `/dev/fuse`) are always reported on standard error with the rank number, regardless of the logging flag. If a rank fails before mounting it still notifies the others, so the job never hangs.

## 4. The launcher

`DAGonFS_Launcher [path/to/DAGonFS.ini] [-n|--dry-run]` reads a configuration file (default `./DAGonFS.ini`), validates it, and replaces itself with the corresponding `mpirun` command via `execvp`. No shell is involved, so values containing spaces or shell metacharacters are passed literally. `--dry-run` prints the command instead of running it.

```ini
[DAGonFS_config]
num_processes = 8            # positive integer
machinefile =                # optional Open MPI hostfile
dagonfs_model = client-server  # client-server | peer-to-peer
fuse_mode = f                # f (foreground) | d (foreground + FUSE debug)
dagonfs_root_dir = /tmp/DAGonFS
enable_logging = false       # true | false
mpirun = mpirun              # launcher command (e.g. srun, mpiexec)
```

| Key | Required | Meaning |
| --- | --- | --- |
| `num_processes` | yes | Number of MPI ranks *P*. |
| `machinefile` | no | Adds `--hostfile <file>` when non-empty. |
| `dagonfs_model` | no (default `client-server`) | Selects `./DAGonFS_CS.exe` or `./DAGonFS_P2P.exe`, resolved relative to the working directory. |
| `fuse_mode` | no (default `f`) | `f` or `d`. |
| `dagonfs_root_dir` | yes | Mount point (client-server) or parent of per-rank mount points (peer-to-peer). |
| `enable_logging` | no (default `false`) | Passes `1` or `0` as the last argument. |
| `mpirun` | no (default `mpirun`) | Command used to start the job. |

Example:

```bash
cd build && cp ../src/DAGonFS.ini . && ./DAGonFS_Launcher --dry-run
# mpirun -np 8 ./DAGonFS_CS.exe -f /tmp/DAGonFS 0
```

## 5. Shutdown

Unmount the front end to stop the service:

```bash
fusermount3 -u /tmp/DAGonFS          # client-server
fusermount3 -u /tmp/DAGonFS/0        # peer-to-peer: any one peer
```

In client-server mode rank 0 then sends `TERMINATE` to the storage ranks. In peer-to-peer mode unmounting any peer makes it broadcast `TERMINATE`; every other peer unmounts its own mount point with `fusermount3 -u` and exits. Killing the MPI job instead leaves stale mount points (`Transport endpoint is not connected`), which must be removed with `fusermount3 -u` (or `-uz`).

## 6. Cluster use

* The executables, the launcher configuration and the mount root must be valid paths on every node; stage the installed tree with the scheduler or use a shared file system.
* Every node needs FUSE. On clusters where only some node classes provide it, select them explicitly (`-w`, `--constraint`).
* Do not place an active mount inside a directory managed by a cleanup job; external removal of the mount point is a failure, not a shutdown protocol.
* DAGonFS keeps all data in memory. The aggregate capacity is the sum of the ranks' free memory; contents are lost when the job ends.

See [TESTING.md](TESTING.md) for the Slurm script used to validate multi-node deployments.
