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

The build produces one executable for both deployment models, `dagonfs`, plus `DAGonFS_Launcher`. `DAGonFS_CS.exe` and `DAGonFS_P2P.exe` are created as symbolic links to `dagonfs` for backward compatibility; each alias defaults to its model. The volume API is installed as `include/dagonfs/{Options,Volume}.hpp` with the static libraries `libdagonfs_core.a` and `libdagonfs_options.a`. The log4cplus shared library is installed under `lib/` with a relative RPATH, so the installed tree can be relocated or staged to compute nodes as a unit. The manual page, `docs/man/dagonfs.1`, is installed as `share/man/man1/dagonfs.1` (with `DAGonFS_Launcher.1` as a link to it), and the Markdown documentation goes to `share/doc/DAGonFS`. Read the page without installing it with `man docs/man/dagonfs.1`.

## 3. Creating a volume

A volume is created by running `dagonfs` on every rank of an MPI job. The deployment model is chosen **per volume, at run time**:

```bash
mpirun -np <P> [--hostfile <file>] dagonfs [--model client-server|peer-to-peer] [--config DAGonFS.ini] [-f|-d] [--log] <root>
```

| Option | Meaning |
| --- | --- |
| `-m`, `--model M` | `client-server` (`cs`): rank 0 mounts `<root>`, ranks 1…P−1 store blocks. `peer-to-peer` (`p2p`): rank *r* mounts `<root>/<r>`, and the namespace is replicated. Default: `client-server`. |
| `-c`, `--config F` | Read `dagonfs_model`, `dagonfs_root_dir`, `fuse_mode` and `enable_logging` from an ini file. |
| `-f` / `-d` | FUSE in the foreground (default; required under MPI) / foreground with a trace of every FUSE request. |
| `--log` | Verbose log4cplus output. The legacy form is a trailing `1` (`0` disables). |
| `<root>` | Mount point (client-server) or parent of the per-rank mount points (peer-to-peer). May be omitted if the ini file sets `dagonfs_root_dir`. |

Precedence, from lowest to highest: built-in defaults; the program name (`DAGonFS_CS.exe` / `DAGonFS_P2P.exe`); the `--config` file; explicit flags. The legacy invocations `mpirun -np P DAGonFS_CS.exe -f <root> [0|1]` and `… DAGonFS_P2P.exe …` therefore keep working unchanged.

Mount points, and any missing parent directories, are created on each node. Startup failures, for instance an existing non-empty mount point or a missing `/dev/fuse`, are always reported on standard error with the rank number, whatever the logging flag. A rank that fails before mounting still notifies the others, so the job never hangs. A peer-to-peer volume requires an MPI library with `MPI_THREAD_MULTIPLE`; `dagonfs` requests it only for that model.

### 3.1 From C++

```cpp
#include <dagonfs/Volume.hpp>

dagonfs::VolumeOptions options;
options.model = dagonfs::Model::PeerToPeer;
options.root = "/tmp/dagonfs";
int provided;
MPI_Init_thread(&argc, &argv, dagonfs::requiredThreadLevel(options.model), &provided);
int status = dagonfs::runVolume(options);   // collective; returns when the volume is unmounted
MPI_Finalize();
```

Link with `dagonfs_core` (and FUSE3, MPI and log4cplus). `dagonfs::readIni`, `applyIni` and `parseCommandLine` expose the same configuration handling as the executable.

### 3.2 From Python

`python/dagonfs` is a dependency-free module that creates and destroys volumes from Python, for example from a DAGonStar workflow script:

```python
import sys; sys.path.insert(0, "python")
from dagonfs import Volume

with Volume("/tmp/dagonfs", model="peer-to-peer", ranks=4, executable="build/dagonfs",
            mpirun_args=["--hostfile", "hosts"], local_ranks=(0,)) as volume:
    scratch = volume.mountpoint(0)     # /tmp/dagonfs/0
    ...
# leaving the block unmounts the volume and waits for the MPI job
```

`Volume.start()` returns once the mount points of `local_ranks`, those on the calling host, are mounted. `Volume.stop()` unmounts them, which shuts the whole volume down, and returns the job's exit status.

## 4. The launcher

`DAGonFS_Launcher [path/to/DAGonFS.ini] [-m|--model M] [-n|--dry-run]` reads a configuration file (default `./DAGonFS.ini`), validates it, and runs the corresponding `mpirun` command through `execvp`. No shell is involved, so values containing spaces or shell metacharacters are passed literally. `--model` overrides `dagonfs_model`, and `--dry-run` prints the command instead of running it.

```ini
[DAGonFS_config]
num_processes = 8              # positive integer
machinefile =                  # optional Open MPI hostfile
dagonfs_model = client-server  # client-server | peer-to-peer
fuse_mode = f                  # f (foreground) | d (foreground + FUSE debug)
dagonfs_root_dir = /tmp/DAGonFS
enable_logging = false         # true | false
mpirun = mpirun                # launcher command (e.g. srun, mpiexec)
executable =                   # default: dagonfs next to the launcher, else from PATH
```

| Key | Used by | Meaning |
| --- | --- | --- |
| `num_processes` | launcher | Number of MPI ranks *P* (required). |
| `machinefile` | launcher | Adds `--hostfile <file>` when non-empty. |
| `mpirun` | launcher | Command used to start the job (default `mpirun`). |
| `executable` | launcher | Path of `dagonfs`. |
| `dagonfs_model` | launcher, `dagonfs --config` | `client-server` (default) or `peer-to-peer`. |
| `dagonfs_root_dir` | launcher, `dagonfs --config` | Mount point / parent of the per-rank mount points (required). |
| `fuse_mode` | launcher, `dagonfs --config` | `f` (default) or `d`. |
| `enable_logging` | launcher, `dagonfs --config` | `true` or `false` (default). |

Example:

```bash
cd build && cp ../src/DAGonFS.ini . && ./DAGonFS_Launcher --model peer-to-peer --dry-run
# Executing: mpirun -np 8 ./dagonfs --model peer-to-peer -f /tmp/DAGonFS
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
