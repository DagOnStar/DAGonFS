# Configuration and operation

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

Build the project, copy the launcher, configuration file, and selected executables to the execution directory, then invoke `./DAGonFS_Launcher`. Unmount with the platform’s FUSE tool (for example, `fusermount3 -u <mountpoint>` on Linux) before terminating MPI processes.
