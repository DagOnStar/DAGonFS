# Architecture

DAGonFS is an in-memory FUSE filesystem with MPI-backed data distribution. It offers two deployment models.

| Model | FUSE process | Data coordination |
| --- | --- | --- |
| Client-server | Rank 0 mounts the filesystem. | Rank 0 distributes block work to storage ranks. |
| Peer-to-peer | Every rank mounts a rank-specific directory. | Peers broadcast metadata requests and exchange blocks collectively. |

Metadata is represented by `INode` subclasses (`File`, `Directory`, `SymbolicLink`, and `SpecialINode`). File payload is divided into `DataBlock` objects and coordinated by the MPI code. The registered FUSE callbacks live in `src/*/include/ramfs/FileSystem.cpp`; the duplicated trees must retain equivalent POSIX semantics.

The mountpoint is volatile: all metadata and file contents disappear when the process exits or the mount is removed. `fsync` and `fsyncdir` therefore acknowledge ordering within the in-memory service; they do not make data durable on a local disk.
