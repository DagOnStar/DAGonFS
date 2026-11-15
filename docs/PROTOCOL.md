# Control-plane protocol

All messages use `MPI_COMM_WORLD` and are point-to-point. The protocol contains no collective operations. In a peer-to-peer volume several ranks originate operations concurrently, and concurrent collectives on one communicator are erroneous MPI.

Requests are plain C structures transmitted as `MPI_BYTE`. Every rank runs the same binary, so the layout is identical on all ranks; homogeneous clusters are assumed. Messages contain only value types and length-bounded byte strings. Process-local addresses are never transmitted.

Both deployment models use the same messages. They differ only in who originates them:

* **Client-server:** rank 0 is the only originator. Namespace and metadata operations are not replicated, because no other rank mounts the volume, and locks are served by rank 0's own lock table.
* **Peer-to-peer:** every peer originates the operations performed on its own mount. Lock requests go to rank 0, the lock coordinator (§3.4).

## 1. Tags

| Tag | Constant | Direction | Content |
| --- | --- | --- | --- |
| 0 | `DAGONFS_TAG_REQUEST` | originator → listener | `PeerRequest` + path bytes |
| 1 | `DAGONFS_TAG_STORE` | originator → rank | the rank's block slice of a `WRITE` |
| 2 | `DAGONFS_TAG_LOAD` | rank → originator | the rank's block slice of a `READ` |
| 3 | `DAGONFS_TAG_ACK` | rank → originator | empty; the `WRITE` has been applied |

Listeners receive only tags 0 and 1. A front end's FUSE thread receives only tags 2 and 3. A rank that is at the same time an originator and a receiver of another originator's transfer can therefore never confuse the two.

## 2. Request message

Every request is **one** message per destination: a fixed header followed by the bytes of at most two paths and of an optional value, with no terminators.

```c
typedef enum { WRITE, READ, CREATE_FILE, DELETE_FILE, CREATE_DIR, DELETE_DIR, RENAME,
               TERMINATE, LINK, DELETE, SYMLINK, SET_ATTR, SET_XATTR, REMOVE_XATTR, MKNOD,
               LOCK_SET, LOCK_GET, LOCK_RELEASE, LOCK_CANCEL, LOCK_REPLY } RequestType;

constexpr size_t DAGONFS_MAX_REQUEST_PATH  = 4096;   /* incl. the NUL of the C string */
constexpr size_t DAGONFS_MAX_REQUEST_VALUE = 65536;  /* Linux XATTR_SIZE_MAX          */

typedef struct { fuse_ino_t inode; size_t fileSize; size_t reqSize; off_t offset; } IORequestPacket;

typedef struct {                       /* SET_ATTR / SET_XATTR / MKNOD                  */
    uint32_t mask;                     /* DAGONFS_ATTR_{MODE,UID,GID,ATIME,MTIME}       */
    uint32_t mode, uid, gid;           /* mode: permission bits (07777)                 */
    int64_t  atimeSeconds, atimeNanoseconds, mtimeSeconds, mtimeNanoseconds;
    int32_t  xattrFlags;               /* XATTR_CREATE / XATTR_REPLACE                  */
    uint32_t fileType;                 /* MKNOD: S_IFIFO, S_IFSOCK, S_IFCHR or S_IFBLK   */
    uint64_t rdev;                     /* MKNOD: device number                          */
} MetadataPacket;

typedef struct {                       /* LOCK_*: request or reply                      */
    uint64_t owner;                    /* FUSE lock owner on the requesting rank        */
    uint64_t token;                    /* requester's id of the pending FUSE request    */
    int64_t  start, end;               /* inclusive byte range; end INT64_MAX = to EOF  */
    uint32_t pid;                      /* locking process (holder's kernel numbering)   */
    int32_t  type;                     /* F_RDLCK, F_WRLCK, F_UNLCK                     */
    uint32_t flags;                    /* DAGONFS_LOCK_FLOCK, DAGONFS_LOCK_WAIT         */
    int32_t  error;                    /* LOCK_REPLY: 0, EAGAIN, EDEADLK, EINTR, EINVAL */
} LockPacket;

typedef struct {
    RequestType     type;
    uint32_t        firstPathLength;   /* target / new object, < DAGONFS_MAX_REQUEST_PATH    */
    uint32_t        secondPathLength;  /* RENAME/LINK/SYMLINK destination or xattr name      */
    uint32_t        valueLength;       /* SET_XATTR value, <= DAGONFS_MAX_REQUEST_VALUE      */
    uint64_t        globalId;          /* CREATE_FILE; LOCK_*: the locked file               */
    IORequestPacket io;                /* WRITE / READ: file id and size; DELETE: file id    */
    MetadataPacket  meta;              /* SET_ATTR / SET_XATTR / MKNOD                       */
    LockPacket      lock;              /* LOCK_*                                             */
} PeerRequest;
/* wire image: PeerRequest | first path bytes | second path bytes | value bytes */
```

The listener waits with `MPI_Iprobe` and a bounded back-off of at most 0.5 ms, rather than with a blocking `MPI_Probe`. A blocking probe holds the MPI library's progress lock while it waits. Under `MPI_THREAD_MULTIPLE` this starved the FUSE thread's own MPI calls: each replicated operation took about 400 ms across nodes (see [TESTING.md](TESTING.md) §4.2).

The listener sizes its buffer with `MPI_Get_count` and receives the message from the probed source. It rejects any message whose declared lengths exceed their bounds or do not add up exactly to the received byte count, so a malformed message can never cause an out-of-bounds read. Paths are relative to the mount point and start with `/`. An operation whose path would not fit fails locally with `ENAMETOOLONG` before anything is sent.

## 3. Requests

| Request | Sent in | Message contents | Receiver action |
| --- | --- | --- | --- |
| `WRITE` | both | `io` = {file id, size} | Listener: receive the slice (tag 1), replace the local slice, update the replica's size (P2P), invalidate the kernel cache (P2P), send ACK (tag 3) |
| `READ` | both | `io` = {file id, stored size} | Listener: send the local slice (tag 2) |
| `DELETE` | client-server | `io.inode` = file id | Listener: release the local slice |
| `CREATE_FILE` | peer-to-peer | `globalId`, path | Replayer: publish `globalId`, `open(O_CREAT\|O_EXCL)` on the local mount |
| `DELETE_FILE` | peer-to-peer | path | Replayer: `unlink` (the replica's FUSE handler releases the local slice) |
| `CREATE_DIR` | peer-to-peer | path | Replayer: `mkdir` |
| `DELETE_DIR` | peer-to-peer | path | Replayer: `rmdir` |
| `RENAME` | peer-to-peer | old path, new path | Replayer: `rename` |
| `LINK` | peer-to-peer | existing path, new path | Replayer: `link` |
| `SYMLINK` | peer-to-peer | link target, new path | Replayer: `symlink` |
| `SET_ATTR` | peer-to-peer | path, `meta` (mode, uid, gid, atime, mtime as applied by the originator) | Replayer: `chmod`, `lchown`, `utimensat(AT_SYMLINK_NOFOLLOW)` for the attributes in `meta.mask` |
| `SET_XATTR` | peer-to-peer | path, attribute name, value, `meta.xattrFlags` | Replayer: `lsetxattr` |
| `REMOVE_XATTR` | peer-to-peer | path, attribute name | Replayer: `lremovexattr` |
| `MKNOD` | peer-to-peer | path, `meta.fileType`, `meta.mode`, `meta.rdev` | Replayer: `mknod` (FIFO, socket or device node; regular files travel as `CREATE_FILE`) |
| `LOCK_SET` | peer-to-peer, peer → rank 0 | `globalId`, `lock` | Listener of the coordinator: apply to the lock table; reply now, or when the lock is granted |
| `LOCK_GET` | peer-to-peer, peer → rank 0 | `globalId`, `lock` | Listener of the coordinator: reply with the first conflicting lock or `F_UNLCK` |
| `LOCK_RELEASE` | peer-to-peer, peer → rank 0 | `globalId`, `lock.owner`, `lock.flags` | Listener of the coordinator: drop the owner's locks on the file (record or flock), grant waiters, reply |
| `LOCK_CANCEL` | peer-to-peer, peer → rank 0 | `lock.token` | Listener of the coordinator: withdraw the waiting request, which is answered with `EINTR` |
| `LOCK_REPLY` | peer-to-peer, rank 0 → peer | `lock.token`, `lock.error`, conflicting lock | Listener: answer the pending FUSE request `token` (`fuse_reply_err` or `fuse_reply_lock`) |
| `TERMINATE` | both | — | Listener: leave the loop; P2P: drain the replayer, then unmount the local mount point with `fusermount3 -u` |

### 3.1 Data transfers

A `WRITE` or `READ` request is immediately followed by the transfer of the block slices (see [DATA-PLANE.md](DATA-PLANE.md)). The originator posts all its sends (`MPI_Isend`) or receives (`MPI_Irecv`) before it waits on any of them. If it sent to the ranks one at a time, it could block on a rank whose listener is serving a second originator, while that originator blocks on a rank still waiting for the first one's data. That would be a wait-for cycle. After a `WRITE`, the originator waits for one ACK from every other rank. Only then does the `flush`, `fsync` or `release` that triggered the write reply to the kernel, which provides close-to-open consistency.

### 3.2 Replays (peer-to-peer)

A replayed system call is served by the peer's own FUSE thread, and that thread may itself be waiting for this peer's listener (a slice or an ACK). The listener therefore never performs a replay. It appends the request to a FIFO served by the **replayer** thread, so replays keep the origination order.

The FUSE handlers recognise a replay by the requesting thread: `fuse_req_ctx(req)->pid` equals the replayer's kernel thread id. A replay is thus never re-broadcast, even when a local request was queued in the kernel just before it. Where thread ids are not available, an atomic flag set by the replayer is used instead.

A `WRITE` can reach a peer before its replayer has executed the matching `CREATE_FILE`. The listener then records the size as *pending* under the file id, and the replayed create applies it when it assigns that id.

### 3.3 Cluster-wide file identity

The data plane addresses a file by `globalId = ((creator rank + 1) << 48) | creator inode`. The creating rank assigns it once, and replaying peers adopt it from the `CREATE_FILE` message. Each rank keeps a mutex-protected index from global id to its `File` object. The listener uses this index to mirror the size of remotely written files without ever reading the inode table, which the FUSE thread grows concurrently. Peer-to-peer volumes never reclaim inode objects, so indexed pointers stay valid.

### 3.4 Locks

Advisory locks need a single arbiter, so a volume has one lock table ([POSIX-COMPATIBILITY.md](POSIX-COMPATIBILITY.md), "Advisory locks"). It lives in the client-server front end, and on rank 0 of a peer-to-peer volume, called the *lock coordinator*. A lock is identified by (file `globalId`, requesting rank, FUSE lock owner, record or flock), so owners of different nodes never collide.

The FUSE handlers `getlk`, `setlk` and `flock` never block the FUSE loop. Each one registers its request under a fresh *token* and submits the operation: to the local table on the coordinator, or as one `LOCK_*` message to rank 0 otherwise. The table answers every request exactly once, with a `LOCK_REPLY` addressed to (rank, token), immediately or, for a waiting `F_SETLKW` or `flock`, when the conflicting locks are released. Every release can grant waiters; their replies are sent by whichever thread applied the release. The listener of the coordinator serves `LOCK_*` messages itself, because a table operation never issues a system call on the mount point.

* **Interruption.** A waiting request installs a libfuse interrupt callback. When the waiting process receives a signal, the callback starts a short-lived thread (a reply must not be sent from inside the callback), which withdraws the waiter with `LOCK_CANCEL`; the table answers `EINTR`. If the lock was granted meanwhile, the grant is the only reply.
* **Release on close.** `flush` (every `close`) first writes the file back, then releases the record locks of its lock owner (`LOCK_RELEASE`), and replies to the kernel only after the coordinator has confirmed: a process that is granted the lock afterwards reads the data. `release` with `flock_release` drops the flock lock of the closed file description. A peer that has never sent a lock request for an owner skips the round trip. Unlocking or downgrading a lock in the peer-to-peer model also writes the file back first.
* **Deadlocks** are detected on the coordinator, which sees every waiter of the volume: a waiting record-lock request that would close a cycle of owners waiting for each other fails with `EDEADLK`.

### 3.5 Termination

When a front end's FUSE loop ends (its mount point was unmounted, or mounting failed), the front end sends `TERMINATE`:

* **Client-server:** rank 0 sends it to every storage rank.
* **Peer-to-peer:** the peer sends it to every peer, including itself. Its own listener then leaves its loop. Every other listener drains its replayer, unmounts its local mount point (which ends that peer's FUSE loop), and marks the shutdown as externally initiated so that it does not broadcast `TERMINATE` again.

## 4. Ordering guarantees

MPI guarantees that messages between a pair of processes, on the same communicator and tag, are not overtaken. Hence:

* every listener sees the requests of one originator in origination order;
* the slice of a `WRITE` is the next tag-1 message from that originator after the request;
* every replayer applies one originator's namespace operations in order, and a `WRITE` is always issued after the `CREATE_FILE` of the same file;
* the coordinator applies one peer's lock requests in order, and a `LOCK_RELEASE` caused by a `close` is sent only after the `WRITE` of the file has been acknowledged by every rank.

No order is defined between operations originated by different peers.

## 5. History

The original client-server control plane broadcast a request header and then ran `MPI_Scatterv`/`MPI_Gatherv`. The peer-to-peer control plane sent a header plus fixed 4 KiB path buffers in two messages, and ran collectives rooted at the originating peer. Both models now share the protocol above. The defects that motivated each change (D33–D40) are described in [DEFECT-ANALYSIS.md](DEFECT-ANALYSIS.md).
