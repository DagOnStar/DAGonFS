# Control-plane protocol

All messages use `MPI_COMM_WORLD` and are point-to-point. The protocol contains no collective operations. In a peer-to-peer volume several ranks originate operations concurrently, and concurrent collectives on one communicator are erroneous MPI.

Requests are plain C structures transmitted as `MPI_BYTE`. Every rank runs the same binary, so the layout is identical on all ranks; homogeneous clusters are assumed. Messages contain only value types and length-bounded byte strings. Process-local addresses are never transmitted.

Both deployment models use the same messages. They differ only in who originates them:

* **Client-server:** rank 0 is the only originator. Namespace operations are not replicated, because no other rank mounts the volume.
* **Peer-to-peer:** every peer originates the operations performed on its own mount.

## 1. Tags

| Tag | Constant | Direction | Content |
| --- | --- | --- | --- |
| 0 | `DAGONFS_TAG_REQUEST` | originator → listener | `PeerRequest` + path bytes |
| 1 | `DAGONFS_TAG_STORE` | originator → rank | the rank's block slice of a `WRITE` |
| 2 | `DAGONFS_TAG_LOAD` | rank → originator | the rank's block slice of a `READ` |
| 3 | `DAGONFS_TAG_ACK` | rank → originator | empty; the `WRITE` has been applied |

Listeners receive only tags 0 and 1. A front end's FUSE thread receives only tags 2 and 3. A rank that is at the same time an originator and a receiver of another originator's transfer can therefore never confuse the two.

## 2. Request message

Every request is **one** message per destination: a fixed header followed by the bytes of at most two paths, with no terminators.

```c
typedef enum { WRITE, READ, CREATE_FILE, DELETE_FILE, CREATE_DIR,
               DELETE_DIR, RENAME, TERMINATE, LINK, DELETE } RequestType;

constexpr size_t DAGONFS_MAX_REQUEST_PATH = 4096;   /* incl. the NUL of the C string */

typedef struct { fuse_ino_t inode; size_t fileSize; size_t reqSize; off_t offset; } IORequestPacket;

typedef struct {
    RequestType     type;
    uint32_t        firstPathLength;   /* target / new object, < DAGONFS_MAX_REQUEST_PATH */
    uint32_t        secondPathLength;  /* RENAME / LINK destination, otherwise 0          */
    uint64_t        globalId;          /* CREATE_FILE                                      */
    IORequestPacket io;                /* WRITE / READ: file id and size; DELETE: file id  */
} PeerRequest;
/* wire image: PeerRequest | first path bytes | second path bytes */
```

The listener waits with `MPI_Iprobe` and a bounded back-off of at most 0.5 ms, rather than with a blocking `MPI_Probe`. A blocking probe holds the MPI library's progress lock while it waits. Under `MPI_THREAD_MULTIPLE` this starved the FUSE thread's own MPI calls: each replicated operation took about 400 ms across nodes (see [TESTING.md](TESTING.md) §4.1).

The listener sizes its buffer with `MPI_Get_count` and receives the message from the probed source. It rejects any message whose declared lengths exceed the bound or do not add up exactly to the received byte count, so a malformed message can never cause an out-of-bounds read. Paths are relative to the mount point and start with `/`. An operation whose path would not fit fails locally with `ENAMETOOLONG` before anything is sent.

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
| `TERMINATE` | both | — | Listener: leave the loop; P2P: drain the replayer, then unmount the local mount point with `fusermount3 -u` |

### 3.1 Data transfers

A `WRITE` or `READ` request is immediately followed by the transfer of the block slices (see [DATA-PLANE.md](DATA-PLANE.md)). The originator posts all its sends (`MPI_Isend`) or receives (`MPI_Irecv`) before it waits on any of them. If it sent to the ranks one at a time, it could block on a rank whose listener is serving a second originator, while that originator blocks on a rank still waiting for the first one's data. That would be a wait-for cycle. After a `WRITE`, the originator waits for one ACK from every other rank. Only then does the `flush`, `fsync` or `release` that triggered the write reply to the kernel, which provides close-to-open consistency.

### 3.2 Replays (peer-to-peer)

A replayed system call is served by the peer's own FUSE thread, and that thread may itself be waiting for this peer's listener (a slice or an ACK). The listener therefore never performs a replay. It appends the request to a FIFO served by the **replayer** thread, so replays keep the origination order.

The FUSE handlers recognise a replay by the requesting thread: `fuse_req_ctx(req)->pid` equals the replayer's kernel thread id. A replay is thus never re-broadcast, even when a local request was queued in the kernel just before it. Where thread ids are not available, an atomic flag set by the replayer is used instead.

A `WRITE` can reach a peer before its replayer has executed the matching `CREATE_FILE`. The listener then records the size as *pending* under the file id, and the replayed create applies it when it assigns that id.

### 3.3 Cluster-wide file identity

The data plane addresses a file by `globalId = ((creator rank + 1) << 48) | creator inode`. The creating rank assigns it once, and replaying peers adopt it from the `CREATE_FILE` message. Each rank keeps a mutex-protected index from global id to its `File` object. The listener uses this index to mirror the size of remotely written files without ever reading the inode table, which the FUSE thread grows concurrently. Peer-to-peer volumes never reclaim inode objects, so indexed pointers stay valid.

### 3.4 Termination

When a front end's FUSE loop ends (its mount point was unmounted, or mounting failed), the front end sends `TERMINATE`:

* **Client-server:** rank 0 sends it to every storage rank.
* **Peer-to-peer:** the peer sends it to every peer, including itself. Its own listener then leaves its loop. Every other listener drains its replayer, unmounts its local mount point (which ends that peer's FUSE loop), and marks the shutdown as externally initiated so that it does not broadcast `TERMINATE` again.

## 4. Ordering guarantees

MPI guarantees that messages between a pair of processes, on the same communicator and tag, are not overtaken. Hence:

* every listener sees the requests of one originator in origination order;
* the slice of a `WRITE` is the next tag-1 message from that originator after the request;
* every replayer applies one originator's namespace operations in order, and a `WRITE` is always issued after the `CREATE_FILE` of the same file.

No order is defined between operations originated by different peers.

## 5. History

The original client-server control plane broadcast a request header and then ran `MPI_Scatterv`/`MPI_Gatherv`. The peer-to-peer control plane sent a header plus fixed 4 KiB path buffers in two messages, and ran collectives rooted at the originating peer. Both models now share the protocol above. The defects that motivated each change (D33–D39) are described in [DEFECT-ANALYSIS.md](DEFECT-ANALYSIS.md).
