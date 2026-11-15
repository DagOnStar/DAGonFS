# Data plane

This document specifies how DAGonFS places file contents in the memory of the MPI ranks and how the content is moved between the front-end buffer and the distributed copy. The data plane (`DataBlockManager`, `Blocks`, `DataBlock`) is shared verbatim by both deployment models.

## 1. Block decomposition

Let $S$ be the size in bytes of the content to store and $B$ the block size (`FILE_SYSTEM_SINGLE_BLOCK_SIZE`, $B = 131072$). The content is padded with zeros to

$$N = \left\lceil S / B \right\rceil$$

whole blocks. Padding keeps every transfer a multiple of a block-sized MPI datatype and makes the stored image deterministic.

## 2. Contiguous block layout

Given $P$ ranks, rank $r \in \{0,\ldots,P-1\}$ owns

$$c_r = \left\lfloor N / P \right\rfloor + [\, r < N \bmod P \,]$$

consecutive blocks starting at index

$$d_r = \sum_{q<r} c_q .$$

The layout is a pure function of $(N, P)$. This property is central to the design: a read only needs the *stored size* $S$ of the file — a value type carried in a 32-byte request header — to reconstruct where every block lives. Consequently, no rank ever needs to know an address in another process's memory.

![Contiguous block distribution of a file over four ranks](../figures/block-distribution.svg)

The layout balances memory within one block per rank ($\max_r c_r - \min_r c_r \le 1$) and gives rank 0 at least as many blocks as any other rank. An immediate corollary used by the implementation is that if rank 0 stores no block of a file, no rank does.

## 3. Transfers

Both operations involve every rank. The *originator* (root) is rank 0 in a client-server volume and the peer that performed the operation in a peer-to-peer volume. Counts are expressed in units of a committed contiguous datatype of $B$ bytes, so a file of up to $2^{31}-1$ blocks (256 TiB) can be described without overflowing MPI's `int` counts.

The transfers are **point-to-point**, on dedicated tags (see [PROTOCOL.md](PROTOCOL.md) §1), and not MPI collectives. In a peer-to-peer volume two peers can originate transfers at the same time, so each listener would take part in collectives rooted at different peers while the peers' FUSE threads run their own. Concurrent collectives on one communicator are erroneous MPI. Under the original collective design they produced files that read back with the right size but a mixture of old and new blocks. A linear point-to-point transfer has the same cost as the linear algorithm that MPI libraries typically use for `MPI_Scatterv`/`MPI_Gatherv`.

### 3.1 `scatterStore` (write-back)

1. The root holds the front-end buffer, of at least $N \cdot B$ bytes, with the padding zeroed.
2. Every rank allocates $c_r$ fresh `DataBlock`s.
3. The root posts one `MPI_Isend` per rank, of $c_r$ blocks from offset $d_r B$ of its buffer, copies its own slice locally, and waits for all sends together.
4. Each other rank describes its new blocks with an `MPI_Type_create_hindexed` datatype built from their absolute addresses, and receives its slice directly into them (`MPI_BOTTOM`). No staging buffer and no extra copy are needed.
5. Each rank atomically replaces its previous slice of the file in the `Blocks` registry and frees the old blocks, then acknowledges the write to the root.

A size of zero releases the slices on every rank. Because each write replaces the whole slice, the number of blocks held for a file always equals its current layout, and rewriting a file never leaks memory.

### 3.2 `gatherLoad` (read)

1. Every rank looks up its slice for the file. If the slice is shorter than $c_r$, which can only happen after an inconsistent history, it is padded with zero blocks so the transfer can always complete.
2. The root allocates the buffer, posts one `MPI_Irecv` per rank into offset $d_r B$, copies its own slice, and waits for all receives together.
3. Each other rank sends its slice in place, through an `hindexed` datatype from `MPI_BOTTOM`.

### 3.3 Release

When the last name of a file disappears and no descriptor is open, its blocks are released. In the client-server model rank 0 releases its own slice and sends a `DELETE` request to the storage ranks. In the peer-to-peer model every peer replays the `unlink` and releases its own slice locally, so no message is required.

## 4. Ownership and safety invariants

* **I1 — Ownership.** A `DataBlock` allocates its buffer in its constructor and frees it in its destructor; it is non-copyable and never adopts a foreign address.
* **I2 — Locality.** The registry of a rank contains only blocks allocated by that rank.
* **I3 — Value-only messages.** MPI packets contain integers and length-bounded byte strings. Process-local pointers are never transmitted (enforced by `tests/test_source_contract.py`).
* **I4 — Layout determinism.** The block layout used by a read is computed from the stored size recorded by the preceding write, so writer and readers agree on block ownership without exchanging metadata.
* **I5 — Thread safety.** All registry operations take an internal mutex, because in the peer-to-peer model the FUSE thread and the listener thread of a peer access the registry concurrently.
* **I6 — No collectives.** Data transfers are point-to-point, and an originator posts all of them before waiting on any, so transfers of concurrent originators can neither be mismatched nor wait for each other in a cycle.

The original implementation violated I1–I3: ranks exchanged raw block addresses, the front end stored addresses belonging to other processes, and overwriting a block freed the previous address — which, for a block owned by another rank, is an invalid `free()` in the wrong address space. See [DEFECT-ANALYSIS.md](DEFECT-ANALYSIS.md) for the full account.

## 5. Cost model

For a store of $S$ bytes over $P$ ranks, with a linear-cost network model (latency $\alpha$, inverse bandwidth $\beta$),

$$T_{store}(S) \approx \underbrace{\alpha (P-1)}_{\text{requests}} + \underbrace{\alpha (P-1) + \beta\,\frac{P-1}{P}\,N B}_{\text{slices}} + \underbrace{\alpha}_{\text{acks (overlapped)}} .$$

A load has the same cost without the acknowledgements. The front-end rank keeps $1/P$ of the data locally, so only the fraction $(P-1)/P$ crosses the network. The per-operation constant terms dominate for small files. This is why DAGonFS synchronises at `close`/`fsync` rather than on every `write`.
