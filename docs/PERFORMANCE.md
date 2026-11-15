# Performance methodology and tuning

## 1. What DAGonFS optimizes

DAGonFS removes persistent-storage latency from the workflow scratch path and distributes payload blocks across MPI ranks. The expected gain is largest when a workflow repeatedly creates, reads, and writes temporary files whose working set fits in aggregate RAM. It is not a universal replacement for a durable parallel filesystem.

The observed time for an operation can be decomposed conceptually as

\[
T_{operation} = T_{FUSE} + T_{metadata} + T_{MPI} + T_{memory}.
\]

For small files, FUSE crossings and metadata management dominate. For large files, MPI communication and memory-copy costs dominate. This model is useful when interpreting measurements: a bandwidth result should include the file size, block size, rank count, host topology, and whether caches were warm.

## 2. Measured operation latency

Table 1 reports the mean latency per operation in a peer-to-peer volume, measured on two nodes with two ranks each, from rank 0's mount (see [TESTING.md](TESTING.md) §4.2).

**Table 1 — peer-to-peer latency per operation across two nodes.**

| Operation | Before D33 | Current |
| --- | --- | --- |
| `mkdir` / `rmdir` | 363 / 450 ms | 0.43 / 0.03 ms |
| create / `unlink` | 427 / 415 ms | 0.07 / 0.03 ms |
| write 4 KiB / read 4 KiB | 373 / 476 ms | 0.26 / 0.20 ms |

The earlier figures were dominated by the listener's blocking `MPI_Probe`. Under `MPI_THREAD_MULTIPLE` it held the MPI progress engine and delayed the front end's own messages. The listener now polls with `MPI_Iprobe` and an exponential back-off capped at 0.5 ms. An idle peer therefore costs a fraction of a core, and a request waits at most 0.5 ms before it is picked up. Namespace replays are asynchronous and do not add to the originator's latency. A write, however, waits for an acknowledgement from every rank.

## 3. Deployment guidance

- Use `Release` builds for measurement; debug logging and sanitizers materially alter timing.
- Select a rank count that matches available CPU and memory resources. Additional ranks may increase MPI coordination overhead for small files.
- Keep the workflow scratch set within aggregate RAM. Swapping reverses the intended performance advantage.
- Group very small temporary files when application semantics allow it; namespace operations have fixed FUSE and metadata overhead.
- Avoid treating `fsync` as durable persistence. Export results to a persistent filesystem before a job ends.

## 4. DAGonFS versus NFS as DAGonStar scratch space

This section compares DAGonFS with the file system that HPC workflows most often use for scratch data: the NFS-mounted home directory. Node-local `tmpfs` is included as an upper bound. It is the fastest possible scratch space, but it is private to one node and cannot be shared by the tasks of a multi-node workflow.

### 4.1 Testbed

| Item | Value |
| --- | --- |
| Compute nodes | Slurm partition `low-wn`: 2 × Intel Xeon Gold 5218 (32 cores), 187 GB RAM, InfiniBand (mlx5), Rocky Linux 8.10 (kernel 4.18.0-553) |
| FUSE | libfuse 3.3.0 runtime; only the FUSE 2 helper `fusermount` 2.9.7 is installed, used through `tests/slurm/fuse-env.sh` |
| NFS | `/home`, NFS 4.2 over TCP, `rsize = wsize = 1 MiB`, hard mount, served by a dedicated file server |
| tmpfs | `/dev/shm` on the compute node |
| DAGonFS | 4 ranks, Open MPI 4.1.4 (UCX 1.15), Release build, 128 KiB blocks, `--map-by slot --bind-to none` |
| Workflow engine | DAGonStar 1.0.2, Python 3.11.13, local batch tasks |
| One node | Slurm job 19453 on wn01, 4 ranks, `--mem=96G`; the node was shared with other users' jobs |
| Two nodes | Slurm job 19454 on wn02–wn03, 2 ranks per node, `--mem=96G` per node |

Both jobs ran on 15 November 2026 and measured every target. In the two-node job, NFS and tmpfs are still used from the first node, so only the DAGonFS rows of that job differ in kind: their blocks are spread over both nodes, and half of every transfer crosses InfiniBand.

### 4.2 Method

`bench/run.py` measures each target in turn with the same two programs:

* `bench/fsbench.py` runs micro-benchmarks, five repetitions each, reported as the median:
  * creation, `stat` and removal of 2000 files, and creation and removal of 2000 directories;
  * write-then-read of 1000 files of 4 KiB;
  * sequential write (ending with `fsync`) and read of files of 1 to 512 MiB, in 1 MiB requests.

  Before each read, the file's pages are dropped from the client page cache (`posix_fadvise(POSIX_FADV_DONTNEED)`). An NFS read therefore crosses the network instead of being served by the page cache that the preceding write just filled.
* `bench/workflow.py` runs three **real DAGonStar workflows**, three times each. The workflow scratch directory (`scratch_dir_base`) is placed on the target. Every dependency is a `workflow://` reference, which DAGonStar stages by copying the producer's output into the consumer's directory.
  * **Large files:** a producer writes 8 files of 128 MiB with `dd`. Eight parallel tasks each stage one file, copy it, and write a 256 MiB output with `cat`. A reducer stages the eight outputs and concatenates them into 2 GiB. About 9 GiB are written to the file system, and as much is read back.
  * **Python tasks:** the same dataflow, but every task is a Python program (`bench/pytask.py`) that reads and writes its files with Python file objects in 1 MiB requests. The transforms write each chunk twice and `fsync` their output. The reducer recomputes the expected content of every chunk and compares it before concatenating, so the run fails if any task read, or DAGonStar staged, wrong data.
  * **Small files:** a producer writes 2000 files of 8 KiB. Eight parallel tasks each stage the whole directory and pack it into a tar archive. A reducer stages and concatenates the archives. The run performs about 18 000 file creations.

  The reducer's output size is checked after every run, so an incomplete or failed workflow cannot be reported as a fast one.

DAGonFS volumes are created and destroyed for each target through the Python volume API (`python/dagonfs`). The peer-to-peer volume is used through rank 0's mount point, so it pays for replicating every operation to three peers.

### 4.3 Micro-benchmarks

![Metadata operations](../figures/bench-metadata.png)

![Small files](../figures/bench-small-files.png)

![Sequential throughput](../figures/bench-throughput.png)

**Table 2 — micro-benchmarks, median of five runs.** "CS" is client-server, "P2P" peer-to-peer; the last column compares DAGonFS client-server with NFS on one node.

| Metric | NFS | tmpfs | CS, 1 node | P2P, 1 node | CS, 2 nodes | P2P, 2 nodes | CS / NFS |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| create (ops/s) | 230 | 274,889 | 40,172 | 13,525 | 41,590 | 16,955 | 175× |
| stat (ops/s) | 1,203 | 710,426 | 649,083 | 41,736 | 651,289 | 42,965 | 540× |
| unlink (ops/s) | 930 | 698,078 | 153,382 | 19,197 | 158,785 | 23,443 | 165× |
| mkdir (ops/s) | 731 | 387,011 | 62,479 | 17,567 | 63,641 | 25,158 | 85× |
| rmdir (ops/s) | 693 | 745,126 | 161,323 | 19,589 | 164,078 | 25,445 | 233× |
| 4 KiB files, write (files/s) | 184 | 121,827 | 5,969 | 4,470 | 5,919 | 3,770 | 32× |
| 4 KiB files, read (files/s) | 267 | 178,592 | 6,589 | 5,559 | 6,559 | 5,164 | 25× |
| 1 MiB write (MiB/s) | 90 | 5,493 | 1,102 | 859 | 910 | 614 | 12× |
| 1 MiB read (MiB/s) | 76 | 2,871 | 1,352 | 1,191 | 1,114 | 1,079 | 18× |
| 16 MiB write (MiB/s) | 105 | 4,720 | 1,105 | 1,086 | 972 | 904 | 11× |
| 16 MiB read (MiB/s) | 158 | 5,205 | 1,720 | 1,713 | 1,597 | 1,459 | 11× |
| 128 MiB write (MiB/s) | 492 | 3,210 | 792 | 773 | 742 | 727 | 1.6× |
| 128 MiB read (MiB/s) | 259 | 4,910 | 1,450 | 1,485 | 1,341 | 1,385 | 5.6× |
| 512 MiB write (MiB/s) | 484 | 3,174 | 809 | 790 | 753 | 746 | 1.7× |
| 512 MiB read (MiB/s) | 303 | 5,036 | 1,467 | 1,411 | 1,385 | 1,250 | 4.8× |

NFS and tmpfs are reported from the one-node job; the two-node job measured them within 15% of these values.

*Comments.*

* **Metadata.** NFS pays a network round trip to the server for every create, remove and attribute check, so it sustains only 230–1200 operations per second. DAGonFS answers these operations from the front end's memory and is faster by two orders of magnitude.
  * The client-server `stat` rate (650 000 ops/s) comes close to tmpfs because the kernel caches attributes for 1 s, so most `stat` calls never reach the file system.
  * Peer-to-peer volumes disable that cache (D38) and replicate every namespace change to the other peers. They are 2.5–16× slower than client-server volumes on metadata, but still 24–59× faster than NFS.
  * Spreading the ranks over two nodes does not slow metadata down: namespace operations are answered by the front end, and replays are asynchronous. The one-node peer-to-peer figures are even 10–40% lower than the two-node ones, because wn01 was shared with other jobs while wn02–wn03 were idle.
* **Small files.** A 4 KiB file costs one round trip per `open`, `write`/`read` and `close` on NFS (about 5 ms per file). On DAGonFS it costs three FUSE crossings plus one point-to-point exchange with the storage ranks, about 0.17 ms.
* **Large files.** DAGonFS writes at 0.8–1.1 GiB/s and reads at 1.3–1.7 GiB/s, against 0.08–0.5 GiB/s for NFS. For large files, three costs keep it at a quarter to a third of tmpfs: every request crosses the FUSE boundary twice, the front end buffers the whole file, and the data are copied once more to distribute them over the ranks.
  * Across two nodes, where half the blocks travel over InfiniBand, throughput drops by 5–30%, most for 1 MiB files, whose transfer time is dominated by per-message latency.
  * The NFS write rate rises with file size because NFS writes back asynchronously until the final `fsync`. Its read rate, 76–303 MiB/s, reflects the server's response once the client cache has been dropped.

### 4.4 DAGonStar workflows

![DAGonStar workflows](../figures/bench-workflow.png)

**Table 3 — DAGonStar workflow wall time, median of three runs (min–max).**

| Workflow | NFS | tmpfs | DAGonFS CS, 1 node | DAGonFS P2P, 1 node | DAGonFS CS, 2 nodes | DAGonFS P2P, 2 nodes |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| 8 × 128 MiB, shell tasks (≈ 9 GiB of I/O) | 61.8 s (60.6–66.1) | 14.9 s (14.7–15.0) | **28.8 s** (28.4–30.2) | 29.2 s (28.3–30.6) | 31.5 s (30.7–31.7) | 34.8 s (32.5–34.9) |
| 8 × 128 MiB, Python tasks | 55.5 s (54.8–58.8) | 15.4 s (15.2–16.1) | **28.2 s** (27.0–28.7) | 30.1 s (30.1–30.2) | 29.8 s (29.4–29.8) | 31.1 s (31.0–31.8) |
| 2000 × 8 KiB files, 8 tasks | 270.4 s (245.4–277.1) | 14.5 s (14.4–14.6) | **22.9 s** (22.8–22.9) | 26.2 s (25.3–26.7) | 24.1 s (23.8–24.5) | 30.4 s (30.0–30.5) |
| Speed-up over NFS (shell / Python / small) | 1× | 4.2× / 3.6× / 18.7× | **2.1× / 2.0× / 11.8×** | 2.1× / 1.8× / 10.3× | 2.0× / 1.9× / 11.2× | 1.8× / 1.8× / 8.9× |

The two-node speed-ups are computed against the one-node NFS times, which the two-node job reproduced within 6%.

*Comments.*

* **DAGonFS made the workflows 2.0–2.1× faster than NFS on large files and 11.8× faster on small files.** The gain is largest for the small-file workflow because it is dominated by metadata operations, where NFS is weakest (§4.3). This is the typical profile of workflow scratch data: many intermediate files, created once, read once, and discarded.
* **Python tasks.** Tasks that read and write through Python file objects see the same benefit as shell tools: 28.2 s on DAGonFS against 55.5 s on NFS. Every byte that the reducer read was checked against its expected content in every run, in both models and on both node counts.
* **DAGonStar's own overhead.** DAGonStar polls task state with fixed sleeps, so even a workflow that moves almost no data takes about 7 s. Discounting that floor, DAGonFS spends about 21 s on the large workflows' I/O against 48–55 s for NFS, and about 16 s on the small-file workflow against about 263 s, 16× less.
* **Two nodes.** Spreading the volume over two nodes costs 5–9% of workflow time in client-server mode and 3–19% in peer-to-peer mode. In exchange, tasks on either node see the same tree, and the volume's capacity is the memory of both nodes.
* **Peer-to-peer cost.** Replicating the namespace to three peers costs 1–15% on one node and 4–26% on two nodes, most on the small-file workflow, which creates about 18 000 files.
* **Variability.** NFS runs vary by up to 9% from the median, because the file server is shared with the rest of the cluster. DAGonFS and tmpfs runs stay within about 7%, because their performance depends only on the allocated nodes.
* **Distance from tmpfs.** DAGonFS remains 1.6–2.0× slower than node-local tmpfs. That is the price of a user-space file system whose data are distributed over the ranks of a job. Unlike tmpfs, DAGonFS presents one namespace to tasks running on different nodes.

### 4.5 Threats to validity

* The NFS server is shared with other users, and its load during the jobs is unknown. The minimum–maximum ranges in Table 3 bound its effect on these runs.
* wn01, which hosted the one-node job, also ran other users' jobs; the two-node job had idle nodes. This explains why some one-node peer-to-peer figures are lower than the two-node ones.
* Dropping the client page cache makes NFS reads honest about the network but not about the server: data written moments earlier are likely still in the server's memory. The NFS read figures are therefore an upper bound for cold data.
* DAGonFS is volatile and NFS is durable; the comparison concerns scratch data that a workflow discards when it ends.

### 4.6 Memory sizing

DAGonFS keeps all data in memory, and each rank must hold:

* its share of the stored data, about $\frac{1}{P}$ of everything written and not yet removed (≈ 9 GiB / 4 here, since the benchmark removes the scratch directory only after a run);
* on a front end, the whole content of every file that is open, up to 2 GiB for the reducer's output here;
* the MPI and FUSE buffers.

Request memory explicitly (`--mem` per node, or `--exclusive`) for at least the scratch working set divided by the number of nodes, plus the largest open files on the front-end node. Both jobs above requested `--mem=96G` per node. A job that relies on a cluster's default per-CPU memory limit (1 GB per CPU on this cluster) is stopped by Slurm's out-of-memory killer during the first large workflow, and the client-server mount then loses its connection to the killed rank.

## 5. Reproducing the measurements

```bash
module load <MPI module>   # sbatch exports the submitting environment: mpirun must be on PATH
cmake -S . -B build -DUSE_MPI=ON -DCMAKE_BUILD_TYPE=Release && cmake --build build -j
python3.11 -m venv venv && venv/bin/pip install matplotlib dagonstar
sbatch -p <partition> -N 1 -n 4 --mem=96G bench/slurm/bench.sbatch build venv/bin/python
sbatch -p <partition> -N 2 --ntasks-per-node=2 --mem=96G bench/slurm/bench.sbatch build venv/bin/python
venv/bin/python bench/plot.py --out figures bench/results/bench-<one-node>.json \
    "bench/results/bench-<two-node>.json:2 nodes:dagonfs-cs,dagonfs-p2p"
```

`bench/run.py --targets nfs,tmpfs,dagonfs-cs,dagonfs-p2p` selects the targets, and `--nfs-dir` the NFS directory. `BENCH_ARGS` passes these options through the Slurm script. The script stops at once if `mpirun` is not on `PATH`. Every node used must provide `/dev/fuse`, the libfuse 3 runtime (`fuse3-libs`) and a set-uid FUSE helper: `fusermount3`, or the FUSE 2 `fusermount`, which the Slurm script links as `fusermount3` through `tests/slurm/fuse-env.sh`. The raw results of this section are kept in `bench/results/` (`bench-19453.json`, `bench-19454.json`). Report distributions rather than single timings, and state the durability difference whenever a volatile file system is compared with a durable one.

## 6. References

- libfuse project, *FUSE: Filesystem in Userspace*. The low-level API defines request/reply and cache semantics used by DAGonFS.
- MPI Forum, *MPI: A Message-Passing Interface Standard*. MPI point-to-point communication provides the distributed communication substrate.
- DAGonStar, https://github.com/DagOnStar/dagonstar: the workflow engine used in §4.
- De Vita et al., “An ad-hoc file system accelerated workflow application for accidental fire fast response,” WiDE ’24. The project README contains the full bibliographic record and DOI.
