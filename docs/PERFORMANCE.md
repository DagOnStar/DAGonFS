# Performance methodology and tuning

## What DAGonFS optimizes

DAGonFS removes persistent-storage latency from the workflow scratch path and distributes payload blocks across MPI ranks. The expected gain is largest when a workflow repeatedly creates, reads, and writes temporary files whose working set fits in aggregate RAM. It is not a universal replacement for a durable parallel filesystem.

The observed time for an operation can be decomposed conceptually as

\[
T_{operation} = T_{FUSE} + T_{metadata} + T_{MPI} + T_{memory}.
\]

For small files, FUSE crossings and metadata management dominate. For large files, MPI communication and memory-copy costs dominate. This model is useful when interpreting measurements: a bandwidth result should include the file size, block size, rank count, host topology, and whether caches were warm.

## Deployment guidance

- Use `Release` builds for measurement; debug logging and sanitizers materially alter timing.
- Select a rank count that matches available CPU and memory resources. Additional ranks may increase MPI coordination overhead for small files.
- Keep the workflow scratch set within aggregate RAM. Swapping reverses the intended performance advantage.
- Group very small temporary files when application semantics allow it; namespace operations have fixed FUSE and metadata overhead.
- Avoid treating `fsync` as durable persistence. Export results to a persistent filesystem before a job ends.

## Reproducible benchmark sketch

Mount DAGonFS, choose an isolated work directory, and record the environment before measuring:

```bash
uname -a
mpirun --version
cmake --build build --config Release
export DAGONFS_MOUNTPOINT=/tmp/DAGonFS
python3 tests/test_python_posix.py
```

For a basic throughput observation from Python:

```python
import os
import time
from pathlib import Path

path = Path(os.environ["DAGONFS_MOUNTPOINT"]) / "benchmark.bin"
payload = b"x" * (64 * 1024 * 1024)
start = time.perf_counter()
path.write_bytes(payload)
elapsed = time.perf_counter() - start
print(f"write throughput: {len(payload) / elapsed / 1e6:.1f} MB/s")
path.unlink()
```

Repeat measurements, report a distribution rather than one timing, and compare against a baseline using the same application, payload, CPU affinity, and mount options. Do not compare a volatile RAM filesystem with a durable filesystem without explicitly stating that their durability guarantees differ.

## References

- libfuse project, *FUSE: Filesystem in Userspace*. The low-level API defines request/reply and cache semantics used by DAGonFS.
- MPI Forum, *MPI: A Message-Passing Interface Standard*. MPI collectives provide the distributed communication substrate.
- De Vita et al., “An ad-hoc file system accelerated workflow application for accidental fire fast response,” WiDE ’24. The project README contains the full bibliographic record and DOI.
