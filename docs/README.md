# DAGonFS documentation

The documents are written to be read in order by a newcomer, and individually as references.

| Document | Content |
| --- | --- |
| [ARCHITECTURE.md](ARCHITECTURE.md) | Design boundary, layers, the two deployment models, request paths, consistency and failure models |
| [DATA-PLANE.md](DATA-PLANE.md) | Block decomposition, contiguous layout, point-to-point store/load, ownership invariants, cost model |
| [PROTOCOL.md](PROTOCOL.md) | MPI message formats and ordering guarantees of both control planes |
| [POSIX-COMPATIBILITY.md](POSIX-COMPATIBILITY.md) | Supported operations, cluster-wide locks, special files, error semantics, close-to-open consistency |
| [CONFIGURATION.md](CONFIGURATION.md) | Prerequisites, build, command line, launcher, shutdown, cluster use |
| [PERFORMANCE.md](PERFORMANCE.md) | Methodology, tuning, and the DAGonFS vs. NFS benchmark with DAGonStar workflows |
| [DAGONSTAR-INTEGRATION.md](DAGONSTAR-INTEGRATION.md) | Using DAGonFS as the scratch space of DAGonStar workflows |
| [TESTING.md](TESTING.md) | Validation levels, test harness, the DAGonStar workflow test, recorded results |
| [DEFECT-ANALYSIS.md](DEFECT-ANALYSIS.md) | November 2026 assessment: defects D1–D47, root causes and remedies |
| [man/dagonfs.1](man/dagonfs.1) | Manual page of `dagonfs` and `DAGonFS_Launcher` (`man docs/man/dagonfs.1`) |
| [presentations/](presentations/README.md) | A 20-minute scientific talk and a 20-minute hands-on tutorial (PowerPoint, with speaker notes) |

Contributor rules are in [../AGENTS.md](../AGENTS.md). The benchmark scripts are in [../bench](../bench), and the figures in [../figures](../figures).

The manual page is installed to `share/man/man1`. When `groff` is available, the build also formats it as text and PostScript (and as PDF with `ps2pdf`), installed under `share/doc/DAGonFS` together with these documents.
