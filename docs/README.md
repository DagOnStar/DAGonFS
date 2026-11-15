# DAGonFS documentation

The documents are written to be read in order by a newcomer, and individually as references.

| Document | Content |
| --- | --- |
| [ARCHITECTURE.md](ARCHITECTURE.md) | Design boundary, layers, the two deployment models, request paths, consistency and failure models |
| [DATA-PLANE.md](DATA-PLANE.md) | Block decomposition, contiguous layout, collective store/load, ownership invariants, cost model |
| [PROTOCOL.md](PROTOCOL.md) | MPI message formats and ordering guarantees of both control planes |
| [POSIX-COMPATIBILITY.md](POSIX-COMPATIBILITY.md) | Supported operations, error semantics, close-to-open consistency |
| [CONFIGURATION.md](CONFIGURATION.md) | Prerequisites, build, command line, launcher, shutdown, cluster use |
| [PERFORMANCE.md](PERFORMANCE.md) | Measurement methodology and tuning |
| [DAGONSTAR-INTEGRATION.md](DAGONSTAR-INTEGRATION.md) | Using DAGonFS as the scratch space of DAGonStar workflows |
| [TESTING.md](TESTING.md) | Validation levels, test harness, recorded results |
| [DEFECT-ANALYSIS.md](DEFECT-ANALYSIS.md) | November 2026 assessment: defects D1–D33, root causes and remedies |

Contributor rules are in [../AGENTS.md](../AGENTS.md); the manual page is in [../doc/english](../doc/english).
