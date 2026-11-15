# DAGonStar integration guide

## Purpose

[DAGonStar](https://github.com/DagOnStar/dagonstar) is a Python workflow engine that represents jobs as directed acyclic graphs and supports task-oriented and data-oriented workflows. Its `workflow://` URI scheme expresses task-produced data dependencies. DAGonFS complements that model by supplying a fast, volatile filesystem for the workflow scratch path: task commands interact with ordinary POSIX paths while DAGonStar resolves and stages dependencies.

DAGonStar remains responsible for workflow scheduling, `workflow://` dependency resolution, checkpointing, and final-stage data movement. DAGonFS remains responsible for POSIX operations inside its mount. DAGonFS does not parse `workflow://` URIs itself.

## Preconditions

1. Install DAGonStar from its upstream repository and validate a local example before introducing DAGonFS.
2. Build DAGonFS with MPI and FUSE3 support.
3. Start DAGonFS before DAGonStar creates task scratch directories.
4. Configure the DAGonStar scratch/work directory to be located in the DAGonFS mount, according to the DAGonStar configuration version deployed at the site.
5. Stage final outputs to persistent storage before unmounting DAGonFS.

In client-server mode, task processes using the mount must execute on the node hosting the FUSE client. In peer-to-peer mode, each participating node uses its rank-specific mountpoint; the workflow launcher must direct each task to the corresponding local path.

## Minimal dataflow example

The following documented DAGonStar dataflow pattern assumes that DAGonStar creates task working directories below the DAGonFS mount. The `workflow:///A/output/f1.txt` reference is resolved by DAGonStar; the commands use ordinary POSIX tools.

```python
from dagon import Workflow
from dagon.task import DagonTask, TaskType

workflow = Workflow("dagonfs-dataflow")
produce = DagonTask(TaskType.BATCH, "A", "mkdir -p output && printf 'DAGonFS\\n' > output/f1.txt")
consume = DagonTask(TaskType.BATCH, "B", "cat workflow:///A/output/f1.txt > result.txt")
workflow.add_task(produce)
workflow.add_task(consume)
workflow.make_dependencies()
workflow.run()
```

This example works with the upstream DAGonStar API. The site-specific setting that chooses the scratch path is intentionally not embedded here because configuration names and scheduler wrappers vary by DAGonStar release.

## Creating the volume from a workflow script

The Python module `python/dagonfs` creates a volume with either model and removes it when the workflow ends. This keeps the scratch file system's life cycle inside the workflow script:

```python
import sys; sys.path.insert(0, "/path/to/DAGonFS/python")
from dagonfs import Volume

with Volume("/tmp/dagonfs", model="client-server", ranks=8,
            executable="/opt/dagonfs/bin/dagonfs",
            mpirun_args=["--hostfile", "hosts"]) as volume:
    scratch = volume.mountpoint(0)          # point DAGonStar's scratch root here
    ...                                     # build and run the workflow
```

Use `model="client-server"` when every task runs on the node hosting rank 0. Use `model="peer-to-peer"` when tasks run on several nodes, each using the mount point of a rank on its own node (`volume.mountpoint(r)`, with `local_ranks` listing the ranks placed on the calling host). The same choice is available without Python as `dagonfs --model …` or as `dagonfs_model` in `DAGonFS.ini`.

In a peer-to-peer volume, a file written and closed on one peer can be read on any other peer as soon as `close` returns. Namespace changes (creation, rename, removal) reach the other peers asynchronously, typically within milliseconds, so a consumer on another node should wait for the producer task's completion, which DAGonStar's dependency graph already enforces. Tasks that run concurrently and update the same file must serialise their updates with `fcntl`/`lockf` or `flock` locks, which DAGonFS enforces across all peers.

## Validated example: Python task scripts

`tests/dagonstar/run_workflow.py` is a complete, tested example ([TESTING.md](TESTING.md) §2.2). It creates a volume with either model, copies the task scripts of `tests/dagonstar/tasks/` onto the volume and runs a fan-out/fan-in workflow whose tasks execute those scripts from the volume:

```python
python = sys.executable + " -B"            # do not write byte code into the volume
workflow = Workflow("dagonfs-posix", config={"batch": {"scratch_dir_base": str(scratch),
                                                      "remove_dir": "False", "threads": "6"}})
workflow.add_task(DagonTask(TaskType.BATCH, "A",
    f"{python} {bin0}/produce.py --shared {shared0} --parts 6 --size 1180425"))
for i in range(6):
    peer = i % len(mounts)                  # peer-to-peer: spread the tasks over the peers
    workflow.add_task(DagonTask(TaskType.BATCH, f"B{i}",
        f"{python} {bins[peer]}/transform.py --index {i} --input workflow:///A/data/part{i}.bin "
        f"--manifest workflow:///A/manifest.json --shared {shared[peer]}"))
outputs = " ".join(f"workflow:///B{i}/out.bin" for i in range(6))
workflow.add_task(DagonTask(TaskType.BATCH, "C",
    f"{python} {bins[-1]}/reduce.py --manifest workflow:///A/manifest.json --shared {shared[-1]} {outputs}"))
workflow.make_dependencies()
workflow.run()
```

The parallel tasks coordinate through a shared directory on the volume, with no service other than the file system: each appends a line to a ledger under an exclusive `fcntl.lockf` lock and increments a counter under `flock`. Locks are cluster-wide in both models, and releasing a lock publishes the data written under it, so the reducer finds exactly one ledger line and one increment per task even when the tasks use different peers. Run it with:

```bash
python3 tests/dagonstar/run_workflow.py --build-dir build --model peer-to-peer --ranks 4 -- --oversubscribe
```

Use `--width` for more parallel tasks; `--peers` restricts the peers used by tasks to the ranks placed on the local host.

## Operational procedure

1. Reserve nodes and ensure MPI, FUSE, DAGonStar, and task executables are available on every node.
2. Start DAGonFS and wait until its mountpoint exists.
3. Set DAGonStar's scratch/work root to that mountpoint, or arrange task launchers so per-task working directories are created there.
4. Run the DAGonStar workflow.
5. Validate and export required outputs to durable storage.
6. Allow DAGonStar cleanup to finish, then unmount DAGonFS and terminate MPI ranks.

An explicit preflight check is useful in batch scripts:

```bash
test -d "$DAGONFS_MOUNTPOINT"
printf 'ok\\n' > "$DAGONFS_MOUNTPOINT/.dagonfs-preflight"
test "$(cat "$DAGONFS_MOUNTPOINT/.dagonfs-preflight")" = ok
rm "$DAGONFS_MOUNTPOINT/.dagonfs-preflight"
```

In peer-to-peer deployments, run an equivalent probe on every node that will execute a workflow task.

## Data lifetime and checkpointing

DAGonFS is volatile. DAGonStar checkpoint metadata may describe workflow progress, but it cannot recreate contents that existed only in DAGonFS after the mount is lost. Keep intermediate, reproducible data in DAGonFS scratch; keep final results, workflow checkpoints, and reusable inputs in persistent storage. Make completion depend on successful stage-out of required outputs.

Do not treat `fsync` as a durability boundary: it completes the in-memory filesystem operation, not a disk commit.

## Performance and troubleshooting

The expected benefit is highest for dependency-heavy workflows whose intermediate data fit in aggregate RAM. Measure complete workflow wall time and report DAGonStar and DAGonFS versions, MPI implementation, rank count, task placement, file-size distribution, baseline filesystem, and final-stage policy.

- **Task cannot find a file:** verify its node sees the expected mount and that DAGonStar resolved the intended producer.
- **Filesystem access hangs:** inspect DAGonFS/MPI logs and check for failed ranks.
- **Output vanishes:** unexported data is expected to disappear when the volatile mount is removed; add an explicit stage-out task.
- **Permissions differ:** verify task UID/GID consistency and scratch-directory permissions across nodes.

## References

- [DAGonStar upstream repository and examples](https://github.com/DagOnStar/dagonstar)
- [DAGonFS architecture](ARCHITECTURE.md)
- [POSIX/FUSE compatibility](POSIX-COMPATIBILITY.md)
- [Performance methodology](PERFORMANCE.md)
