#!/usr/bin/env python3
"""Run a DAGonStar workflow whose tasks are Python scripts working on a DAGonFS volume.

The driver creates a volume (either model), copies the task scripts of
``tests/dagonstar/tasks`` onto it and runs this fan-out/fan-in workflow, with
DAGonStar's scratch directory on rank 0's mount point::

    A  produce.py    writes the input parts, a manifest and the shared files
    B<i> transform.py  (width tasks, in parallel) verifies part i, transforms it,
                     appends to the shared ledger under fcntl locks and
                     increments the shared counter under flock
    C  reduce.py     verifies every output, the ledger and the counter

Every dependency is a ``workflow://`` reference resolved and staged by
DAGonStar. Each task runs the script from the volume itself. In the
peer-to-peer model, task B<i> runs the copy of its script seen through peer
``i % ranks`` and uses that peer's view of the shared directory, so locks,
data and namespace are exercised across peers; the reducer uses the last
peer. The driver checks that every task succeeded and that the reducer
recorded a consistent result, and always unmounts the volume.

    python3 tests/dagonstar/run_workflow.py --build-dir build --model client-server --ranks 4 -- --oversubscribe
    python3 tests/dagonstar/run_workflow.py --build-dir build --model peer-to-peer  --ranks 4 -- --oversubscribe

Requires DAGonStar (``pip install dagonstar``) in the interpreter that runs
the driver; the tasks run with the same interpreter.
"""

import argparse
import json
import logging
import os
from pathlib import Path
import shutil
import sys
import tempfile
import time
import uuid

HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(HERE.parents[1] / "python"))

from dagonfs import MODELS, Volume  # noqa: E402


def wait_for(predicate, timeout):
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if predicate():
            return True
        time.sleep(0.1)
    return predicate()


def build_workflow(Workflow, DagonTask, TaskType, mounts, base, width, size):
    python = "{} -B".format(sys.executable)  # never write byte code on the volume
    run_id = base.name
    shared = [m / "wf-shared" / run_id for m in mounts]
    scripts = [m / "wf-bin" / run_id for m in mounts]
    config = {"batch": {"scratch_dir_base": str(base), "remove_dir": "False", "threads": str(width)}}
    workflow = Workflow("dagonfs-posix", config=config, max_threads=width)

    produce = DagonTask(TaskType.BATCH, "A", "{} {}/produce.py --shared {} --parts {} --size {}".format(
        python, scripts[0], shared[0], width, size))
    workflow.add_task(produce)
    for index in range(width):
        peer = index % len(mounts)
        command = ("{} {}/transform.py --index {} --input workflow:///A/data/part{}.bin "
                   "--manifest workflow:///A/manifest.json --shared {}").format(
            python, scripts[peer], index, index, shared[peer])
        workflow.add_task(DagonTask(TaskType.BATCH, "B{}".format(index), command))
    outputs = " ".join("workflow:///B{}/out.bin".format(index) for index in range(width))
    reduce_command = "{} {}/reduce.py --manifest workflow:///A/manifest.json --shared {} {}".format(
        python, scripts[-1], shared[-1], outputs)
    workflow.add_task(DagonTask(TaskType.BATCH, "C", reduce_command))
    workflow.make_dependencies()
    return workflow, shared, scripts


def run(volume, mounts, width, size):
    from dagon import Workflow
    from dagon.task import DagonTask, TaskType

    base = mounts[0] / "wf-scratch" / uuid.uuid4().hex[:8]
    base.mkdir(parents=True)
    workflow, shared, scripts = build_workflow(Workflow, DagonTask, TaskType, mounts, base, width, size)

    # The task scripts live on the volume: copied on rank 0, executed on every peer.
    scripts[0].mkdir(parents=True)
    for script in sorted((HERE / "tasks").glob("*.py")):
        shutil.copy2(str(script), str(scripts[0] / script.name))
    for path in scripts[1:]:
        if not wait_for(lambda: all((path / s.name).exists() for s in (HERE / "tasks").glob("*.py")), 30):
            raise RuntimeError("task scripts did not reach {}".format(path))
    shared[0].parent.mkdir(exist_ok=True)
    shared[0].mkdir()
    for path in shared[1:]:
        if not wait_for(path.exists, 30):
            raise RuntimeError("shared directory did not reach {}".format(path))

    start = time.perf_counter()
    workflow.run()
    elapsed = time.perf_counter() - start

    statuses = {task.name: str(getattr(task, "status", "")) for task in workflow.tasks}
    failed = sorted(name for name, status in statuses.items() if not status.endswith("FINISHED"))
    result_path = shared[0] / "result.json"
    if not failed and not wait_for(result_path.exists, 30):
        failed = ["C (no result.json)"]
    if failed:
        for log in sorted(base.rglob("*.err")) + sorted(base.rglob("*.out")):
            text = log.read_text(errors="replace").strip()
            if text:
                print("--- {}\n{}".format(log, text[-2000:]), file=sys.stderr)
        raise RuntimeError("tasks did not finish: {} (statuses: {})".format(failed, statuses))
    result = json.loads(result_path.read_text())
    if not result.get("ok") or result.get("counter") != width or result.get("ledger_lines") != width + 1:
        raise RuntimeError("inconsistent workflow result: {}".format(result))
    print("workflow: {} tasks finished in {:.2f} s; result {}".format(len(statuses), elapsed, result))
    for path in (base, scripts[0], shared[0]):
        shutil.rmtree(str(path), ignore_errors=True)


def main():
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--build-dir", default="build", type=Path)
    parser.add_argument("--model", choices=MODELS, default="client-server")
    parser.add_argument("--ranks", type=int, default=4)
    parser.add_argument("--width", type=int, default=6, help="parallel transform tasks")
    parser.add_argument("--size", type=int, default=128 * 1024 * 9 + 777, help="bytes per input part")
    parser.add_argument("--root", type=Path, default=None, help="mount root (default: a new temporary directory)")
    parser.add_argument("--mpirun", default=os.environ.get("MPIRUN", "mpirun"))
    parser.add_argument("--timeout", type=float, default=30.0, help="seconds to wait for the mount")
    parser.add_argument("--peers", type=int, default=None,
                        help="peer-to-peer: ranks whose mount points are on this host and are used by tasks "
                             "(ranks 0 .. peers-1; default: all ranks)")
    parser.add_argument("mpirun_args", nargs="*")
    args = parser.parse_args()
    logging.disable(logging.WARNING)

    binary = (args.build_dir / "dagonfs").resolve()
    if not binary.exists():
        parser.error("{} does not exist; build the project first".format(binary))
    try:
        import dagon  # noqa: F401
    except ImportError:
        parser.error("DAGonStar is not installed for {} (pip install dagonstar)".format(sys.executable))

    root = args.root or Path(tempfile.mkdtemp(prefix="dagonfs-wf-"))
    peers = 1 if args.model == "client-server" else (args.peers or args.ranks)
    local_ranks = list(range(peers))
    volume = Volume(root, model=args.model, ranks=args.ranks, executable=binary, mpirun=args.mpirun,
                    mpirun_args=args.mpirun_args, local_ranks=local_ranks)
    print("starting:", " ".join(volume.command()), flush=True)
    status = 1
    try:
        volume.start(timeout=args.timeout)
        mounts = [Path(volume.mountpoint(rank)) for rank in local_ranks]
        run(volume, mounts, args.width, args.size)
        status = 0
    except Exception as error:  # report and fall through to cleanup
        print("error: {}".format(error), file=sys.stderr)
    finally:
        job_status = volume.stop()
        print("DAGonFS job exited with status {}; log: {}".format(job_status, volume.log_path))
        if job_status not in (0, None) and status == 0:
            status = job_status
    return status


if __name__ == "__main__":
    sys.exit(main())
