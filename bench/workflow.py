#!/usr/bin/env python3
"""DAGonStar workflows used to compare workflow scratch spaces.

Two fan-out/fan-in dataflows are run with DAGonStar, whose scratch directory
(``scratch_dir_base``) is placed on the file system under test. Every
dependency is a ``workflow://`` reference, which DAGonStar stages by copying
the producer's output into the consumer's scratch directory.

* ``large``: a producer writes W files of S MiB; W parallel tasks each stage one
  file, copy it and write a twice-as-large output; a reducer stages all outputs
  and concatenates them.
* ``small``: a producer writes a directory of N files of 8 KiB; W parallel tasks
  each stage the whole directory and pack it into a tar archive; a reducer
  stages and concatenates the archives.

Prints the wall time of each run (staging included, scratch removal excluded) as JSON.
"""

import argparse
import json
import logging
from pathlib import Path
import shutil
import sys
import time
import uuid

from dagon import Workflow
from dagon.task import DagonTask, TaskType


def build(kind, workflow, width, size_mib, files):
    if kind == "large":
        produce = " && ".join(f"dd if=/dev/zero of=part{i}.bin bs=1M count={size_mib} status=none"
                              for i in range(width))
        tasks = [DagonTask(TaskType.BATCH, "A", produce)]
        for i in range(width):
            tasks.append(DagonTask(TaskType.BATCH, f"B{i}",
                                   f"dd if=workflow:///A/part{i}.bin of=in.bin bs=1M status=none && "
                                   f"cat in.bin in.bin > out.bin"))
        inputs = " ".join(f"workflow:///B{i}/out.bin" for i in range(width))
        tasks.append(DagonTask(TaskType.BATCH, "C", f"cat {inputs} > all.bin && wc -c all.bin > size.txt"))
    else:
        produce = (f"mkdir files && for i in $(seq 1 {files}); do "
                   f"head -c 8192 /dev/zero > files/f$i.dat; done")
        tasks = [DagonTask(TaskType.BATCH, "A", produce)]
        for i in range(width):
            tasks.append(DagonTask(TaskType.BATCH, f"B{i}",
                                   f"cp -r workflow:///A/files in && tar cf out.tar in"))
        inputs = " ".join(f"workflow:///B{i}/out.tar" for i in range(width))
        tasks.append(DagonTask(TaskType.BATCH, "C", f"cat {inputs} > all.tar && wc -c all.tar > size.txt"))
    for task in tasks:
        workflow.add_task(task)
    workflow.make_dependencies()


def run_once(kind, scratch, width, size_mib, files):
    base = scratch / f"wf-{kind}-{uuid.uuid4().hex[:8]}"
    base.mkdir(parents=True)
    config = {"batch": {"scratch_dir_base": str(base), "remove_dir": "False", "threads": str(width)}}
    workflow = Workflow(f"bench-{kind}", config=config, max_threads=width)
    build(kind, workflow, width, size_mib, files)
    start = time.perf_counter()
    workflow.run()
    elapsed = time.perf_counter() - start
    failed = [t.name for t in workflow.tasks if str(getattr(t, "status", "")).endswith("FAILED")]
    # The reducer's output proves that every stage ran and staged its inputs.
    sizes = [int(p.read_text().split()[0]) for p in base.rglob("size.txt")]
    shutil.rmtree(base, ignore_errors=True)
    expected = width * 2 * size_mib << 20 if kind == "large" else None
    if failed or len(sizes) != 1 or (expected is not None and sizes[0] != expected) or sizes[0] == 0:
        raise RuntimeError(f"workflow incomplete: failed={failed} reducer sizes={sizes} expected={expected}")
    return elapsed


def main():
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--dir", required=True, type=Path, help="scratch base on the file system under test")
    parser.add_argument("--kind", choices=("large", "small"), default="large")
    parser.add_argument("--width", type=int, default=8, help="parallel tasks")
    parser.add_argument("--size-mib", type=int, default=64, help="large: MiB per producer file")
    parser.add_argument("--files", type=int, default=2000, help="small: files written by the producer")
    parser.add_argument("--repeats", type=int, default=3)
    args = parser.parse_args()
    logging.disable(logging.WARNING)
    times = [run_once(args.kind, args.dir, args.width, args.size_mib, args.files) for _ in range(args.repeats)]
    json.dump({"kind": args.kind, "width": args.width, "size_mib": args.size_mib, "files": args.files,
               "seconds": times}, sys.stdout)
    print()


if __name__ == "__main__":
    main()
