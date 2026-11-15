#!/usr/bin/env python3
"""Run the micro-benchmarks and the DAGonStar workflows on each scratch file system.

Targets:

* ``nfs``: a directory on the NFS-mounted home file system (``--nfs-dir``);
* ``tmpfs``: node-local ``/dev/shm``, an in-memory upper bound;
* ``dagonfs-cs``: a DAGonFS client-server volume (``--ranks`` ranks);
* ``dagonfs-p2p``: a DAGonFS peer-to-peer volume, used through rank 0's mount.

DAGonFS volumes are created and destroyed with the Python volume API. The results
are written as JSON to ``--output``. Run inside an allocation (see bench/slurm).
"""

import argparse
import getpass
import json
import os
from pathlib import Path
import platform
import shutil
import subprocess
import sys
import time

HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(HERE.parent / "python"))

from dagonfs import Volume  # noqa: E402


def run_json(command):
    print("  $", " ".join(map(str, command)), flush=True)
    result = subprocess.run(list(map(str, command)), capture_output=True, text=True)
    if result.returncode != 0:
        raise RuntimeError(f"{command[1]} failed ({result.returncode}): {result.stderr[-2000:]}")
    # The JSON document is the last line printed; DAGonStar may print before it.
    return json.loads(result.stdout.strip().splitlines()[-1])


def benchmark(directory, args):
    directory.mkdir(parents=True, exist_ok=True)
    python = sys.executable
    out = {"fs": run_json([python, HERE / "fsbench.py", "--dir", directory, "--repeats", args.repeats,
                           "--large-sizes", args.large_sizes])}
    out["workflow_large"] = run_json([python, HERE / "workflow.py", "--dir", directory, "--kind", "large",
                                      "--width", args.width, "--size-mib", args.size_mib,
                                      "--repeats", args.workflow_repeats])
    out["workflow_small"] = run_json([python, HERE / "workflow.py", "--dir", directory, "--kind", "small",
                                      "--width", args.width, "--files", args.files,
                                      "--repeats", args.workflow_repeats])
    return out


def main():
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--build-dir", type=Path, default=HERE.parent / "build")
    parser.add_argument("--targets", default="nfs,tmpfs,dagonfs-cs,dagonfs-p2p")
    parser.add_argument("--nfs-dir", type=Path, default=Path.home() / "dagonfs-bench-nfs")
    parser.add_argument("--ranks", type=int, default=4)
    parser.add_argument("--mpirun-args", default="--map-by slot --bind-to none")
    parser.add_argument("--repeats", type=int, default=5)
    parser.add_argument("--large-sizes", default="1,16,128,512")
    parser.add_argument("--width", type=int, default=8)
    parser.add_argument("--size-mib", type=int, default=128)
    parser.add_argument("--files", type=int, default=2000)
    parser.add_argument("--workflow-repeats", type=int, default=3)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()

    tag = os.environ.get("SLURM_JOB_ID", str(os.getpid()))
    results = {"host": platform.node(), "nodes": os.environ.get("SLURM_JOB_NODELIST", platform.node()),
               "ranks": args.ranks, "date": time.strftime("%Y-%m-%d %H:%M"), "targets": {},
               "parameters": {k: str(v) for k, v in vars(args).items()}}
    for target in args.targets.split(","):
        print(f"=== {target}", flush=True)
        if target == "nfs":
            directory = args.nfs_dir / tag
            results["targets"][target] = benchmark(directory, args)
            shutil.rmtree(directory, ignore_errors=True)
        elif target == "tmpfs":
            directory = Path("/dev/shm") / f"dagonfs-bench-{getpass.getuser()}-{tag}"
            results["targets"][target] = benchmark(directory, args)
            shutil.rmtree(directory, ignore_errors=True)
        elif target in ("dagonfs-cs", "dagonfs-p2p"):
            model = "client-server" if target == "dagonfs-cs" else "peer-to-peer"
            root = Path("/tmp") / f"dagonfs-bench-{getpass.getuser()}-{tag}-{model}"
            volume = Volume(root, model=model, ranks=args.ranks, executable=args.build_dir / "dagonfs",
                            mpirun_args=args.mpirun_args.split())
            with volume:
                results["targets"][target] = benchmark(volume.mountpoint(0) / "bench", args)
        else:
            parser.error(f"unknown target {target}")
        args.output.write_text(json.dumps(results, indent=1))
    print(f"results: {args.output}")


if __name__ == "__main__":
    main()
