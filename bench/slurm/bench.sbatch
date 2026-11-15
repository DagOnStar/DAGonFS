#!/bin/bash
# DAGonFS vs. NFS benchmark (micro-benchmarks and DAGonStar workflows).
#
#   sbatch -p <partition> -N 1 -n 4 --exclusive bench/slurm/bench.sbatch [build-dir] [python]
#
# DAGonFS keeps all data in memory: use --exclusive or --mem=<size> per node
# (the large workflow keeps about 9 GiB plus the open files of the front end);
# a default per-CPU memory limit gets ranks OOM-killed. Every node must provide
# FUSE 3 (fuse3-libs and a set-uid fusermount3) for the DAGonFS targets.
#
# <python> must provide DAGonStar (dagon); the default is python3 from PATH.
# Results: bench/results/bench-<jobid>.json; plot them with bench/plot.py.
#SBATCH --job-name=dagonfs-bench
#SBATCH --time=02:00:00
#SBATCH --output=dagonfs-bench-%j.out

set -eu
BUILD_DIR=${1:-build}
PYTHON=${2:-python3}
mkdir -p bench/results
echo "nodes: ${SLURM_JOB_NODELIST:-local}  ranks: ${SLURM_NTASKS:-4}"
"${PYTHON}" bench/run.py --build-dir "${BUILD_DIR}" --ranks "${SLURM_NTASKS:-4}" \
    --output "bench/results/bench-${SLURM_JOB_ID:-local}.json" ${BENCH_ARGS:-}
