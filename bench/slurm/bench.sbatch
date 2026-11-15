#!/bin/bash
# DAGonFS vs. NFS benchmark (micro-benchmarks and DAGonStar workflows).
#
#   sbatch -p <partition> -N 1 -n 4 --mem=96G bench/slurm/bench.sbatch [build-dir] [python]
#   sbatch -p <partition> -N 2 --ntasks-per-node=2 --mem=96G bench/slurm/bench.sbatch [build-dir] [python]
#
# DAGonFS keeps all data in memory: use --exclusive or --mem=<size> per node
# (the large workflow keeps about 9 GiB plus the open files of the front end);
# a default per-CPU memory limit gets ranks OOM-killed. Every node must provide
# /dev/fuse, fuse3-libs and a set-uid fusermount3 (or the FUSE 2 fusermount,
# see tests/slurm/fuse-env.sh) for the DAGonFS targets.
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
if ! command -v "${MPIRUN:-mpirun}" >/dev/null 2>&1; then
    echo "${MPIRUN:-mpirun} is not on PATH: load the MPI environment (e.g. module load ...) before sbatch, which exports it" >&2
    exit 2
fi
. tests/slurm/fuse-env.sh
trap '[ -n "${DAGONFS_SHIM_DIR:-}" ] && rm -rf "${DAGONFS_SHIM_DIR}"' EXIT
echo "nodes: ${SLURM_JOB_NODELIST:-local}  ranks: ${SLURM_NTASKS:-4}"
"${PYTHON}" bench/run.py --build-dir "${BUILD_DIR}" --ranks "${SLURM_NTASKS:-4}" \
    --output "bench/results/bench-${SLURM_JOB_ID:-local}.json" ${BENCH_ARGS:-}
