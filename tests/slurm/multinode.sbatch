#!/bin/bash
# Mounted validation of both DAGonFS models under Slurm, on one or more nodes:
# the POSIX suite with the peer-to-peer checks (tests/mount.py), then the
# DAGonStar workflow whose tasks are Python scripts (tests/dagonstar/run_workflow.py).
#
#   sbatch -p <partition> -N 2 --ntasks-per-node=2 tests/slurm/multinode.sbatch [build-dir] [python]
#
# <python> runs the harnesses and the workflow tasks; the workflow test needs
# DAGonStar in it (it is skipped otherwise). Default: python3 from PATH.
#
# Requirements on every allocated node: /dev/fuse, libfuse3 and a set-uid
# fusermount3 (or the FUSE 2 fusermount, see tests/slurm/fuse-env.sh).
# The harnesses run on the first node, where rank 0 (and, with --map-by slot,
# rank 1) are placed; the remaining ranks store blocks on the other nodes.
#SBATCH --job-name=dagonfs-multinode
#SBATCH --time=00:30:00
#SBATCH --output=dagonfs-multinode-%j.out

set -u
BUILD_DIR=${1:-build}
PYTHON=${2:-python3}
RANKS=${SLURM_NTASKS:-4}
ROOT_BASE=/tmp/dagonfs-${USER}-${SLURM_JOB_ID:-local}
MPIRUN_ARGS=(--map-by slot --bind-to none)
status=0

if ! command -v "${MPIRUN:-mpirun}" >/dev/null 2>&1; then
    echo "${MPIRUN:-mpirun} is not on PATH: load the MPI environment (e.g. module load ...) before sbatch, which exports it" >&2
    exit 2
fi
. tests/slurm/fuse-env.sh   # sbatch runs a copy of this script: paths are relative to the repository
echo "nodes: ${SLURM_JOB_NODELIST:-local}  ranks: ${RANKS}  python: $("${PYTHON}" --version 2>&1)"
for model in client-server peer-to-peer; do
    echo "=== ${model} (${RANKS} ranks): POSIX suite"
    srun -N "${SLURM_NNODES:-1}" -n "${SLURM_NNODES:-1}" --ntasks-per-node=1 rm -rf "${ROOT_BASE}-${model}" || true
    "${PYTHON}" tests/mount.py --build-dir "${BUILD_DIR}" --model "${model}" --ranks "${RANKS}" \
        --peer-rank 1 --root "${ROOT_BASE}-${model}/mnt" -- "${MPIRUN_ARGS[@]}" || status=1
done
if "${PYTHON}" -c "import dagon" 2>/dev/null; then
    for model in client-server peer-to-peer; do
        echo "=== ${model} (${RANKS} ranks): DAGonStar workflow with Python tasks"
        "${PYTHON}" tests/dagonstar/run_workflow.py --build-dir "${BUILD_DIR}" --model "${model}" --ranks "${RANKS}" \
            --peers 2 --width 8 --root "${ROOT_BASE}-wf-${model}/mnt" -- "${MPIRUN_ARGS[@]}" || status=1
    done
else
    echo "=== DAGonStar is not installed for ${PYTHON}: workflow test skipped"
fi
[ -n "${DAGONFS_SHIM_DIR:-}" ] && rm -rf "${DAGONFS_SHIM_DIR}"
exit ${status}
