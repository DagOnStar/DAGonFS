#!/bin/bash
# Multi-node integration test of both DAGonFS models.
#
#   sbatch -p <partition> -N 2 --ntasks-per-node=2 tests/slurm/multinode.sbatch [build-dir]
#
# Requirements on every allocated node: /dev/fuse, fusermount3 and libfuse3.
# The harness runs on the first node, where rank 0 (and, with --map-by slot,
# rank 1) are placed; the remaining ranks store blocks on the other nodes.
#SBATCH --job-name=dagonfs-multinode
#SBATCH --time=00:15:00
#SBATCH --output=dagonfs-multinode-%j.out

set -u
BUILD_DIR=${1:-build}
RANKS=${SLURM_NTASKS:-4}
ROOT_BASE=/tmp/dagonfs-${USER}-${SLURM_JOB_ID:-local}
MPIRUN_ARGS=(--map-by slot --bind-to none)
status=0

echo "nodes: ${SLURM_JOB_NODELIST:-local}  ranks: ${RANKS}"
for model in client-server peer-to-peer; do
    echo "=== ${model} (${RANKS} ranks)"
    srun -N "${SLURM_NNODES:-1}" -n "${SLURM_NNODES:-1}" --ntasks-per-node=1 rm -rf "${ROOT_BASE}-${model}" || true
    python3 tests/mount.py --build-dir "${BUILD_DIR}" --model "${model}" --ranks "${RANKS}" \
        --peer-rank 1 --root "${ROOT_BASE}-${model}/mnt" -- "${MPIRUN_ARGS[@]}" || status=1
done
exit ${status}
