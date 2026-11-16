#!/bin/bash
# Frontend positional-I/O probe on one compute node.
#
#   module load <compiler/MPI module>        # jobs inherit this environment
#   cd <DAGonFS checkout with a build/ directory>
#   sbatch -p <partition> bench/slurm/frontend_probe.sbatch [OPS] [MODES]
#
# Results go to bench/results/frontends-<node>-<jobid>.json. FUSE modes are
# skipped automatically when the node has no usable /dev/fuse.
#SBATCH --job-name=dagonfs-frontends
#SBATCH --nodes=1
#SBATCH --ntasks=2
#SBATCH --time=00:30:00
#SBATCH --mem=8G
#SBATCH --output=frontend-%j.out

set -euo pipefail
cd "${SLURM_SUBMIT_DIR:-.}"
export LD_LIBRARY_PATH="$PWD/build/external/lib:${LD_LIBRARY_PATH:-}"
OPS="${1:-2000}"
MODES="${2:-native,preload,fuse,fuse-p2p,local}"
if [[ ! -r /dev/fuse || ! -w /dev/fuse ]] || ! command -v fusermount3 >/dev/null; then
    MODES="$(echo "$MODES" | tr ',' '\n' | grep -v '^fuse' | paste -sd, -)"
fi
NODE="$(hostname -s)"
PYTHON="$(command -v python3.11 || command -v python3)"
echo "node $NODE, modes $MODES, ops $OPS"
mkdir -p bench/results
"$PYTHON" bench/frontend_probe.py --modes "$MODES" --repeats 5 --ops "$OPS" --host "$NODE" \
    --output "bench/results/frontends-$NODE-${SLURM_JOB_ID:-local}-$OPS.json" -- --oversubscribe
