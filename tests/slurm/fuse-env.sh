# Source from a Slurm job script before starting DAGonFS:  . tests/slurm/fuse-env.sh
#
# libfuse 3 mounts and unmounts through the set-uid helper fusermount3. Some
# nodes ship the libfuse 3 runtime (fuse3-libs) but only the FUSE 2 helper,
# fusermount. Both helpers speak the same protocol with the kernel and with
# the library (a file descriptor passed over a socket), so this script puts a
# "fusermount3" symbolic link to fusermount first on PATH. The link lives in a
# directory shared by all nodes of the job (the submission directory), because
# remote ranks resolve the helper on their own node.
#
# Nothing is changed when fusermount3 exists. Requires DAGONFS_SHIM_DIR or
# SLURM_SUBMIT_DIR to be on a file system visible to every node.

if ! command -v fusermount3 >/dev/null 2>&1; then
    if command -v fusermount >/dev/null 2>&1; then
        DAGONFS_SHIM_DIR=${DAGONFS_SHIM_DIR:-${SLURM_SUBMIT_DIR:-$PWD}/.dagonfs-shim-${SLURM_JOB_ID:-$$}}
        mkdir -p "${DAGONFS_SHIM_DIR}"
        ln -sf "$(command -v fusermount)" "${DAGONFS_SHIM_DIR}/fusermount3"
        export PATH="${DAGONFS_SHIM_DIR}:${PATH}"
        echo "fuse-env: fusermount3 -> $(command -v fusermount) (${DAGONFS_SHIM_DIR})"
    else
        echo "fuse-env: neither fusermount3 nor fusermount is installed; DAGonFS cannot mount" >&2
    fi
fi
