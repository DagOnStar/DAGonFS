"""Create, observe and destroy DAGonFS volumes from Python (standard library only)."""

import os
from pathlib import Path
import shutil
import subprocess
import sys
import time

MODELS = ("client-server", "peer-to-peer")
FRONTENDS = ("fuse3", "native", "preload")


class VolumeError(RuntimeError):
    """A volume could not be created or did not shut down cleanly."""


def is_mounted(path):
    """True if ``path`` is currently a mount point."""
    path = os.path.realpath(path)
    try:
        with open("/proc/self/mounts", encoding="utf-8") as mounts:
            return any(line.split()[1] == path for line in mounts)
    except FileNotFoundError:  # not Linux
        return os.path.ismount(path)


def unmount(path):
    """Unmount ``path`` if it is mounted (fusermount3 -u on Linux, umount on macOS)."""
    if not is_mounted(path):
        return
    if sys.platform == "darwin":
        command = ["umount", str(path)]
    else:
        command = [shutil.which("fusermount3") or "fusermount", "-u", str(path)]
    subprocess.run(command, check=False)


def _wait_for(predicate, timeout):
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if predicate():
            return True
        time.sleep(0.2)
    return predicate()


class Volume:
    """A DAGonFS volume spanning the ranks of one MPI job.

    :param root: mount point (client-server) or parent of the per-rank mount
        points ``<root>/<rank>`` (peer-to-peer); for the ``native`` and
        ``preload`` frontends, the Unix socket served by rank 0.
    :param model: ``"client-server"`` or ``"peer-to-peer"``.
    :param ranks: number of MPI processes.
    :param executable: path of ``dagonfs`` (default: ``$DAGONFS``, then ``PATH``).
    :param mpirun: MPI launcher command (default: ``$MPIRUN`` or ``mpirun``).
    :param mpirun_args: extra launcher arguments, e.g. ``["--hostfile", "hosts"]``.
    :param local_ranks: ranks whose mount points are on this host and are
        waited for by :meth:`start` (default: rank 0 only).
    :param debug: trace FUSE requests; :param logging: verbose logging.
    :param log: file receiving the job's output (default: ``<root>.log``).
    :param frontend: ``"fuse3"`` (default), ``"native"`` or ``"preload"``.
        The FUSE-free frontends need no kernel module, serve only the
        client-server model, and are stopped through the socket.
    :param preload_library: ``libdagonfs_preload`` path used by
        :meth:`environment` for the ``preload`` frontend.
    :param preload_root: path prefix the interposer redirects (default ``/dagonfs``).
    """

    def __init__(self, root, model="client-server", ranks=2, executable=None, mpirun=None,
                 mpirun_args=(), local_ranks=(0,), debug=False, logging=False, log=None,
                 frontend="fuse3", preload_library=None, preload_root="/dagonfs"):
        if model not in MODELS:
            raise ValueError(f"model must be one of {MODELS}, not {model!r}")
        if frontend not in FRONTENDS:
            raise ValueError(f"frontend must be one of {FRONTENDS}, not {frontend!r}")
        if frontend != "fuse3" and model != "client-server":
            raise ValueError("the native and preload frontends serve only client-server volumes")
        if not str(preload_root).startswith("/"):
            raise ValueError("preload_root must be an absolute path")
        self.frontend = frontend
        self.preload_library = preload_library
        self.preload_root = str(preload_root).rstrip("/") or "/"
        if ranks < 1:
            raise ValueError("ranks must be positive")
        self.root = Path(root)
        self.model = model
        self.ranks = ranks
        executable = str(executable or os.environ.get("DAGONFS") or shutil.which("dagonfs") or "dagonfs")
        # The job runs from the root's parent directory: resolve relative paths now.
        self.executable = os.path.abspath(executable) if os.sep in executable else executable
        self.mpirun = mpirun or os.environ.get("MPIRUN", "mpirun")
        self.mpirun_args = list(mpirun_args)
        self.local_ranks = [r for r in local_ranks if self.mounts(r)]
        self.debug = debug
        self.logging = logging
        self.log_path = Path(log) if log else self.root.parent / f"{self.root.name}.log"
        self._job = None
        self._log = None

    # -- layout ---------------------------------------------------------------
    @property
    def mounted(self):
        """True if the volume is exposed through FUSE mount points."""
        return self.frontend == "fuse3"

    @property
    def socket(self):
        """The native socket (``None`` for a FUSE volume)."""
        return None if self.mounted else self.root

    def mounts(self, rank):
        """True if ``rank`` exposes a mount point."""
        return (self.mounted and 0 <= rank < self.ranks
                and (self.model == "peer-to-peer" or rank == 0))

    def mountpoint(self, rank=0):
        """Mount point of ``rank`` (``None`` if that rank does not mount the volume)."""
        if not self.mounts(rank):
            return None
        return self.root if self.model == "client-server" else self.root / str(rank)

    def command(self):
        """The launch command, as a list of arguments (no shell is used)."""
        command = [self.mpirun, "-np", str(self.ranks), *self.mpirun_args, self.executable,
                   "--model", self.model]
        if not self.mounted:
            command += ["--frontend", self.frontend]
        command.append("-d" if self.debug else "-f")
        if self.logging:
            command.append("--log")
        return command + [str(self.root)]

    def environment(self):
        """Variables that let a client process use this volume.

        Only the ``preload`` frontend sets the interposer variable
        (``LD_PRELOAD``, or ``DYLD_INSERT_LIBRARIES`` on macOS).
        """
        if self.mounted:
            return {"DAGONFS_MOUNTPOINT": str(self.mountpoint(0) or self.root)}
        environment = {"DAGONFS_SOCKET": str(self.root)}
        if self.frontend == "preload":
            environment["DAGONFS_PRELOAD_ROOT"] = self.preload_root
            if self.preload_library:
                variable = "DYLD_INSERT_LIBRARIES" if sys.platform == "darwin" else "LD_PRELOAD"
                environment[variable] = str(self.preload_library)
        return environment

    def client(self):
        """A :class:`dagonfs.native.Client` for a native or preload volume."""
        if self.mounted:
            raise VolumeError("a FUSE volume is used through its mount point, not a client")
        from .native import Client
        return Client(self.root)

    # -- life cycle -----------------------------------------------------------
    @property
    def running(self):
        return self._job is not None and self._job.poll() is None

    def start(self, timeout=30.0):
        """Launch the volume and wait until the local mount points are mounted."""
        if self._job is not None:
            raise VolumeError("volume already started")
        if self.mounted:
            self.root.mkdir(parents=True, exist_ok=True)
        else:
            self.root.parent.mkdir(parents=True, exist_ok=True)
        self._log = open(self.log_path, "w", encoding="utf-8")
        self._job = subprocess.Popen(self.command(), stdout=self._log, stderr=subprocess.STDOUT,
                                     cwd=self.root.parent)
        if not self.mounted:
            from .native import wait_until_ready
            if not wait_until_ready(str(self.root), timeout, alive=lambda: self.running):
                status = self._job.poll()
                self.stop()
                detail = f"exited with status {status}" if status is not None else "did not answer in time"
                raise VolumeError(f"DAGonFS {detail}; see {self.log_path}")
            return self
        mounted = lambda: all(is_mounted(self.mountpoint(r)) for r in self.local_ranks)
        if not _wait_for(lambda: mounted() or not self.running, timeout) or not mounted():
            status = self._job.poll()
            self.stop()
            detail = f"exited with status {status}" if status is not None else "did not mount in time"
            raise VolumeError(f"DAGonFS {detail}; see {self.log_path}")
        return self

    def stop(self, timeout=30.0):
        """Unmount the volume and wait for the MPI job; returns its exit status."""
        if self._job is None:
            return None
        if not self.mounted and self.running:
            from .native import Client
            try:
                Client(self.root, timeout=timeout).shutdown()
            except OSError:
                pass  # terminated below if it does not exit
        for rank in self.local_ranks:
            unmount(self.mountpoint(rank))
        try:
            self._job.wait(timeout=timeout)
        except subprocess.TimeoutExpired:
            self._job.terminate()
            try:
                self._job.wait(timeout=10)
            except subprocess.TimeoutExpired:
                self._job.kill()
                self._job.wait()
        # A killed job leaves stale mounts behind.
        for rank in self.local_ranks:
            unmount(self.mountpoint(rank))
        if self._log is not None:
            self._log.close()
            self._log = None
        status, self._job = self._job.returncode, None
        return status

    def __enter__(self):
        return self.start()

    def __exit__(self, *exc):
        status = self.stop()
        if status not in (0, None) and exc[0] is None:
            raise VolumeError(f"DAGonFS exited with status {status}; see {self.log_path}")
        return False
