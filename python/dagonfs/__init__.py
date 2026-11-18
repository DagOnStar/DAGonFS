"""Python API for DAGonFS volumes.

A volume is created by starting ``dagonfs`` under MPI and is destroyed by
unmounting it::

    from dagonfs import Volume

    with Volume("/tmp/scratch", model="peer-to-peer", ranks=4,
                mpirun_args=["--hostfile", "hosts"]) as volume:
        work = volume.mountpoint(0)      # /tmp/scratch/0
        ...                              # run workflow tasks on the mount(s)

The deployment model is chosen per volume at run time; the same ``dagonfs``
executable serves both models.

FUSE-free volumes (``frontend="native"`` or ``"preload"``) are served on a Unix
socket; :mod:`dagonfs.native` is a standard-library client for them, and
``python -m dagonfs`` exposes it on the command line.
"""

from .native import Client, NativeStat
from .volume import FRONTENDS, MODELS, Volume, VolumeError, is_mounted, unmount

__all__ = ["Client", "FRONTENDS", "MODELS", "NativeStat", "Volume", "VolumeError",
           "is_mounted", "unmount"]
