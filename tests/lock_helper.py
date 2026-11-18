#!/usr/bin/env python3
"""Child process for the advisory-lock tests.

POSIX record locks belong to a process, so a conflicting owner must be a
separate process. ``tests/test_python_posix.py`` and the peer-to-peer checks of
``tests/mount.py`` run this script, possibly on another peer's mount point::

    lock_helper.py try       PATH posix|flock ex|sh START LENGTH   # non-blocking: prints granted | busy
    lock_helper.py wait      PATH posix|flock ex|sh START LENGTH   # prints waiting, then granted
    lock_helper.py interrupt PATH posix|flock ex|sh START LENGTH   # SIGALRM after 1 s: interrupted | granted
    lock_helper.py getlk     PATH START LENGTH                     # prints: TYPE START LENGTH of a conflict
    lock_helper.py deadlock  PATH                                  # holds byte 10, then waits for byte 0
    lock_helper.py count     PATH ITERATIONS posix|flock           # locked read-increment-write loop

A granted lock is held until the script exits, or until a line arrives on
stdin when ``--hold`` is given.
"""

import errno
import fcntl
import os
import signal
import struct
import sys

if sys.platform == "darwin":
    # struct flock on macOS: l_start, l_len, l_pid, l_type, l_whence (24 bytes).
    FLOCK_FORMAT = "qqihh"

    def pack_flock(kind, whence, start, length, pid):
        return struct.pack(FLOCK_FORMAT, start, length, pid, kind, whence)

    def unpack_flock(data):
        start, length, pid, kind, whence = struct.unpack(FLOCK_FORMAT, data)
        return kind, whence, start, length, pid
else:
    FLOCK_FORMAT = "hhqqi4x"  # struct flock on LP64 Linux (32 bytes)

    def pack_flock(kind, whence, start, length, pid):
        return struct.pack(FLOCK_FORMAT, kind, whence, start, length, pid)

    def unpack_flock(data):
        return struct.unpack(FLOCK_FORMAT, data)
BUSY = (errno.EAGAIN, errno.EACCES)


class Interrupted(Exception):
    pass


def lock(fd, kind, mode, start, length, block):
    if kind == "flock":
        operation = fcntl.LOCK_EX if mode == "ex" else fcntl.LOCK_SH
        fcntl.flock(fd, operation | (0 if block else fcntl.LOCK_NB))
    else:
        operation = fcntl.LOCK_EX if mode == "ex" else fcntl.LOCK_SH
        fcntl.lockf(fd, operation | (0 if block else fcntl.LOCK_NB), length, start, os.SEEK_SET)


def say(text):
    sys.stdout.write(text + "\n")
    sys.stdout.flush()


def hold(argv):
    if "--hold" in argv:
        sys.stdin.readline()


def main(argv):
    command, path = argv[1], argv[2]
    flags = os.O_RDWR
    fd = os.open(path, flags)
    if command == "try":
        kind, mode, start, length = argv[3], argv[4], int(argv[5]), int(argv[6])
        try:
            lock(fd, kind, mode, start, length, block=False)
        except OSError as error:
            if error.errno not in BUSY:
                raise
            say("busy")
            return 0
        say("granted")
        hold(argv)
    elif command == "wait":
        kind, mode, start, length = argv[3], argv[4], int(argv[5]), int(argv[6])
        say("waiting")
        lock(fd, kind, mode, start, length, block=True)
        say("granted")
        hold(argv)
    elif command == "interrupt":
        kind, mode, start, length = argv[3], argv[4], int(argv[5]), int(argv[6])

        def alarm(signum, frame):
            raise Interrupted()

        signal.signal(signal.SIGALRM, alarm)
        signal.alarm(1)
        try:
            lock(fd, kind, mode, start, length, block=True)
        except Interrupted:
            say("interrupted")
            return 0
        signal.alarm(0)
        say("granted")
    elif command == "getlk":
        start, length = int(argv[3]), int(argv[4])
        query = pack_flock(fcntl.F_WRLCK, os.SEEK_SET, start, length, 0)
        result = unpack_flock(fcntl.fcntl(fd, fcntl.F_GETLK, query))
        names = {fcntl.F_RDLCK: "rdlck", fcntl.F_WRLCK: "wrlck", fcntl.F_UNLCK: "unlck"}
        say("{} {} {}".format(names.get(result[0], result[0]), result[2], result[3]))
    elif command == "deadlock":
        fcntl.lockf(fd, fcntl.LOCK_EX, 1, 10, os.SEEK_SET)
        say("holding")
        fcntl.lockf(fd, fcntl.LOCK_EX, 1, 0, os.SEEK_SET)
        say("granted")
    elif command == "count":
        iterations, kind = int(argv[3]), argv[4]
        os.close(fd)
        for _ in range(iterations):
            # Open before locking: the descriptor's view must still be refreshed
            # by the previous holder's write-back.
            with open(path, "r+b") as stream:
                lock(stream.fileno(), kind, "ex", 0, 0, block=True)
                value = int(stream.read().decode() or "0")
                stream.seek(0)
                stream.write(str(value + 1).encode())
                stream.truncate()
                stream.flush()
            # close() wrote the data back and released the lock.
        say("done")
        return 0
    else:
        raise SystemExit("unknown command: " + command)
    os.close(fd)
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv))
