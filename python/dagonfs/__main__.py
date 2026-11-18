"""Command-line client for native DAGonFS volumes.

    python -m dagonfs [--socket SOCKET] ping
    python -m dagonfs [--socket SOCKET] ls  [PATH]
    python -m dagonfs [--socket SOCKET] put LOCAL REMOTE
    python -m dagonfs [--socket SOCKET] get REMOTE LOCAL
    python -m dagonfs [--socket SOCKET] mkdir PATH
    python -m dagonfs [--socket SOCKET] rm PATH
    python -m dagonfs [--socket SOCKET] wait [--timeout SECONDS]
    python -m dagonfs [--socket SOCKET] shutdown

The socket defaults to ``$DAGONFS_SOCKET``.  Exit status 0 means success.
"""

import argparse
import os
import sys

from .native import Client, wait_until_ready


def main(argv=None):
    parser = argparse.ArgumentParser(prog="python -m dagonfs", description=__doc__.split("\n")[0])
    parser.add_argument("--socket", default=os.environ.get("DAGONFS_SOCKET"))
    commands = parser.add_subparsers(dest="command", required=True)
    commands.add_parser("ping")
    commands.add_parser("ls").add_argument("path", nargs="?", default="/")
    for name, first, second in (("put", "local", "remote"), ("get", "remote", "local")):
        sub = commands.add_parser(name)
        sub.add_argument(first)
        sub.add_argument(second)
    commands.add_parser("mkdir").add_argument("path")
    commands.add_parser("rm").add_argument("path")
    commands.add_parser("wait").add_argument("--timeout", type=float, default=30.0)
    commands.add_parser("shutdown")
    options = parser.parse_args(argv)
    if not options.socket:
        parser.error("no --socket given and DAGONFS_SOCKET is not set")
    if options.command == "wait":
        return 0 if wait_until_ready(options.socket, options.timeout) else 1
    try:
        with Client(options.socket) as client:
            if options.command == "ping":
                return 0 if client.ping() else 1
            if options.command == "ls":
                for name in client.listdir(options.path):
                    print(name)
            elif options.command == "put":
                print(client.put(options.local, options.remote))
            elif options.command == "get":
                print(client.get(options.remote, options.local))
            elif options.command == "mkdir":
                client.makedirs(options.path)
            elif options.command == "rm":
                if client.stat(options.path).is_dir():
                    client.rmdir(options.path)
                else:
                    client.unlink(options.path)
            elif options.command == "shutdown":
                client.shutdown()
    except OSError as error:
        print("dagonfs: %s" % error, file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
