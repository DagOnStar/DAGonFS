#!/usr/bin/env python3
"""Reducer task (C): check every transformed part and the shared ledger.

Writes ``result.json`` in its working directory and in the shared directory;
exits with status 1 on the first inconsistency.
"""

import argparse
import fcntl
import json
import os
import struct

import common


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--manifest", required=True)
    parser.add_argument("--shared", required=True)
    parser.add_argument("outputs", nargs="+", help="out.bin of every B task (workflow:// references)")
    args = parser.parse_args()

    with open(args.manifest) as stream:
        manifest = json.load(stream)
    common.check(len(args.outputs) == manifest["parts"], "expected one output per part")

    digests = {}
    for index, output in enumerate(args.outputs):
        with open(output, "rb") as stream:
            data = stream.read()
        common.check(common.sha256_bytes(data[::-1]) == manifest["sha256"][index],
                     "output {} is not the reversed part".format(index))
        digests["B{}".format(index)] = common.sha256_bytes(data)

    # Every transform recorded itself once, under the record lock.
    ledger = os.path.join(args.shared, "ledger.txt")
    lines = common.locked_read(ledger).decode().splitlines()
    common.check(lines[0] == "A {}".format(manifest["parts"]), "ledger header: {!r}".format(lines[:1]))
    recorded = dict(line.split(" ", 1) for line in lines[1:])
    common.check(len(lines) == manifest["parts"] + 1, "ledger has {} lines".format(len(lines)))
    common.check(recorded == digests, "ledger digests differ from the outputs")

    # Every increment of the flock-protected counter survived.
    counter = os.path.join(args.shared, "counter")
    with open(counter, "rb") as stream:
        fcntl.flock(stream.fileno(), fcntl.LOCK_SH)
        value = int(stream.read().decode())
    common.check(value == manifest["parts"], "counter is {}, expected {}".format(value, manifest["parts"]))

    # No lock is left behind.
    with open(ledger, "rb") as stream:
        query = struct.pack("hhqqi4x", fcntl.F_WRLCK, os.SEEK_SET, 0, 0, 0)
        answer = struct.unpack("hhqqi4x", fcntl.fcntl(stream.fileno(), fcntl.F_GETLK, query))
    common.check(answer[0] == fcntl.F_UNLCK, "a record lock is still held on the ledger")

    result = {"ok": True, "parts": manifest["parts"], "ledger_lines": len(lines), "counter": value}
    for target in ("result.json", os.path.join(args.shared, "result.json")):
        with open(target + ".tmp", "w") as stream:
            json.dump(result, stream)
        os.replace(target + ".tmp", target)
    print(json.dumps(result))


if __name__ == "__main__":
    main()
