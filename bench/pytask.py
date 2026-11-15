#!/usr/bin/env python3
"""Task program of the ``python`` benchmark workflow (bench/workflow.py).

Every task reads and writes its files with Python's own file objects, in
1 MiB requests, as typical Python workflow steps do::

    pytask.py produce   --parts W --size-mib S          # writes part0.bin ... part<W-1>.bin
    pytask.py transform --input FILE --output FILE      # writes every 1 MiB chunk twice
    pytask.py reduce    --output FILE INPUT...          # checks, concatenates, writes size.txt

Contents are deterministic: the reducer recomputes every chunk of input i
(the output of the transform of part i) and compares, so a run that read or
staged wrong data fails instead of being timed.
"""

import argparse
import hashlib
import os
import sys

MiB = 1 << 20


def chunk(index, number):
    """Deterministic 1 MiB chunk (repeated 4 KiB pattern: cheap to produce)."""
    seed = hashlib.sha256("{}-{}".format(index, number).encode()).digest()
    return (seed * (4096 // len(seed))) * (MiB // 4096)


def produce(args):
    for index in range(args.parts):
        with open("part{}.bin".format(index), "wb") as stream:
            for number in range(args.size_mib):
                stream.write(chunk(index, number))


def transform(args):
    with open(args.input, "rb") as source, open(args.output, "wb") as target:
        while True:
            data = source.read(MiB)
            if not data:
                break
            target.write(data)
            target.write(data)
        target.flush()
        os.fsync(target.fileno())


def reduce(args):
    total = 0
    with open(args.output, "wb") as target:
        for index, path in enumerate(args.inputs):
            with open(path, "rb") as source:
                number = 0
                while True:
                    first, second = source.read(MiB), source.read(MiB)
                    if not first:
                        break
                    if first != second or first != chunk(index, number):
                        sys.exit("wrong content in {} (input {}, chunk {})".format(path, index, number))
                    target.write(first)
                    target.write(second)
                    total += len(first) + len(second)
                    number += 1
    with open("size.txt", "w") as stream:
        stream.write("{} {}\n".format(total, args.output))


def main():
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    commands = parser.add_subparsers(dest="command", required=True)
    p = commands.add_parser("produce")
    p.add_argument("--parts", type=int, required=True)
    p.add_argument("--size-mib", type=int, required=True)
    p = commands.add_parser("transform")
    p.add_argument("--input", required=True)
    p.add_argument("--output", required=True)
    p = commands.add_parser("reduce")
    p.add_argument("--output", required=True)
    p.add_argument("inputs", nargs="+")
    args = parser.parse_args()
    {"produce": produce, "transform": transform, "reduce": reduce}[args.command](args)


if __name__ == "__main__":
    main()
