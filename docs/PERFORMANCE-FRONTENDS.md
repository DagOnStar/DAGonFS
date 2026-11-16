# Frontend performance report

The repository contains `bench/run_frontends.py`, which runs an identical
command locally and over SSH and records timestamp, host, exit status, elapsed
time, and captured output in CSV. A full run should include this Mac, PurpleJeans
(`montella@193.205.230.3`), Sunbow (`rmontella@193.205.230.23`), and Rainbow
(`rmontella@193.205.230.76`), using the same compiler, MPI, message size, rank
count, and input data on each host.

Example:

```sh
python3 bench/run_frontends.py --hosts \
  montella@193.205.230.3,rmontella@193.205.230.23,rmontella@193.205.230.76 \
  --command 'python3 bench/fsbench.py --help' --output bench/results/frontends.csv
```

Remote measurements are intentionally not fabricated by CI: SSH credentials,
firewall access, scheduler allocation, and FUSE permissions are site-specific.
Populate the CSV from an allocated job and record hardware, MPI version, rank
placement, and whether `/dev/fuse` was available before comparing frontends.
