#!/usr/bin/env python3
"""Reproducible frontend benchmark runner.

Runs the same local benchmark command for FUSE/native/preload and optionally
dispatches it to hosts supplied with --hosts user@host,... via ssh.
"""
import argparse, csv, datetime, os, subprocess, time

def run(label, command):
    start=time.perf_counter(); p=subprocess.run(command, shell=True, text=True,
                                                stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
    return label, p.returncode, time.perf_counter()-start, p.stdout

def main():
    ap=argparse.ArgumentParser(); ap.add_argument("--command", required=True)
    ap.add_argument("--hosts", default=""); ap.add_argument("--output", default="frontend-results.csv")
    args=ap.parse_args(); hosts=[h for h in args.hosts.split(",") if h]
    targets=[("local", args.command)] + [(h, f"ssh {h} {subprocess.list2cmdline([args.command])}") for h in hosts]
    rows=[]
    for host, command in targets:
        label, code, seconds, output=run(host, command)
        rows.append({"timestamp":datetime.datetime.now(datetime.timezone.utc).isoformat(),"host":label,"returncode":code,"seconds":f"{seconds:.6f}","output":output[-2000:]})
    with open(args.output,"w",newline="",encoding="utf-8") as f: w=csv.DictWriter(f,fieldnames=rows[0]); w.writeheader(); w.writerows(rows)
    return 0 if all(int(r["returncode"])==0 for r in rows) else 1
if __name__=="__main__": raise SystemExit(main())
