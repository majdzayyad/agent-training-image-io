#!/usr/bin/env python3
"""Sequential, matched reader calibration for the training-pipeline simulation."""

import argparse
import csv
import fcntl
import hashlib
import json
import math
import os
import platform
import random
import re
import shutil
import statistics
import subprocess
import threading
import time
from datetime import datetime, timezone
from pathlib import Path

from scripts.generate import trace
from scripts.range_server import ImageServer

ROOT = Path(__file__).resolve().parent
FIXED = ("fixed16", "fixed64", "fixed256", "fixed1024")


def digest(path):
    value = hashlib.sha256()
    with path.open("rb") as source:
        while block := source.read(1024 * 1024):
            value.update(block)
    return value.hexdigest()


def write_new(path, value):
    with path.open("x") as output:
        json.dump(value, output, indent=2, sort_keys=True)
        output.write("\n")


def source_hashes():
    return {
        str(path.relative_to(ROOT)): digest(path)
        for pattern in (
            "calibrate.py", "simulate.py", "experiment.json", "CMakeLists.txt", ".clang-format",
            "include/**/*.hpp", "src/*.cpp", "benchmarks/*.cpp", "tests/*", "scripts/*.py",
        )
        for path in sorted(ROOT.glob(pattern))
        if path.is_file()
    }


def result_path(label):
    if not re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9._-]*", label) or label in (".", ".."):
        raise ValueError("output must be a new result-directory label, not a path")
    return ROOT / "results" / label


def select_fixed(rows):
    if not rows or any(row["status"] != "ok" for row in rows):
        raise ValueError("development selection requires all valid scheduled rows")
    groups = {arm: [r for r in rows if r["arm"] == arm] for arm in FIXED}
    keys = [{(r["case"], r["repeat"]) for r in values} for values in groups.values()]
    if not keys[0] or any(key != keys[0] for key in keys):
        raise ValueError("unbalanced fixed-size development data")
    if any(len(values) != len(keys[0]) for values in groups.values()):
        raise ValueError("duplicate development rows")
    scores = {
        arm: math.exp(statistics.fmean(math.log(r["process_wall_s"]) for r in values))
        for arm, values in groups.items()
    }
    return {"arm": min(FIXED, key=lambda arm: (scores[arm], FIXED.index(arm))),
            "geometric_process_wall_s": scores}


def load_selection(label):
    path = result_path(label)
    manifest = json.loads((path / "manifest.json").read_text())
    complete = json.loads((path / "complete.json").read_text())
    selection = json.loads((path / "selection.json").read_text())
    rows = [json.loads(line) for line in (path / "runs.jsonl").read_text().splitlines()]
    if (manifest["stage"] != "development" or complete["failures"] or
            complete["runs"] != complete["expected_runs"] or len(rows) != complete["runs"] or
            digest(path / "runs.jsonl") != complete["runs_sha256"] or
            selection["runs_sha256"] != complete["runs_sha256"] or
            selection["choice"] != select_fixed(rows)):
        raise ValueError("development selection provenance mismatch")
    return selection


def cases_for(protocol, stage_name):
    stage = protocol["stages"][stage_name]
    cases = []
    for pattern, requests in stage["patterns"].items():
        for clients in stage["clients"]:
            for profile in stage["profiles"]:
                for identity in stage.get("identities", ["logical"]):
                    for frontend in stage.get("frontends", ["concurrent"]):
                        cases.append({
                            "id": f"{pattern}-c{clients}-{profile}-{identity}-{frontend}",
                            "pattern": pattern, "requests": requests, "clients": clients,
                            "profile": profile, "identity": identity, "frontend": frontend,
                            "queue": stage.get("queue", protocol["queue_feedback"]),
                            "cache_mib": stage.get("cache_mib_by_pattern", {}).get(
                                pattern, protocol["cache_mib"]),
                            "seed": stage["seed"], **protocol["profiles"][profile],
                        })
    if len(cases) * len(stage["arms"]) * protocol["repeats"] != stage["expected_runs"]:
        raise ValueError("protocol run count mismatch")
    return cases


def validate_row(row, case, protocol, arm, checksum):
    if row["checksum"] != checksum or row["policy"] != arm["policy"]:
        raise ValueError("checksum or policy mismatch")
    if row["clients"] != case["clients"] or row["reads"] != case["clients"] * case["requests"]:
        raise ValueError("workload count mismatch")
    for key in ("failed_fetches", "cache_admission_failures", "prefetch_bytes"):
        if row.get(key, 0):
            raise ValueError(f"unexpected {key}")
    if (row["peak_cache_bytes"] > case["cache_mib"] * 2**20 or
            row["peak_inflight_bytes"] > protocol["inflight_mib"] * 2**20):
        raise ValueError("payload budget exceeded")
    for key in ("elapsed_s", "p95_us", "p99_us"):
        if not math.isfinite(row[key]) or row[key] <= 0:
            raise ValueError(f"invalid {key}")
    if arm["binary"] == "reader":
        expected = {
            "refill_rule": arm.get("rule", "cost"),
            "queue_feedback": arm.get("queue", case["queue"]),
            "stream_identity": case["identity"], "frontend": case["frontend"],
        }
        if any(row[key] != value for key, value in expected.items()):
            raise ValueError("diagnostic flag mismatch")


def check_quiescence():
    output = subprocess.check_output(["ps", "-eo", "pid=,comm=,args="], text=True)
    competing = []
    for line in output.splitlines():
        pid, command, arguments = line.strip().split(None, 2)
        if int(pid) == os.getpid():
            continue
        if command in ("replay", "replay.frozen", "ninja", "cmake", "ctest") or (
            command.startswith("python") and any(
                name in arguments for name in ("measure.py", "run_real.py")
            )
        ):
            competing.append(line.strip())
    if competing:
        raise RuntimeError("other timing/build processes are active: " + repr(competing))
    return {"checked_utc": datetime.now(timezone.utc).isoformat(),
            "competing_processes": [], "dedicated_physical_host": False}


def run(args):
    if not args.quiescence_ack:
        raise ValueError("timing requires --quiescence-ack after checking project activity")
    protocol = json.loads((ROOT / "experiment.json").read_text())
    stage = protocol["stages"][args.stage]
    selection = load_selection(args.development) if args.stage == "evaluation" else None
    if selection and selection["protocol_sha256"] != digest(ROOT / "experiment.json"):
        raise ValueError("development selection used a different experiment protocol")
    source_start = source_hashes()
    quiescence = check_quiescence()
    output = result_path(args.output)
    output.mkdir(parents=True, exist_ok=False)
    (output / "bin").mkdir()
    binaries = {"reader": output / "bin" / "replay.frozen"}
    shutil.copy2(ROOT / "build" / "replay", binaries["reader"])
    binary_hashes = {name: digest(path) for name, path in binaries.items()}
    image = ROOT / "data" / "image.bin"
    image_sha256 = digest(image)
    if image_sha256 != protocol["image"]["sha256"] or image.stat().st_size != protocol["image"]["bytes"]:
        raise ValueError("image does not match the frozen generated input")
    (output / "traces").mkdir()
    cases = cases_for(protocol, args.stage)
    traces, checksums = {}, {}
    for case in cases:
        name = f'{case["pattern"]}-c{case["clients"]}-s{case["seed"]}'
        if name not in traces:
            path = output / "traces" / (name + ".csv")
            trace(path, image.stat().st_size, case["clients"], case["requests"],
                  case["pattern"], case["seed"])
            reference = subprocess.run(
                [str(binaries["reader"]), "--source", str(image), "--trace", str(path), "--policy", "local"],
                check=True, capture_output=True, text=True, timeout=protocol["timeout_s"],
            )
            checksums[name] = json.loads(reference.stdout)["checksum"]
            traces[name] = {"path": str(path.relative_to(output)), "sha256": digest(path)}
        case["trace"] = traces[name]
        case["expected_checksum"] = checksums[name]
    sequence = []
    rng = random.Random(protocol["order_seed"])
    for repeat in range(protocol["repeats"]):
        order = cases.copy()
        rng.shuffle(order)
        for case in order:
            arms = stage["arms"].copy()
            rng.shuffle(arms)
            sequence.extend({"repeat": repeat, "case": case, "arm": arm} for arm in arms)
    manifest = {
        "started_utc": datetime.now(timezone.utc).isoformat(), "stage": args.stage,
        "protocol": protocol, "protocol_sha256": digest(ROOT / "experiment.json"),
        "source_sha256_start": source_start, "binary_sha256": binary_hashes,
        "image_sha256": image_sha256, "image_bytes": image.stat().st_size,
        "selection_frozen_before_evaluation": selection, "sequence": sequence,
        "quiescence": quiescence, "kernel": platform.release(),
        "cpu_count": os.cpu_count(), "python": platform.python_version(),
        "compiler": subprocess.check_output(["c++", "--version"], text=True).splitlines()[0],
        "checksum_reference": "same frozen executable, local unshaped reads, before timing",
    }
    write_new(output / "manifest.json", manifest)
    progress = {"completed": 0, "total": len(sequence), "state": "starting"}
    stop = threading.Event()

    def heartbeat():
        while not stop.is_set():
            snapshot = {**progress, "updated_utc": datetime.now(timezone.utc).isoformat()}
            temporary = output / "progress.tmp"
            temporary.write_text(json.dumps(snapshot) + "\n")
            temporary.replace(output / "progress.json")
            stop.wait(5)

    monitor = threading.Thread(target=heartbeat, daemon=True)
    monitor.start()
    rows = []
    try:
        with ImageServer(("127.0.0.1", 0), image) as server:
            helper = threading.Thread(target=server.serve_forever, daemon=True)
            helper.start()
            try:
                url = f"http://127.0.0.1:{server.server_port}/image.bin"
                with (output / "runs.jsonl").open("x") as journal:
                    for index, specification in enumerate(sequence):
                        case = specification["case"]
                        arm = protocol["arms"][specification["arm"]]
                        argv = [
                            str(binaries[arm["binary"]]), "--source", url,
                            "--trace", str(output / case["trace"]["path"]), "--policy", arm["policy"],
                            "--cache-mib", str(case["cache_mib"]),
                            "--workers", str(protocol["workers"]),
                            "--inflight-mib", str(protocol["inflight_mib"]),
                            "--latency-us", str(case["latency_us"]),
                            "--bandwidth-mib", str(case["bandwidth_mib"]),
                        ]
                        if arm["binary"] == "reader":
                            argv.extend([
                                "--refill-rule", arm.get("rule", "cost"),
                                "--queue-feedback", arm.get("queue", case["queue"]),
                                "--stream-identity", case["identity"],
                                "--frontend", case["frontend"],
                            ])
                        progress.update(state="running", case=case["id"], arm=specification["arm"],
                                        run_started_utc=datetime.now(timezone.utc).isoformat())
                        begin = time.perf_counter()
                        with subprocess.Popen(argv, stdout=subprocess.PIPE, stderr=subprocess.PIPE,
                                              text=True) as child:
                            progress["child_pid"] = child.pid
                            status = "ok"
                            try:
                                stdout, stderr = child.communicate(timeout=protocol["timeout_s"])
                                if child.returncode:
                                    status = "failure"
                            except subprocess.TimeoutExpired:
                                child.kill()
                                stdout, stderr = child.communicate()
                                status = "timeout"
                        wall = time.perf_counter() - begin
                        row = {}
                        if status == "ok":
                            try:
                                row = json.loads(stdout)
                                validate_row(row, case, protocol, arm, case["expected_checksum"])
                            except (ValueError, KeyError, TypeError) as error:
                                status = "invalid"
                                stderr += "\n" + str(error)
                        row.update({
                            "status": status, "process_wall_s": wall, "case": case["id"],
                            "arm": specification["arm"], "repeat": specification["repeat"],
                            "order": index, "pattern": case["pattern"], "profile": case["profile"],
                            "clients": case["clients"], "seed": case["seed"],
                            "cache_mib": case["cache_mib"], "identity": case["identity"],
                            "frontend_factor": case["frontend"],
                            "returncode": child.returncode,
                        })
                        if status != "ok":
                            row.update(stderr=stderr, stdout=stdout)
                        rows.append(row)
                        journal.write(json.dumps(row, sort_keys=True) + "\n")
                        journal.flush()
                        os.fsync(journal.fileno())
                        progress.update(completed=len(rows), child_pid=None)
                        if (index + 1) % 25 == 0 or index + 1 == len(sequence) or status != "ok":
                            print(f'{len(rows)}/{len(sequence)} {case["id"]} '
                                  f'{specification["arm"]}: {status} {wall:.3f}s', flush=True)
            finally:
                server.shutdown()
                helper.join()
        source_end = source_hashes()
        if (source_start != source_end or
                binary_hashes != {name: digest(path) for name, path in binaries.items()} or
                image_sha256 != digest(image)):
            raise RuntimeError("source, binary, or input changed during timing")
        with (output / "runs.csv").open("x", newline="") as destination:
            writer = csv.DictWriter(destination, fieldnames=sorted({key for row in rows for key in row}),
                                    lineterminator="\n")
            writer.writeheader()
            writer.writerows(rows)
        complete = {
            "finished_utc": datetime.now(timezone.utc).isoformat(), "runs": len(rows),
            "expected_runs": len(sequence), "failures": sum(r["status"] != "ok" for r in rows),
            "runs_sha256": digest(output / "runs.jsonl"), "source_sha256_end": source_end,
            "binary_sha256_end": binary_hashes, "helper_server_stopped": True,
            "start_end_sources_binaries_image_match": True,
        }
        write_new(output / "complete.json", complete)
        if args.stage == "development":
            write_new(output / "selection.json", {
                "created_utc": datetime.now(timezone.utc).isoformat(),
                "choice": select_fixed(rows), "runs_sha256": complete["runs_sha256"],
                "protocol_sha256": manifest["protocol_sha256"],
                "development_directory": output.name,
            })
        progress["state"] = "complete" if not complete["failures"] else "complete-with-failures"
        print(json.dumps({key: value for key, value in complete.items()
                          if key not in ("source_sha256_end", "binary_sha256_end")}), flush=True)
    except BaseException as error:
        progress["state"] = "aborted"
        write_new(output / "failure.json", {"error": str(error), "completed": len(rows)})
        raise
    finally:
        stop.set()
        monitor.join()
        (output / "progress.json").write_text(json.dumps({
            **progress, "updated_utc": datetime.now(timezone.utc).isoformat()
        }) + "\n")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--stage", choices=("development", "evaluation", "identity"),
                        required=True)
    parser.add_argument("--output", required=True, help="new directory label under results")
    parser.add_argument("--development", help="completed development label; required for evaluation")
    parser.add_argument("--quiescence-ack", action="store_true")
    args = parser.parse_args()
    if args.stage == "evaluation" and not args.development:
        parser.error("--evaluation requires --development")
    with (ROOT / ".timing.lock").open("a") as guard:
        fcntl.flock(guard, fcntl.LOCK_EX | fcntl.LOCK_NB)
        run(args)
