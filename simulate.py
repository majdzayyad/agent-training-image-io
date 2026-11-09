#!/usr/bin/env python3
"""Matched training-execution analysis with explicit inference accounting."""

import argparse
import collections
import hashlib
import heapq
import json
import math
import random
import statistics
from pathlib import Path

from calibrate import digest, select_fixed, write_new

ROOT = Path(__file__).resolve().parent
KINDS = ("delay", "inference", "tools", "cpu", "inference", "delay", "delay")
T95_3 = 4.302652729911275


def make_work(config, profile, seed):
    rng = random.Random(seed)
    low, high = config["service_jitter"]
    resources = {
        "actors": config["actors"], "inference": config["inference_workers"],
        "tools": config["tool_workers"], "cpu": config["cpu_workers"],
    }
    if any(not isinstance(value, int) or value < 1 for value in resources.values()):
        raise ValueError("resource capacities must be positive integers")
    if not 0 < low <= high or config["updates"] < 1 or config["rollouts_per_update"] < 1:
        raise ValueError("invalid work or jitter configuration")
    scale = (
        config["setup_s"], profile["inference_s"], 1.0, profile["cpu_s"],
        profile["inference_s"], config["verification_s"], config["teardown_s"],
    )
    if any(not math.isfinite(value) or value < 0 for value in (*scale, profile["learner_s"])):
        raise ValueError("service times must be finite and nonnegative")
    updates = []
    for _ in range(config["updates"]):
        jobs = [[value * rng.uniform(low, high) for value in scale]
                for _ in range(config["rollouts_per_update"])]
        rng.shuffle(jobs)
        updates.append({"jobs": jobs, "learner_s": profile["learner_s"] * rng.uniform(low, high)})
    work = {"resources": resources, "updates": updates}
    encoded = json.dumps(work, sort_keys=True, separators=(",", ":")).encode()
    return work, hashlib.sha256(encoded).hexdigest()


def simulate_batch(jobs, capacities, *, include_inference=True):
    if not jobs or any(len(job) != len(KINDS) for job in jobs):
        raise ValueError("each nonempty batch needs complete rollout phases")
    if any(not math.isfinite(value) or value < 0 for job in jobs for value in job):
        raise ValueError("invalid phase duration")
    if any(not isinstance(capacities.get(key), int) or capacities[key] < 1
           for key in ("actors", "inference", "tools", "cpu")):
        raise ValueError("invalid resource capacity")
    active = {kind: 0 for kind in ("inference", "tools", "cpu")}
    queues = {kind: collections.deque() for kind in active}
    busy = {kind: 0.0 for kind in active}
    waits = {kind: 0.0 for kind in active}
    peak = {kind: 0 for kind in active}
    pending = collections.deque(range(len(jobs)))
    kinds = tuple("delay" if kind == "inference" and not include_inference else kind
                  for kind in KINDS)
    events = []
    serial = 0
    completed = 0

    def launch(now, job, phase, arrived):
        nonlocal serial
        kind = kinds[phase]
        duration = 0.0 if KINDS[phase] == "inference" and not include_inference else jobs[job][phase]
        if kind != "delay":
            active[kind] += 1
            peak[kind] = max(peak[kind], active[kind])
            busy[kind] += duration
            waits[kind] += now - arrived
        serial += 1
        heapq.heappush(events, (now + duration, serial, job, phase))

    def submit(now, job, phase):
        kind = kinds[phase]
        if kind == "delay" or active[kind] < capacities[kind]:
            launch(now, job, phase, now)
        else:
            queues[kind].append((job, phase, now))

    for _ in range(min(capacities["actors"], len(jobs))):
        submit(0.0, pending.popleft(), 0)
    now = 0.0
    while events:
        now, _, job, phase = heapq.heappop(events)
        kind = kinds[phase]
        if kind != "delay":
            active[kind] -= 1
            if queues[kind]:
                waiting_job, waiting_phase, arrived = queues[kind].popleft()
                launch(now, waiting_job, waiting_phase, arrived)
        if phase + 1 < len(KINDS):
            submit(now, job, phase + 1)
        else:
            completed += 1
            if pending:
                submit(now, pending.popleft(), 0)
    if completed != len(jobs) or pending or any(queues.values()) or any(active.values()):
        raise RuntimeError("incomplete rollout batch")
    return {"wall_s": now, "completed": completed, "resource_busy_s": busy,
            "summed_resource_wait_s": waits, "peak_occupancy": peak}


def training_time(work, tool_seconds, *, include_inference=True):
    if not math.isfinite(tool_seconds) or tool_seconds < 0:
        raise ValueError("invalid measured tool duration")
    total = rollout_time = learner_time = 0.0
    completed = 0
    busy = {kind: 0.0 for kind in ("inference", "tools", "cpu")}
    waits = dict(busy)
    peak = {kind: 0 for kind in busy}
    for update in work["updates"]:
        jobs = [job.copy() for job in update["jobs"]]
        for job in jobs:
            job[2] *= tool_seconds
        batch = simulate_batch(jobs, work["resources"], include_inference=include_inference)
        total += batch["wall_s"] + update["learner_s"]
        rollout_time += batch["wall_s"]
        learner_time += update["learner_s"]
        completed += batch["completed"]
        for kind in busy:
            busy[kind] += batch["resource_busy_s"][kind]
            waits[kind] += batch["summed_resource_wait_s"][kind]
            peak[kind] = max(peak[kind], batch["peak_occupancy"][kind])
    return {
        "training_s": total, "rollout_phase_s": rollout_time, "learner_phase_s": learner_time,
        "completed_rollouts": completed, "completed_updates": len(work["updates"]),
        "model_calls": 2 * completed, "resource_busy_s": busy,
        "summed_resource_wait_s": waits, "peak_occupancy": peak,
        "inference_accounting": "included" if include_inference else "excluded",
    }


def verify_source_snapshot(source_identity):
    analysis_files = {"simulate.py", "tests/test_experiment.py"}
    for relative, expected_hash in source_identity.items():
        if digest(ROOT / relative) == expected_hash:
            continue
        archived = ROOT / "provenance" / "calibration-v1" / relative
        if relative in analysis_files and archived.is_file() and digest(archived) == expected_hash:
            continue
        raise ValueError(f"measured reader/calibration source changed: {relative}")


def load_calibration(directory, protocol):
    datasets, manifests = {}, {}
    protocol_sha = digest(ROOT / "experiment.json")
    source_identity = None
    binary_identity = None
    for stage in ("development", "evaluation", "identity"):
        path = directory / stage
        manifest = json.loads((path / "manifest.json").read_text())
        complete = json.loads((path / "complete.json").read_text())
        raw = path / "runs.jsonl"
        rows = [json.loads(line) for line in raw.read_text().splitlines()]
        expected = protocol["stages"][stage]["expected_runs"]
        if (digest(raw) != complete["runs_sha256"] or len(rows) != expected or
                complete["runs"] != expected or complete["expected_runs"] != expected or
                complete["failures"] or manifest["stage"] != stage or
                manifest["protocol_sha256"] != protocol_sha or
                manifest["source_sha256_start"] != complete["source_sha256_end"] or
                manifest["binary_sha256"] != complete["binary_sha256_end"]):
            raise ValueError(f"incomplete, changed or incompatible calibration: {stage}")
        if source_identity is None:
            source_identity = manifest["source_sha256_start"]
            binary_identity = manifest["binary_sha256"]
        if source_identity != manifest["source_sha256_start"]:
            raise ValueError("calibration stages used different source")
        if binary_identity != manifest["binary_sha256"]:
            raise ValueError("calibration stages used different reader builds")
        verify_source_snapshot(source_identity)
        seen = set()
        checksums = {}
        for index, (row, specification) in enumerate(zip(rows, manifest["sequence"])):
            key = row["case"], row["repeat"], row["arm"]
            if key in seen or row["status"] != "ok":
                raise ValueError("duplicate or failed calibration row")
            seen.add(key)
            if (row["order"], *key) != (
                index, specification["case"]["id"], specification["repeat"], specification["arm"]
            ):
                raise ValueError("calibration order differs from frozen schedule")
            if (not math.isfinite(row["process_wall_s"]) or row["process_wall_s"] <= 0 or
                    row["failed_fetches"] or row["cache_admission_failures"] or row["prefetch_bytes"]):
                raise ValueError("invalid reader measurement")
            if row["checksum"] != checksums.setdefault(row["case"], row["checksum"]):
                raise ValueError("policies did not perform identical logical reads")
            trace = specification["case"]["trace"]
            if trace["path"] not in checksums:
                if digest(path / trace["path"]) != trace["sha256"]:
                    raise ValueError("calibration trace changed")
                checksums[trace["path"]] = trace["sha256"]
        if len(manifest["sequence"]) != len(rows):
            raise ValueError("missing scheduled calibration rows")
        datasets[stage], manifests[stage] = rows, manifest
    selection = json.loads((directory / "development" / "selection.json").read_text())
    if (selection["choice"] != select_fixed(datasets["development"]) or
            selection["runs_sha256"] != digest(directory / "development" / "runs.jsonl") or
            selection != manifests["evaluation"]["selection_frozen_before_evaluation"]):
        raise ValueError("static baseline was not frozen on development data")
    return datasets, selection


def effect(log_blocks):
    if len(log_blocks) != 3:
        raise ValueError("the protocol specifies three physical calibration repeats")
    center = statistics.fmean(log_blocks)
    radius = T95_3 * statistics.stdev(log_blocks) / math.sqrt(3)
    ratio = math.exp(center)
    low, high = math.exp(center - radius), math.exp(center + radius)
    return {
        "candidate_over_baseline": ratio, "ci95": [low, high],
        "reduction_pct": 100 * (1 - ratio),
        "reduction_ci95": [100 * (1 - high), 100 * (1 - low)],
    }


def offline_fixed_choices(rows):
    choices = {}
    cases = sorted({row["case"] for row in rows})
    for case in cases:
        groups = {
            arm: [row for row in rows if row["case"] == case and row["arm"] == arm]
            for arm in ("fixed16", "fixed64", "fixed256", "fixed1024")
        }
        keys = [{(row["repeat"], row["schedule_seed"]) for row in values}
                for values in groups.values()]
        if not keys[0] or any(value != keys[0] for value in keys):
            raise ValueError("incomplete fixed-size oracle inputs")
        choices[case] = min(
            groups, key=lambda arm: (
                statistics.fmean(math.log(row["execution_s"]) for row in groups[arm]),
                int(arm.removeprefix("fixed")),
            )
        )
    return choices


def summarize(rows, candidate, baseline, baseline_by_case=None):
    index = {}
    for row in rows:
        key = row["case"], row["repeat"], row["schedule_seed"], row["arm"]
        if key in index:
            raise ValueError("duplicate simulated pairing key")
        index[key] = row
    pairs = collections.defaultdict(list)
    by_case = collections.defaultdict(lambda: collections.defaultdict(list))
    baseline_times, candidate_times = [], []
    for key, row in index.items():
        case, repeat, seed, arm = key
        if arm != candidate:
            continue
        base = baseline_by_case[case] if baseline_by_case else baseline
        control = index.get((case, repeat, seed, base))
        if control is None:
            raise ValueError("missing paired simulation")
        for invariant in ("work_sha256", "completed_rollouts", "completed_updates", "model_calls",
                          "logical_bytes_per_job", "logical_checksum", "inference_accounting"):
            if row[invariant] != control[invariant]:
                raise ValueError(f"unequal work: {invariant}")
        log_ratio = math.log(row["execution_s"] / control["execution_s"])
        pairs[repeat].append(log_ratio)
        by_case[case][repeat].append(log_ratio)
        baseline_times.append(math.log(control["execution_s"]))
        candidate_times.append(math.log(row["execution_s"]))
    if not pairs or len({len(values) for values in pairs.values()}) != 1:
        raise ValueError("unbalanced repeat blocks")
    conditions = {
        case: effect([statistics.fmean(values[repeat]) for repeat in sorted(values)])
        for case, values in sorted(by_case.items())
    }
    result = effect([statistics.fmean(pairs[repeat]) for repeat in sorted(pairs)])
    return {
        "candidate": candidate, "baseline": baseline, **result,
        "baseline_geometric_s": math.exp(statistics.fmean(baseline_times)),
        "candidate_geometric_s": math.exp(statistics.fmean(candidate_times)),
        "conditions": conditions,
        "condition_reduction_range_pct": [
            min(value["reduction_pct"] for value in conditions.values()),
            max(value["reduction_pct"] for value in conditions.values()),
        ],
        "point_regressions": sum(value["candidate_over_baseline"] > 1
                                 for value in conditions.values()),
    }


def run(calibration, output, analysis_path=None):
    protocol = json.loads((ROOT / "experiment.json").read_text())
    analysis_path = Path(analysis_path) if analysis_path is not None else ROOT / "analysis.json"
    analysis = json.loads(analysis_path.read_text())
    accounting = analysis["inference_accounting"]
    profiles = analysis["profiles"]
    if accounting not in ("included", "excluded"):
        raise ValueError("inference accounting must be included or excluded")
    if (not profiles or len(set(profiles)) != len(profiles) or
            any(name not in protocol["simulation"]["profiles"] for name in profiles)):
        raise ValueError("invalid analysis profile selection")
    include_inference = accounting == "included"
    datasets, selection = load_calibration(calibration, protocol)
    output.mkdir(parents=True, exist_ok=False)
    config = protocol["simulation"]
    plans = {
        (profile, seed): make_work(config, config["profiles"][profile], seed)
        for profile in profiles
        for seed in config["schedule_seeds"]
    }
    input_hashes = {stage: digest(calibration / stage / "runs.jsonl") for stage in datasets}
    manifest = {
        "protocol": protocol, "protocol_sha256": digest(ROOT / "experiment.json"),
        "simulator_sha256": digest(Path(__file__)), "calibration_sha256": input_hashes,
        "analysis": analysis, "analysis_sha256": digest(analysis_path),
        "development_selection": selection,
        "endpoint": analysis["metric"],
    }
    write_new(output / "manifest.json", manifest)
    rows = []
    with (output / "runs.jsonl").open("x") as journal:
        for stage in ("evaluation", "identity"):
            for measured in datasets[stage]:
                for profile in profiles:
                    for seed in config["schedule_seeds"]:
                        work, fingerprint = plans[profile, seed]
                        value = training_time(work, measured["process_wall_s"],
                                              include_inference=include_inference)
                        value["execution_s"] = value.pop("training_s")
                        row = {
                            "stage": stage, "case": measured["case"], "arm": measured["arm"],
                            "repeat": measured["repeat"], "schedule_seed": seed,
                            "training_profile": profile, "work_sha256": fingerprint,
                            "logical_bytes_per_job": measured["logical_bytes"],
                            "logical_checksum": measured["checksum"],
                            "measured_tool_job_s": measured["process_wall_s"], **value,
                        }
                        rows.append(row)
                        journal.write(json.dumps(row, sort_keys=True) + "\n")
                if len(rows) % 512 == 0:
                    journal.flush()
                    print(f"{len(rows)} simulated runs", flush=True)
    summaries = {}
    selected = selection["choice"]["arm"]
    for stage in ("evaluation", "identity"):
        summaries[stage] = {}
        for profile in profiles:
            values = [row for row in rows if row["stage"] == stage and
                      row["training_profile"] == profile]
            summaries[stage][profile] = {
                name: summarize(values, "adaptive", baseline)
                for name, baseline in (
                    ("fixed256", "fixed256"), ("selected_fixed", selected), ("grow_reset", "grow-reset")
                )
            }
            oracle = offline_fixed_choices(values)
            summaries[stage][profile]["offline_fixed_oracle"] = {
                **summarize(values, "adaptive", "offline-fixed-oracle", oracle),
                "choices": oracle,
                "interpretation": "Best tested fixed setting chosen on evaluation data per condition; nondeployable. Intervals do not account for selection.",
            }
    expected = sum(protocol["stages"][stage]["expected_runs"] for stage in ("evaluation", "identity"))
    expected *= len(profiles) * len(config["schedule_seeds"])
    if len(rows) != expected:
        raise RuntimeError("incomplete simulation matrix")
    if (digest(Path(__file__)) != manifest["simulator_sha256"] or
            digest(ROOT / "experiment.json") != manifest["protocol_sha256"] or
            digest(analysis_path) != manifest["analysis_sha256"] or
            any(digest(calibration / stage / "runs.jsonl") != value
                for stage, value in input_hashes.items())):
        raise RuntimeError("simulation code, protocol or calibration changed during execution")
    summary = {
        "question": analysis["question"], "simulated_runs": len(rows),
        "metric": analysis["metric"], "inference_accounting": accounting,
        "analysis": analysis, "analysis_sha256": manifest["analysis_sha256"],
        "physical_calibration_runs": sum(len(values) for values in datasets.values()),
        "selected_fixed": selected, "protocol_sha256": manifest["protocol_sha256"],
        "simulator_sha256": manifest["simulator_sha256"], "raw_sha256": digest(output / "runs.jsonl"),
        "results": summaries, "uncertainty": config["inference"], "not_modeled": config["not_modeled"],
    }
    write_new(output / "summary.json", summary)
    write_new(output / "complete.json", {
        "runs": len(rows), "expected_runs": expected, "runs_sha256": summary["raw_sha256"],
        "all_work_matched": True,
    })
    print(json.dumps({
        "simulated_runs": len(rows), "selected_fixed": selected,
        "inference_accounting": accounting,
        "execution_time_reduction_pct": {
            name: {baseline: value["reduction_pct"] for baseline, value in values.items()}
            for name, values in summaries["evaluation"].items()
        },
    }, indent=2))


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--calibration", type=Path, default=ROOT / "data" / "reference")
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--analysis", type=Path, default=ROOT / "analysis.json")
    args = parser.parse_args()
    run(args.calibration.resolve(), args.output.resolve(), args.analysis.resolve())
