import copy
import hashlib
import json
import math
import sys
import tempfile
import unittest
from pathlib import Path
from unittest import mock

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from calibrate import FIXED, cases_for, result_path, select_fixed, write_new
from simulate import (
    make_work, offline_fixed_choices, simulate_batch, summarize, training_time,
    verify_source_snapshot,
)


def work(jobs, learner=0, updates=1, **resources):
    capacities = {"actors": 2, "inference": 2, "tools": 1, "cpu": 2, **resources}
    return {"resources": capacities,
            "updates": [{"jobs": jobs, "learner_s": learner} for _ in range(updates)]}


class TrainingSimulationTests(unittest.TestCase):
    def test_includes_all_rollout_phases_and_final_update(self):
        result = training_time(work([[1, 2, 1, 3, 4, 5, 6]], learner=7), 10)
        self.assertEqual(result["training_s"], 38)
        self.assertEqual(result["completed_rollouts"], 1)
        self.assertEqual(result["model_calls"], 2)

    def test_resource_queue_and_parallelism_change_makespan(self):
        jobs = [[0, 0, 3, 0, 0, 0, 0], [0, 0, 1, 0, 0, 0, 0]]
        serial = training_time(work(jobs, learner=5), 1)
        parallel = training_time(work(jobs, learner=5, tools=2), 1)
        self.assertEqual(serial["training_s"], 9)
        self.assertEqual(parallel["training_s"], 8)
        self.assertEqual(serial["peak_occupancy"]["tools"], 1)
        self.assertEqual(serial["summed_resource_wait_s"]["tools"], 3)

    def test_on_policy_update_barrier_prevents_future_work_overlap(self):
        result = training_time(work([[0, 0, 3, 0, 0, 0, 0]], learner=5, updates=2), 1)
        self.assertEqual(result["training_s"], 16)
        self.assertEqual(result["completed_updates"], 2)
        self.assertEqual(result["completed_rollouts"], 2)

    def test_inference_queue_is_not_treated_as_unlimited_parallelism(self):
        jobs = [[0, 1, 0, 0, 1, 0, 0]] * 2
        result = training_time(work(jobs, learner=5, inference=1), 0)
        self.assertEqual(result["training_s"], 9)
        self.assertEqual(result["resource_busy_s"]["inference"], 4)

    def test_inference_exclusion_reschedules_instead_of_subtracting_parallel_time(self):
        jobs = [[0, 10, 1, 0, 10, 0, 0]] * 2
        descriptor = work(jobs, learner=5)
        included = training_time(descriptor, 1)
        excluded = training_time(descriptor, 1, include_inference=False)
        self.assertEqual(excluded["training_s"], 7)
        self.assertLess(included["training_s"] - included["resource_busy_s"]["inference"], 0)
        self.assertEqual(excluded["resource_busy_s"]["inference"], 0)
        self.assertEqual(excluded["summed_resource_wait_s"]["inference"], 0)
        self.assertEqual(excluded["peak_occupancy"]["inference"], 0)
        self.assertEqual(excluded["learner_phase_s"], included["learner_phase_s"])
        self.assertEqual(excluded["completed_rollouts"], included["completed_rollouts"])
        self.assertEqual(excluded["model_calls"], included["model_calls"])

    def test_excluded_clock_retains_setup_cpu_verification_teardown_and_updates(self):
        result = training_time(work([[1, 20, 1, 3, 40, 5, 6]], learner=7),
                               10, include_inference=False)
        self.assertEqual(result["training_s"], 32)
        self.assertEqual(result["inference_accounting"], "excluded")

    def test_excluded_clock_is_independent_of_model_latency_and_capacity(self):
        config = json.loads((Path(__file__).resolve().parents[1] / "experiment.json").read_text())["simulation"]
        first, _ = make_work(config, config["profiles"]["tool_wait"], 1234)
        second, _ = make_work(config, config["profiles"]["model_wait"], 1234)
        first["resources"]["inference"] = 1
        second["resources"]["inference"] = 64
        a = training_time(first, 0.3, include_inference=False)
        b = training_time(second, 0.3, include_inference=False)
        self.assertEqual(a["training_s"], b["training_s"])
        self.assertEqual(a["resource_busy_s"], b["resource_busy_s"])
        self.assertEqual(a["summed_resource_wait_s"], b["summed_resource_wait_s"])
        self.assertEqual(a["completed_updates"], b["completed_updates"])

    def test_io_speedup_is_not_relabelled_as_training_speedup(self):
        descriptor = work([[0, 0, 1, 0, 0, 0, 0]], learner=100)
        baseline = training_time(descriptor, 10)
        candidate = training_time(descriptor, 5)
        self.assertAlmostEqual(1 - candidate["training_s"] / baseline["training_s"], 5 / 110)
        self.assertEqual(baseline["learner_phase_s"], candidate["learner_phase_s"])

    def test_same_seed_preserves_work_without_mutating_the_template(self):
        config = json.loads((Path(__file__).resolve().parents[1] / "experiment.json").read_text())["simulation"]
        first, fingerprint = make_work(config, config["profiles"]["tool_wait"], 1234)
        second, other_fingerprint = make_work(config, config["profiles"]["tool_wait"], 1234)
        self.assertEqual(fingerprint, other_fingerprint)
        original = copy.deepcopy(first)
        a = training_time(first, 1)
        b = training_time(second, 2)
        self.assertEqual(first, original)
        self.assertEqual(a["completed_rollouts"], 256)
        self.assertEqual(a["completed_rollouts"], b["completed_rollouts"])
        self.assertEqual(a["completed_updates"], b["completed_updates"])
        self.assertEqual(a["model_calls"], b["model_calls"])
        self.assertEqual(a["learner_phase_s"], b["learner_phase_s"])
        self.assertGreater(b["training_s"], a["training_s"])

    def test_invalid_times_and_resources_are_rejected(self):
        with self.assertRaises(ValueError):
            training_time(work([[0] * 7]), float("nan"))
        with self.assertRaises(ValueError):
            simulate_batch([[0] * 7], {"actors": 0, "tools": 1, "cpu": 1, "inference": 1})
        with self.assertRaises(ValueError):
            simulate_batch([[0, -1, 0, 0, 0, 0, 0]], work([])["resources"])

    def test_schedule_seeds_are_averaged_inside_measured_repeat_blocks(self):
        rows = []
        for repeat in range(3):
            for seed in (1, 2):
                for case, ratio in (("fast", 0.5), ("slow", 2)):
                    for arm, duration in (("baseline", 10), ("adaptive", 10 * ratio)):
                        rows.append({
                            "case": case, "repeat": repeat, "schedule_seed": seed, "arm": arm,
                            "execution_s": duration, "work_sha256": "matched",
                            "completed_rollouts": 2, "completed_updates": 1, "model_calls": 4,
                            "logical_bytes_per_job": 4096, "logical_checksum": "same",
                            "inference_accounting": "excluded",
                        })
        result = summarize(rows, "adaptive", "baseline")
        self.assertAlmostEqual(result["candidate_over_baseline"], 1)
        self.assertEqual(result["point_regressions"], 1)
        self.assertAlmostEqual(result["candidate_geometric_s"] / result["baseline_geometric_s"], 1)
        broken = copy.deepcopy(rows)
        broken[0]["completed_rollouts"] = 1
        with self.assertRaises(ValueError):
            summarize(broken, "adaptive", "baseline")
        with self.assertRaises(ValueError):
            summarize(rows[1:], "adaptive", "baseline")
        broken = copy.deepcopy(rows)
        broken[0]["inference_accounting"] = "included"
        with self.assertRaises(ValueError):
            summarize(broken, "adaptive", "baseline")

    def test_oracle_chooses_one_fixed_setting_per_condition(self):
        rows = [
            {"case": "test", "repeat": repeat, "schedule_seed": 1, "arm": arm,
             "execution_s": (1, 10, 1)[repeat] if arm == "fixed16" else 3}
            for repeat in range(3) for arm in FIXED
        ]
        self.assertEqual(offline_fixed_choices(rows), {"test": "fixed16"})
        with self.assertRaises(ValueError):
            offline_fixed_choices(rows[:-1])


class CalibrationTests(unittest.TestCase):
    def test_reanalysis_can_use_preserved_analysis_but_not_changed_reader_code(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            (root / "src").mkdir()
            archived = root / "provenance" / "calibration-v1"
            archived.mkdir(parents=True)
            (root / "simulate.py").write_bytes(b"new analysis")
            (archived / "simulate.py").write_bytes(b"original analysis")
            (root / "src" / "fetch_policy.cpp").write_bytes(b"original reader")
            hashes = {
                "simulate.py": hashlib.sha256(b"original analysis").hexdigest(),
                "src/fetch_policy.cpp": hashlib.sha256(b"original reader").hexdigest(),
            }
            with mock.patch("simulate.ROOT", root):
                verify_source_snapshot(hashes)
                (root / "src" / "fetch_policy.cpp").write_bytes(b"changed reader")
                with self.assertRaises(ValueError):
                    verify_source_snapshot(hashes)
                (root / "src" / "fetch_policy.cpp").write_bytes(b"original reader")
                (archived / "simulate.py").write_bytes(b"changed archive")
                with self.assertRaises(ValueError):
                    verify_source_snapshot(hashes)

    def test_fixed_choice_requires_balanced_successful_development_rows(self):
        rows = [{"case": "dev", "repeat": repeat, "arm": arm, "status": "ok",
                 "process_wall_s": index + 1}
                for repeat in range(3) for index, arm in enumerate(FIXED)]
        self.assertEqual(select_fixed(rows)["arm"], "fixed16")
        for invalid in (rows[:-1], rows + [rows[0]]):
            with self.assertRaises(ValueError):
                select_fixed(invalid)
        rows[0]["status"] = "timeout"
        with self.assertRaises(ValueError):
            select_fixed(rows)

    def test_frozen_plan_counts_and_non_overwriting_outputs(self):
        protocol = json.loads((Path(__file__).resolve().parents[1] / "experiment.json").read_text())
        for stage in protocol["stages"]:
            self.assertTrue(cases_for(protocol, stage))
        for label in ("..", "../outside", r"..\outside"):
            with self.assertRaises(ValueError):
                result_path(label)
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "record.json"
            write_new(path, {"original": True})
            with self.assertRaises(FileExistsError):
                write_new(path, {"replacement": True})
            self.assertEqual(json.loads(path.read_text()), {"original": True})


if __name__ == "__main__":
    unittest.main()
