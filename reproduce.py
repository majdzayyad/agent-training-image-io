#!/usr/bin/env python3
"""Reproduce the published simulation, or recollect its reader calibration first."""

import argparse
import json
import shutil
import subprocess
import sys
from pathlib import Path

from calibrate import ROOT, check_quiescence, digest, result_path, write_new
from scripts.generate import image


def export_calibration(destination, sources):
    destination.mkdir(parents=True, exist_ok=False)
    for stage, source in sources.items():
        target = destination / stage
        target.mkdir()
        for name in ("manifest.json", "complete.json", "runs.jsonl", "runs.csv", "selection.json"):
            path = source / name
            if path.exists():
                shutil.copy2(path, target / name)
        shutil.copytree(source / "traces", target / "traces")


def execute(command, cwd=ROOT):
    subprocess.run(command, cwd=cwd, check=True)


def reproduce(args):
    output = result_path(args.output)
    if output.exists():
        raise FileExistsError("choose a new output label; existing results are never replaced")
    if args.fresh and not args.quiescence_ack:
        raise ValueError("--fresh requires --quiescence-ack after checking for other timed work")
    if args.fresh:
        check_quiescence()
    if args.pdf and not (ROOT / "paper" / "render.py").is_file():
        raise FileNotFoundError("the paper renderer is required for --pdf")
    output.mkdir(parents=True)
    protocol = json.loads((ROOT / "experiment.json").read_text())
    calibration = ROOT / "data" / "reference"
    if args.fresh:
        execute(["cmake", "-S", ".", "-B", "build", "-G", "Ninja", "-DCMAKE_BUILD_TYPE=Release"])
        execute(["cmake", "--build", "build", "-j4"])
        execute(["ctest", "--test-dir", "build", "--output-on-failure"])
        input_path = ROOT / "data" / "image.bin"
        if not input_path.exists():
            image(input_path, protocol["image"]["bytes"], protocol["image"]["seed"])
        if digest(input_path) != protocol["image"]["sha256"]:
            raise ValueError("existing image differs from the declared deterministic input")
        labels = {stage: f"{args.output}-{stage}" for stage in protocol["stages"]}
        for stage, label in labels.items():
            command = [
                sys.executable, "calibrate.py", "--stage", stage, "--output", label,
                "--quiescence-ack",
            ]
            if stage == "evaluation":
                command.extend(["--development", labels["development"]])
            execute(command)
        calibration = output / "calibration"
        export_calibration(calibration, {stage: result_path(label)
                                         for stage, label in labels.items()})
    simulation = output / "simulation"
    execute([sys.executable, "simulate.py", "--calibration", str(calibration),
             "--output", str(simulation)])
    renderer = ROOT / "paper" / "render.py"
    if renderer.exists():
        command = [
            sys.executable, str(renderer), "--results", str(simulation),
            "--calibration", str(calibration), "--output", str(output / "paper"),
        ]
        if args.pdf:
            command.append("--pdf")
        execute(command)
    write_new(output / "complete.json", {
        "kind": "fresh calibration and simulation" if args.fresh else "reference-data reproduction",
        "protocol_sha256": digest(ROOT / "experiment.json"),
        "analysis_sha256": digest(ROOT / "analysis.json"),
        "simulation_summary_sha256": digest(simulation / "summary.json"),
        "paper_rendered": renderer.exists(), "pdf_requested": args.pdf,
    })
    print(f"Completed without overwriting prior results: {output}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output", required=True, help="new directory label under results")
    parser.add_argument("--fresh", action="store_true", help="rebuild and recollect physical calibration")
    parser.add_argument("--quiescence-ack", action="store_true")
    parser.add_argument("--pdf", action="store_true", help="also build the PDF using LaTeX")
    reproduce(parser.parse_args())
