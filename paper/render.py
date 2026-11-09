#!/usr/bin/env python3
"""Generate the concise paper directly from the complete training simulation."""

import argparse
import json
import shutil
import subprocess
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
from calibrate import digest, write_new
from simulate import load_calibration


def change(percent):
    return f'{abs(percent):.1f}\\% {"lower" if percent >= 0 else "higher"}'


def render(results, calibration, output, pdf=False):
    protocol = json.loads((ROOT / "experiment.json").read_text())
    load_calibration(calibration, protocol)
    summary = json.loads((results / "summary.json").read_text())
    complete = json.loads((results / "complete.json").read_text())
    if (digest(results / "runs.jsonl") != summary["raw_sha256"] or
            summary["raw_sha256"] != complete["runs_sha256"] or
            complete["runs"] != complete["expected_runs"] or
            summary["protocol_sha256"] != digest(ROOT / "experiment.json") or
            summary["simulator_sha256"] != digest(ROOT / "simulate.py")):
        raise ValueError("paper inputs do not match the completed experiment")
    output.mkdir(parents=True, exist_ok=False)
    generated = output / "generated"
    generated.mkdir()
    for name in ("paper.tex", "background.bib", "refill.bib"):
        shutil.copy2(ROOT / "paper" / name, output / name)
    values = summary["results"]["evaluation"]
    tool = values["tool_wait"]["fixed256"]
    model = values["model_wait"]["fixed256"]
    (generated / "abstract-result.tex").write_text(
        f'Across the calibrated read conditions, simulated training time is '
        f'{change(tool["reduction_pct"])} when the other stages are short, and '
        f'{change(model["reduction_pct"])} with long model calls, relative to fixed256. '
        'The effect depends on where the job waits; losing stream identity can reverse it.\n'
    )
    table = [
        "\\begin{table*}[t]\\centering\\small\n"
        "\\begin{tabular}{lrrr}\\toprule\n"
        "Training setting & Fixed256 (s) & Adaptive (s) & Time saved\\\\\\midrule\n"
    ]
    for profile, configuration in protocol["simulation"]["profiles"].items():
        value = values[profile]["fixed256"]
        label = "Short compute stages" if profile == "tool_wait" else configuration["label"]
        table.append(
            f'{label} & {value["baseline_geometric_s"]:.1f} & '
            f'{value["candidate_geometric_s"]:.1f} & {value["reduction_pct"]:.1f}\\%\\\\\n'
        )
    table.append(
        "\\bottomrule\\end{tabular}\n"
        "\\caption{Total simulated time through the final learner update: geometric "
        "means across 27 read conditions with identical work in every arm. "
        "These are model results, not measured GPU training runs.}"
        "\\label{tab:training}\\end{table*}\n"
    )
    (generated / "results-table.tex").write_text("".join(table))
    tuned = values["tool_wait"]["selected_fixed"]
    grow = values["tool_wait"]["grow_reset"]
    identity = summary["results"]["identity"]["tool_wait"]["fixed256"]["conditions"][
        "sequential-c8-contended-collapsed-serialized"
    ]
    text = (
        f'Table~\\ref{{tab:training}} reports the training endpoint rather than the '
        f'isolated reader endpoint. With short compute stages, adaptive fetching gives '
        f'{change(tool["reduction_pct"])} total time against fixed256. '
        f'The development traces selected \\texttt{{{summary["selected_fixed"]}}}; '
        f'against that static setting, training time is {change(tuned["reduction_pct"])}. '
        f'Against grow/reset it is {change(grow["reduction_pct"])} in the same setting.\n\n'
        'Stream handling provides a more serious counterexample. In the sequential '
        'condition that collapses stream IDs behind a serialized frontend, simulated '
        f'training takes {identity["candidate_over_baseline"]:.2f}$\\times$ fixed256 time '
        'when other stages are short. Interleaved reads no longer look sequential to '
        'the policy; the read-job records show a shift toward small refills and more requests. '
        'The frontend setting is identical in both arms. This is a simulated failure '
        'case, not an observed slowdown of DSec.\n'
    )
    (generated / "results-text.tex").write_text(text)
    write_new(output / "paper-inputs.json", {
        "protocol_sha256": summary["protocol_sha256"],
        "simulation_rows_sha256": summary["raw_sha256"],
        "summary_sha256": digest(results / "summary.json"),
        "repository": "https://github.com/majdzayyad/agent-training-image-io",
        "release": "v1.0.0", "author": "Majd Zayyad",
    })
    if pdf:
        commands = [
            ["pdflatex", "-interaction=nonstopmode", "-halt-on-error", "-jobname=agent-training", "paper.tex"],
            ["bibtex", "agent-training"],
            ["pdflatex", "-interaction=nonstopmode", "-halt-on-error", "-jobname=agent-training", "paper.tex"],
            ["pdflatex", "-interaction=nonstopmode", "-halt-on-error", "-jobname=agent-training", "paper.tex"],
        ]
        with (output / "build.log").open("x") as log:
            for command in commands:
                subprocess.run(command, cwd=output, stdout=log, stderr=subprocess.STDOUT, check=True)
        if shutil.which("pdftotext"):
            subprocess.run(["pdftotext", "-layout", "agent-training.pdf", "agent-training.txt"],
                           cwd=output, check=True)
    print(f"Paper {'PDF and source' if pdf else 'source'}: {output}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--results", type=Path, required=True)
    parser.add_argument("--calibration", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--pdf", action="store_true")
    args = parser.parse_args()
    render(args.results.resolve(), args.calibration.resolve(), args.output.resolve(), args.pdf)
