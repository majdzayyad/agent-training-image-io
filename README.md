# Adaptive Image Reads for Agent Training

**Majd Zayyad**

Does a smarter image reader finish the same training-execution work sooner, excluding model inference?

This repository measures a C++ image reader, then feeds complete read-job durations into a fixed-work scheduler. The current endpoint counts setup, image reads, tool computation, non-inference resource queues, verification, teardown and learner updates. Model inference is instantaneous and does not use a model-worker queue. It compares adaptive refill sizing with fixed sizes and a simple grow/reset rule.

These are **simulated training-execution times excluding inference**, not end-to-end wall-clock training speedups, actual RL runs or time-to-quality measurements. The schedule is recomputed without inference; overlapping model-call times are not subtracted from elapsed time.

DSec motivates the problem: its Section 5.3 reuses 3FS's large-I/O throughput for demand-loaded sandbox files, while reporting a small-random-I/O penalty. Workload-aware I/O granularity bridges that mismatch: large requests amortize overhead; small requests reduce read amplification for sparse or bursty access. The paper connects this to the Lambda image-loading study and adaptive read-ahead research.

## Reproduce the reported experiment

Use Python 3.12 on Linux or WSL. The reference-data calculation uses only the standard library:

```bash
git clone --branch v1.1.0 https://github.com/majdzayyad/agent-training-image-io.git
cd agent-training-image-io
python3 reproduce.py --output reproduced-001
```

This validates the included calibration records, reruns the complete scheduling matrix, and regenerates the paper's tables and source under `results/reproduced-001`. Choose a new label for another run; existing output is never replaced.

The paper is [`paper/agent-training.pdf`](paper/agent-training.pdf). To rebuild the PDF, install LaTeX with `pdflatex`, `bibtex`, the standard recommended fonts and `microtype`, then run:

```bash
python3 reproduce.py --output paper-reproduced-001 --pdf
```

On Ubuntu 24.04, suitable packages are `texlive-latex-extra` and `texlive-fonts-recommended`. The current paper and inference-excluded analysis are release **v1.1.0**.

The inference-included study remains unchanged in **v1.0.0** and `data/reference/simulation/`. The new results are in `data/reference/non-inference-v1/`. They reuse the same physical reader measurements; no reader retuning or new measurement is implied by changing the endpoint.

## Recollect the physical reader measurements

Use Linux (Ubuntu 24.04 is the measured environment), Python 3.12, a C++20 compiler, CMake, Ninja, and libcurl development headers. On Ubuntu, the relevant packages are `g++`, `cmake`, `ninja-build`, `libcurl4-openssl-dev`, and `python3`.

Stop other project benchmarks/builds before timing:

```bash
python3 reproduce.py --output fresh-001 --fresh --quiescence-ack --pdf
```

This builds the reader, runs its correctness checks, generates the same **512 MiB** synthetic image, freezes one executable per calibration stage, and runs all arms sequentially. It collects the development matrix, freezes the static choice, collects the evaluation and stream-identity matrices, and reruns the training simulation using those new measurements.

No private image, credentials, downloaded task corpus or unpublished benchmark binary is needed. The image is generated from its declared seed and checked against its SHA-256. Fresh physical timings will vary with the machine; reference-data simulation is deterministic for the pinned code and inputs.

Each run keeps its protocol, source/binary/input hashes, run order, complete raw rows and status. Failures and timeouts are retained and block simulation rather than becoming successful speedups. A heartbeat and per-trial timeout expose stalls. The preflight is not a guarantee of dedicated physical hardware.

## What is compared

- Fixed 16, 64, 256 and 1024 KiB refills, adaptive refill selection, and grow/reset use the **same reader executable and resource limits**.
- The deployable fixed choice is selected from development data only. The separate offline oracle is chosen on evaluation data and is explicitly nondeployable.
- The controller sees local queue wait and observed request duration, not the synthetic backend's exact remote-queue decomposition.
- Logical/collapsed stream IDs and concurrent/serialized frontends are matched across every arm in the identity experiment.

The public matrix includes unfavorable conditions. In particular, stream collapse can turn sequential work into apparently discontinuous access. The implementation is not retuned to remove that diagnostic result.

## Files and assumptions

| File | Purpose |
|---|---|
| `experiment.json` | Unchanged calibration protocol and shared work/resource definitions |
| `analysis.json` | Current endpoint, inference accounting and selected compute profiles |
| `calibrate.py` | Non-overwriting, sequential measurement harness |
| `simulate.py` | Event-driven scheduler with explicit inference inclusion/exclusion |
| `reproduce.py` | Reference-data and fresh-measurement entry point |
| `src/`, `include/`, `benchmarks/` | Complete reader and replay implementation |
| `data/reference/` | Calibration records, traces, selection and full simulated outcomes |
| `paper/` | Concise paper, source and data-driven renderer |
| `docs/method.md` | Exact interpretation, resources, statistics and limitations |
| `provenance/calibration-v1/` | Original analysis/test files captured by the calibration hashes |

Every simulated job completes 256 rollouts and eight synchronous learner updates. Non-inference service variation and work are paired across policies. The long-model-call profile is omitted from the new analysis because it becomes identical to the short-compute profile once inference and its queue are removed. All read conditions, controls and identity cases remain included.

Each tool worker has an independent measured reader/backend budget; the model does not manufacture several full-bandwidth workers on one shared bandwidth limit. It does not model actual policy learning, cross-host backend interference, warm-cache reuse, or the distribution of real coding tasks.

Use the results to understand **when image reads change inference-excluded execution**, not to claim the same percentage improvement in total training wall time.

## Checks

```bash
cmake -S . -B build -G Ninja -DCMAKE_BUILD_TYPE=Release
cmake --build build -j4
ctest --test-dir build --output-on-failure
```

The tests cover data correctness, HTTP contracts, bounds and concurrency, as well as actor/resource queues, update barriers, work preservation, paired statistics and non-overwriting output. Dependencies retain their respective licenses; no third-party implementation is vendored into this project.
