# Adaptive Image Reads for Agent Training

**Majd Zayyad**

Does a smarter image reader finish a training job sooner, or just improve an I/O benchmark?

This repository measures a C++ image reader, then feeds complete read-job durations into a fixed-work training scheduler. The scheduler includes model calls, tool computation, resource queues, verification, teardown and learner updates. It compares adaptive refill sizing with fixed sizes and a simple grow/reset rule.

These are **simulated training-completion times**, not actual RL runs or time-to-quality measurements. No trained model, production DSec service or 3FS deployment is required.

## Reproduce the reported experiment

Use Python 3.12. The reference-data calculation uses only the standard library:

```bash
git clone --branch v1.0.0 https://github.com/majdzayyad/agent-training-image-io.git
cd agent-training-image-io
python3 reproduce.py --output reproduced-001
```

This validates the included calibration records, reruns the complete scheduling matrix, and regenerates the paper's tables and source under `results/reproduced-001`. Choose a new label for another run; existing output is never replaced.

The paper is [`paper/agent-training.pdf`](paper/agent-training.pdf). To rebuild the PDF, install LaTeX with `pdflatex`, `bibtex`, the standard recommended fonts and `microtype`, then run:

```bash
python3 reproduce.py --output paper-reproduced-001 --pdf
```

On Ubuntu 24.04, suitable packages are `texlive-latex-extra` and `texlive-fonts-recommended`. A frozen public version is available as release **v1.0.0**.

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
| `experiment.json` | Workloads, resources, seeds, policy arms and training cost settings |
| `calibrate.py` | Non-overwriting, sequential measurement harness |
| `simulate.py` | Event-driven actor/model/tool/CPU scheduler and matched analysis |
| `reproduce.py` | Reference-data and fresh-measurement entry point |
| `src/`, `include/`, `benchmarks/` | Complete reader and replay implementation |
| `data/reference/` | Calibration records, traces, selection and full simulated outcomes |
| `paper/` | Concise paper, source and data-driven renderer |
| `docs/method.md` | Exact interpretation, resources, statistics and limitations |

Every simulated job completes 256 rollouts and eight synchronous learner updates. Service variation and non-I/O work are paired across policies. Each tool worker has an independent measured reader/backend budget; the model does not manufacture several full-bandwidth workers on one shared bandwidth limit. It does not model actual policy learning, cross-host backend interference, warm-cache reuse, or the distribution of real coding tasks.

Use the results to understand **when image reads can change training completion**, not to claim a measured improvement in an existing training cluster.

## Checks

```bash
cmake -S . -B build -G Ninja -DCMAKE_BUILD_TYPE=Release
cmake --build build -j4
ctest --test-dir build --output-on-failure
```

The tests cover data correctness, HTTP contracts, bounds and concurrency, as well as actor/resource queues, update barriers, work preservation, paired statistics and non-overwriting output. Dependencies retain their respective licenses; no third-party implementation is vendored into this project.
