# Experiment details

The main paper asks whether adaptive image fetching reduces the completion time of equal training-execution work **excluding model inference**. `analysis.json` defines the current endpoint separately from the unchanged reader-calibration protocol in `experiment.json`.

## Why DSec motivates this adaptation

DSec v1, Section 5.3 (printed pages 16-17), describes reusing the production training filesystem, 3FS, for on-demand sandbox images. It reports high throughput for large sequential transfers and weak small-random-I/O performance in that setting. DSec responds with local writes, local metadata where the image format permits, and bulk demand reads; its OverlayBD/ublk path fetches 256 KiB chunks into a local L2 cache.

The relevant pattern is workload-aware I/O granularity: amortize per-request cost without excessive read amplification. The primary AWS Lambda paper's Section 2 chunking discussion says that smaller chunks can improve highly random loading, whereas larger chunks reduce metadata/request counts and provide sequential read-ahead. AMP's Sections II.B-II.C describe adaptive read-ahead degree; its Section III analysis relates transfer size to fixed-plus-size-dependent service cost.

This is a performance mismatch, not a claim that 3FS only supports sequential access. Its own README explicitly describes random-access dataloaders. Our independent HTTP reader tests a client-side sizing policy; it does not demonstrate a changed production 3FS deployment.

Primary sources:

- DSec: https://arxiv.org/html/2609.22978v1#S5.SS3
- Lambda: https://www.usenix.org/system/files/atc23-brooker.pdf
- AMP: https://www.usenix.org/legacy/events/fast07/tech/full_papers/gill/gill.pdf
- 3FS capabilities: https://github.com/deepseek-ai/3FS#diverse-workloads

## Reader calibration

`experiment.json` is the authoritative configuration. A deterministically generated 512 MiB image is served over loopback HTTP. The server file is deliberately warmed by hashing; each timed reader process begins with an empty 64 MiB cache. Four I/O workers share an 8 MiB in-flight payload limit. Metadata and process memory are reported separately from payload limits.

The main matrix crosses random/sequential/mixed reads, 1/8/32 logical streams, and three backend profiles: unshaped loopback; 2000 us plus 512 MiB/s; and 500 us plus 128 MiB/s. The latter two are synthetic delay/shared-byte-budget wrappers, not measured 3FS or physical-network behavior. A stream is a logical input to one shared reader/cache, not an additional sandbox cache.

All six arms use one build. The adaptive arm is `adaptive-v1`; all arms disable asynchronous prefetch. Extending a blocking refill still reads future bytes synchronously. The `observable-only` option leaves physical backend delays untouched but withholds `backend_queue` from the estimator. Reader-owned submission-to-worker wait remains observable; hidden remote wait stays in measured service duration.

The primary calibration clock is total child-process wall time, including setup, metadata, reads, remaining work, output and exit. It is the service time of a **synthetic read job**, not a storage-only duration. Data checksums must match a local reference and every policy. Allocation failures, fetch errors, unexpected async activity and payload-budget violations invalidate a run.

Development has 108 measurements and chooses among the four fixed sizes by equal-case/equal-repeat geometric process time. That choice is frozen before the 486-row evaluation. The 144-row identity matrix crosses sequential/mixed traffic with logical/collapsed IDs and concurrent/serialized frontends for all six arms. There are three physical repeats per condition. All cases remain in the record.

## Adaptive rule

For each nonempty demand, the reader updates a bounded per-stream continuation estimate and a smoothed request length. It scores nominal, image-clipped aligned windows using estimated request/queue cost plus transfer cost, divided by predicted useful bytes. Actual transfers can be shorter due to cache hits, shared flights, image end and available in-flight space.

| State or parameter | Value and update |
|---|---|
| Stream records | At most 1024, least-recently-observed eviction |
| Initial continuation and length | `p=0`, `u=4096` bytes; first demand replaces `u` |
| Adjacent demand | `p = 0.75*p + 0.25` |
| Discontinuity | `p = 0.5*p` |
| Request-length smoothing | `u = 0.75*u + 0.25*length`, scoring floor 4096 bytes |
| Candidate windows | 16/64/256/1024 KiB; cap by cache, flight budget and 1 MiB |
| Initial costs | 200 us overhead, 256 MiB/s bandwidth, zero queue estimate |
| Service fit | Weighted linear regression in KiB/us, decay 0.95 |
| Identifiability | Weighted size variance greater than 64; positive slope, nonnegative intercept |
| Clamps | Overhead 1 us to 1 s; bandwidth 1 to 32768 MiB/s |
| Queue smoothing | `Q = 0.8*Q + 0.2*observed_wait` |
| Ties | Ascending menu, replace only on a strictly lower score |

The useful-byte model counts the current demand plus a geometric continuation sequence. For `p > 1 - 1e-8`, it uses the all-forward-bytes limit. The request scorer observes hits too, and scores nominal windows before residual cache/flight coverage is known. Invalid samples or unidentifiable fits retain prior/previous service parameters. The implementation is in `src/fetch_policy.cpp` and `src/image_reader.cpp`.

Grow/reset is a simpler AS-inspired comparator, not a faithful AMP implementation. It starts at 16 KiB, doubles the next window after a logical demand submits at least one new fetch, retains its size on hits/shared flights, and resets on discontinuity. Its intermediate 32/128/512 KiB windows and 1 MiB cap are explicit; no policy is represented as an authentic whole-system prefetch implementation.

## Training scheduler

Each of eight updates receives 32 rollouts. At most eight actors are active. A rollout holds its actor slot through:

1. Setup.
2. A model call.
3. One measured image-read job.
4. Additional tool CPU computation.
5. A second model call.
6. Verification and teardown.

In v1.0.0, model calls, CPU work and read jobs each used four service slots with FIFO queues. In the current analysis, model-call placeholders are instantaneous and bypass the inference pool completely. CPU and read-job queues retain their capacities. A completed rollout frees its actor slot. After all 32 rollouts finish, the learner performs its update; no rollout for the next update starts before that update completes.

This is a counterfactual, synchronous execution benchmark. It is not the old elapsed time minus summed inference service or wait time: those quantities overlap across workers, and removing inference changes queue arrivals. The scheduler recomputes all remaining waits and overlaps. Tests require the excluded endpoint to be independent of model-call duration and inference-worker capacity, with zero inference busy time and queue wait.

The measured whole-process duration replaces only the read-job stage. The shared work specification retains the following service inputs; `analysis.json` disables inference and selects the three non-redundant profiles:

| Shared profile | Nominal model call (ignored now) | Tool CPU work | Learner update |
|---|---:|---:|---:|
| Short non-I/O stages | 0.05 s | 0.02 s | 0.2 s |
| Long model calls | 5 s | 0.02 s | 0.2 s |
| Long tool computation | 0.05 s | 5 s | 0.2 s |
| Long learner updates | 0.05 s | 0.02 s | 20 s |

Setup, verification and teardown are respectively 0.02, 0.01 and 0.01 s. The declared scheduling seeds generate paired uniform service multipliers in [0.75, 1.25] and paired job order. These multipliers introduce modeled service variation; they are not extra observed tasks. The same multiplier is used for corresponding policy jobs, including the read stage.

Each tool slot represents an independently provisioned reader/backend budget. Shared-backend contention between slots is not inferred by multiplying one bandwidth budget. Each calibrated job has a fresh reader, so warm-cache reuse is not modeled. A job may contain multiple logical read streams internally; those streams are not the scheduler's actor slots.

The endpoint is simulated completion after the final learner update with inference service and inference queueing removed. Setup, read-job time, tool CPU work, verification, teardown, learner updates and all other queueing remain. All policies must match rollout/update/model-call-placeholder counts, logical read data, inference-accounting mode and the work fingerprint. Summed resource waits or busy times are not subtracted from wall time.

New raw rows use `execution_s` and explicitly record `inference_accounting`. The library's `training_time` helper retains its inference-included default for compatibility and testing; the command-line experiment passes the accounting mode from `analysis.json` explicitly.

## Analysis

For each condition, selected compute setting, measured repeat and schedule seed, compare paired inference-excluded completion times. Average log ratios across scheduling seeds **inside** a measured-repeat block. Aggregate conditions equally within that block, then form a descriptive Student-t interval across the three physical repeats. Scheduling seeds do not increase the count of physical measurements.

The main table uses geometric times, so its ratio is consistent with the reported geometric reduction. `summary.json` also includes every condition, point regressions, ranges and intervals. These quantify a controlled simulation conditioned on a small calibration dataset, not uncertainty over production training deployments.

The development-selected static baseline is deployable without seeing evaluation outcomes. The offline oracle instead selects the best tested fixed setting separately for each evaluation condition and training profile, across all repeats/seeds, then keeps that choice for every pair in the condition. It is nondeployable; its descriptive interval does not account for evaluation-data selection.

## Interpretation and prior work

The design combines aligned request geometry, inferred continuation and observed cost at a blocking image-read boundary. AMP establishes adaptive synchronous and asynchronous read-ahead. WaRP plans known required ranges using calibrated service costs. Khameleon schedules probability-weighted query utility, AdaCache covers known missing intervals with aligned blocks, and TIP uses application hints and marginal buffer value. These are the relevant design context, not systems faithfully reimplemented in this benchmark.

The closest-method review did not establish exact algorithm identity or a firstness claim. The NAS 2021 paper *On Adapting the Cache Block Size in SSD Caches* remains an unresolved full-text comparison (DOI `10.1109/NAS51552.2021.9605462`).

This experiment does not run an actual LLM, learner, task verifier, production DSec service or 3FS deployment. Synthetic read families are not a representative repository/task/image distribution. A practical time-to-training-quality claim needs captured real tool work, a representative storage path and matched actual learning runs.

Source/input/binary hashes and exclusive-create output protect the retained experiment from accidental changes. They establish local consistency, not externally timestamped preregistration.

The physical records are reused without edits. Their manifests also captured `simulate.py` and its test file, although neither affected the reader's measured duration. Their original v1.0.0 bytes are preserved in `provenance/calibration-v1/`; only those two analysis/test files may be validated against that archive. Every reader, replay, input-generator, calibration-harness and build file must still match the active source. Fresh calibration validates the current files normally. Earlier inference-included results remain in their original data directory and release.
