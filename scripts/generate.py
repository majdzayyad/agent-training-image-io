#!/usr/bin/env python3
"""Generate immutable, non-sparse image bytes and repeatable logical read streams."""

import argparse
import csv
import hashlib
import json
import random
from pathlib import Path

BLOCK = 4096
MIB = 1024 * 1024


def image(path: Path, size: int, seed: int) -> None:
    rng = random.Random(seed)
    digest = hashlib.sha256()
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("xb") as output:
        remaining = size
        while remaining:
            data = rng.randbytes(min(MIB, remaining))
            output.write(data)
            digest.update(data)
            remaining -= len(data)
    path.with_suffix(path.suffix + ".json").write_text(
        json.dumps({"bytes": size, "seed": seed, "sha256": digest.hexdigest()}, indent=2) + "\n"
    )


def trace(path: Path, size: int, clients: int, requests: int, pattern: str, seed: int) -> None:
    if clients < 1 or requests < 1 or size < 4 * BLOCK:
        raise ValueError("positive client/request counts and at least 16 KiB are required")
    rng = random.Random(seed)
    blocks = size // BLOCK
    streams = []
    for stream in range(clients):
        cursor = stream * blocks // clients
        rows = []
        hot = rng.randrange(blocks - 4)
        burst_left = 0
        burst_index = 0
        for index in range(requests):
            sequential = pattern == "sequential" or (pattern == "mixed" and index % 32 < 16)
            length_blocks = 1 if sequential else rng.choice([1, 2, 4])
            if pattern == "bursty":
                if burst_left == 0:
                    cursor = rng.randrange(blocks - 24)
                    burst_left = (3, 8, 24, 5)[burst_index % 4]
                    burst_index += 1
                offset, length_blocks = cursor, 1
                cursor += 1
                burst_left -= 1
            elif pattern == "crossing":
                offset = 4 * rng.randrange(1, (blocks - 4) // 4) - 1
                length_blocks = 4
            elif pattern == "churn":
                length_blocks = 16
                offset = (stream * blocks // clients + (index % 512) * 16) % (blocks - 15)
            elif sequential:
                offset = cursor % (blocks - length_blocks + 1)
                cursor = offset + length_blocks
            elif pattern == "mixed" and index % 8 == 7:
                offset = hot
            else:
                offset = rng.randrange(blocks - length_blocks + 1)
            delay = 0
            if pattern == "mixed":
                delay = 1000 if index % 64 == 63 else (50 if index % 16 == 15 else 0)
            rows.append((stream, offset * BLOCK, length_blocks * BLOCK, delay))
        streams.append(rows)
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("x", newline="") as output:
        writer = csv.writer(output, lineterminator="\n")
        writer.writerow(["stream_id", "offset", "length", "delay_us"])
        for index in range(requests):
            for rows in streams:
                writer.writerow(rows[index])


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    sub = parser.add_subparsers(dest="command", required=True)
    make_image = sub.add_parser("image")
    make_image.add_argument("path", type=Path)
    make_image.add_argument("--mib", type=int, default=512)
    make_image.add_argument("--seed", type=int, default=20261109)
    make_trace = sub.add_parser("trace")
    make_trace.add_argument("path", type=Path)
    make_trace.add_argument("--image-mib", type=int, default=512)
    make_trace.add_argument("--clients", type=int, default=16)
    make_trace.add_argument("--requests", type=int, default=128)
    make_trace.add_argument(
        "--pattern", choices=["random", "sequential", "mixed", "bursty", "crossing", "churn"],
        default="mixed",
    )
    make_trace.add_argument("--seed", type=int, default=271828)
    args = parser.parse_args()
    if args.command == "image":
        if args.mib <= 0:
            parser.error("--mib must be positive")
        image(args.path, args.mib * MIB, args.seed)
    else:
        trace(args.path, args.image_mib * MIB, args.clients, args.requests, args.pattern, args.seed)


if __name__ == "__main__":
    main()
