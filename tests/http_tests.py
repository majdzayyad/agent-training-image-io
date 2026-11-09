#!/usr/bin/env python3
"""End-to-end HTTP protocol and CLI failure tests; no external service required."""

import json
import random
import subprocess
import sys
import tempfile
import threading
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "scripts"))
from range_server import ImageServer  # noqa: E402


def run(binary, source, trace, ok=True, *extra):
    result = subprocess.run(
        [binary, "--source", source, "--trace", str(trace), "--policy", "fixed16", *extra],
        capture_output=True,
        text=True,
        timeout=15,
    )
    if (result.returncode == 0) != ok:
        raise AssertionError(f"unexpected exit {result.returncode}: {result.stdout} {result.stderr}")
    return json.loads(result.stdout) if ok else result.stderr


class BrokenHandler(BaseHTTPRequestHandler):
    mode = ""
    protocol_version = "HTTP/1.1"

    def log_message(self, *_):
        pass

    def do_HEAD(self):
        self.send_response(200)
        self.send_header("Content-Length", "65536")
        self.send_header("ETag", 'W/"source"' if self.mode == "weak" else '"source"')
        self.end_headers()

    def do_GET(self):
        if self.mode == "changed":
            self.send_response(412)
            self.send_header("Content-Length", "0")
            self.end_headers()
            return
        self.send_response(200 if self.mode == "ignored" else 206)
        self.send_header("ETag", '"other"' if self.mode == "wrong-etag" else '"source"')
        self.send_header(
            "Content-Range", "bytes 1-16384/65536" if self.mode == "wrong-range" else "bytes 0-16383/65536"
        )
        length = 4 if self.mode == "short" else 16384
        self.send_header("Content-Length", str(length))
        self.end_headers()
        try:
            self.wfile.write(b"x" * length)
        except (BrokenPipeError, ConnectionResetError):
            pass


def with_server(server, action):
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    try:
        return action(f"http://127.0.0.1:{server.server_port}/image.bin")
    finally:
        server.shutdown()
        thread.join()
        server.server_close()


def main():
    binary = sys.argv[1]
    with tempfile.TemporaryDirectory(prefix="aio-http-") as directory:
        root = Path(directory)
        image = root / "image.bin"
        image.write_bytes(random.Random(5678).randbytes(65536 + 13))
        trace = root / "trace.csv"
        trace.write_text("stream_id,offset,length,delay_us\n0,1,4096,0\n0,65532,17,0\n")
        local = run(binary, str(image), trace, True, "--policy", "local")
        remote = with_server(ImageServer(("127.0.0.1", 0), image), lambda url: run(binary, url, trace))
        assert local["checksum"] == remote["checksum"], "HTTP byte mismatch"
        assert remote["remote_bytes"] == 2 * 16384 + 13, remote

        def mutate(url):
            original = image.read_bytes()
            image.write_bytes(b"changed")
            run(binary, url, trace, False)
            image.write_bytes(original)

        with_server(ImageServer(("127.0.0.1", 0), image), mutate)
        trace.write_text("stream_id,offset,length,delay_us\n0,0,4096,0\n")
        for mode in ("ignored", "wrong-range", "short", "changed", "wrong-etag", "weak"):
            handler = type("FaultHandler", (BrokenHandler,), {"mode": mode})
            with_server(
                ThreadingHTTPServer(("127.0.0.1", 0), handler),
                lambda url: run(binary, url, trace, False),
            )
        for row in ("0,-1,4096,0", "0,18446744073709551616,1,0", "0,0,0,0", "0,0,4,0,"):
            trace.write_text("stream_id,offset,length,delay_us\n" + row + "\n")
            run(binary, str(image), trace, False)
        trace.write_text(
            "stream_id,offset,length,delay_us\n"
            "0,1,16384,0\n1,32769,16384,0\n0,16385,8192,0\n1,65532,17,0\n"
        )
        expected = run(binary, str(image), trace, True, "--policy", "local")["checksum"]
        for rule in ("cost", "grow-reset", "fixed-cost", "locality-only", "alignment-agnostic"):
            for identity, frontend in (
                ("logical", "concurrent"), ("collapsed", "concurrent"),
                ("logical", "serialized"), ("collapsed", "serialized"),
            ):
                result = run(
                    binary, str(image), trace, True, "--policy", "adaptive-v1",
                    "--refill-rule", rule, "--queue-feedback", "observable-only",
                    "--stream-identity", identity, "--frontend", frontend,
                )
                assert result["checksum"] == expected
                assert result["prefetch_bytes"] == result["failed_fetches"] == 0
                assert result["stream_identity"] == identity and result["frontend"] == frontend
        for flag, value in (
            ("--refill-rule", "unknown"), ("--queue-feedback", "unknown"),
            ("--stream-identity", "unknown"), ("--frontend", "unknown"),
            ("--refill-rule", "grow-reset"),
        ):
            run(binary, str(image), trace, False, flag, value)
    print("HTTP range, immutability, truncation, and trace validation tests passed")


if __name__ == "__main__":
    main()
