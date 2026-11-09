#!/usr/bin/env python3
"""Loopback-only immutable single-image HTTP/1.1 range server for the lab."""

import argparse
import hashlib
import os
import re
import socket
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from urllib.parse import urlsplit


class ImageServer(ThreadingHTTPServer):
    daemon_threads = True
    block_on_close = True

    def __init__(self, address, image: Path):
        self.fd = os.open(image, os.O_RDONLY)
        try:
            self.original = os.fstat(self.fd)
            digest = hashlib.sha256()
            offset = 0
            while data := os.pread(self.fd, 1024 * 1024, offset):
                digest.update(data)
                offset += len(data)
            self.etag = '"' + digest.hexdigest() + '"'
            super().__init__(address, RangeHandler)
        except BaseException:
            os.close(self.fd)
            raise

    def server_close(self):
        super().server_close()
        os.close(self.fd)


class RangeHandler(BaseHTTPRequestHandler):
    protocol_version = "HTTP/1.1"

    def setup(self):
        super().setup()
        self.connection.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)

    def log_message(self, _format, *_args):
        pass

    def reply(self, status, length=0, content_range=None):
        self.send_response(status)
        self.send_header("Content-Length", str(length))
        self.send_header("ETag", self.server.etag)
        self.send_header("Accept-Ranges", "bytes")
        self.send_header("Content-Type", "application/octet-stream")
        if content_range is not None:
            self.send_header("Content-Range", content_range)
        self.end_headers()

    def serve(self, head):
        if urlsplit(self.path).path != "/image.bin":
            self.reply(404)
            return
        stat = os.fstat(self.server.fd)
        if (stat.st_size, stat.st_mtime_ns, stat.st_ctime_ns) != (
            self.server.original.st_size,
            self.server.original.st_mtime_ns,
            self.server.original.st_ctime_ns,
        ):
            self.reply(412)
            return
        match = self.headers.get("If-Match")
        if match is not None and match != self.server.etag:
            self.reply(412)
            return
        size = stat.st_size
        if head:
            self.reply(200, size)
            return
        requested = self.headers.get("Range")
        if requested is None:
            begin, end, status = 0, size - 1, 200
        else:
            parsed = re.fullmatch(r"bytes=(\d+)-(\d+)", requested)
            if parsed is None:
                self.reply(416, content_range=f"bytes */{size}")
                return
            begin, end = map(int, parsed.groups())
            if begin > end or end >= size:
                self.reply(416, content_range=f"bytes */{size}")
                return
            status = 206
        self.reply(status, end - begin + 1, f"bytes {begin}-{end}/{size}" if status == 206 else None)
        try:
            while begin <= end:
                data = os.pread(self.server.fd, min(256 * 1024, end - begin + 1), begin)
                if not data:
                    self.close_connection = True
                    return
                self.wfile.write(data)
                begin += len(data)
        except (BrokenPipeError, ConnectionResetError):
            self.close_connection = True

    def do_HEAD(self):
        self.serve(True)

    def do_GET(self):
        self.serve(False)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("image", type=Path)
    parser.add_argument("--port", type=int, default=8080)
    args = parser.parse_args()
    with ImageServer(("127.0.0.1", args.port), args.image) as server:
        print(f"http://127.0.0.1:{server.server_port}/image.bin", flush=True)
        try:
            server.serve_forever()
        except KeyboardInterrupt:
            pass


if __name__ == "__main__":
    main()
