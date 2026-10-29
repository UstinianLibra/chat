#!/usr/bin/env python3
"""CHAT/1 聊天服务器的应用层消息丢失测试。

本脚本只用于测试，聊天程序本身由 C++ 编写。
脚本会建立 N 个真实 TCP 客户端，每个客户端发送 M 条带序号的 UTF-8 消息，
并验证每个客户端都以恰好一次的频率收到全部消息，且服务器时间戳有效。

用法：
    python3 tools/load_test.py --spawn-server --port 19000
    python3 tools/load_test.py --host 127.0.0.1 --port 9000 \
        --clients 4 --messages 200
"""

import argparse
import os
import socket
import subprocess
import sys
import threading
import time
from collections import Counter, defaultdict
from pathlib import Path

PROTOCOL = "CHAT/1"
MAX_HEADER = 512
TYPES = {
    "HELLO", "WELCOME", "MSG", "JOIN", "LEAVE", "SYS",
    "LIST", "LIST_REPLY", "PING", "PONG", "QUIT", "ERROR",
}
ROOT = Path(__file__).resolve().parents[1]


def now_ms() -> int:
    return int(time.time() * 1000)


def send_frame(sock: socket.socket, msg_type: str, seq: int, sender: str,
               body: bytes = b"", timestamp_ms: int | None = None) -> None:
    if msg_type not in TYPES:
        raise ValueError(f"bad type {msg_type}")
    if timestamp_ms is None:
        timestamp_ms = now_ms()
    header = (
        f"{PROTOCOL}\t{msg_type}\t{seq}\t{sender}\t{timestamp_ms}\t"
        f"{len(body)}\r\n"
    ).encode("ascii")
    sock.sendall(header)
    if body:
        sock.sendall(body)


def _readline(reader, limit: int) -> bytes:
    line = reader.readline(limit + 1)
    if not line:
        raise ConnectionError("server closed the connection")
    if not line.endswith(b"\r\n"):
        raise ValueError(f"unterminated/oversized header: {line[:80]!r}")
    return line[:-2]


def read_frame(reader):
    line = _readline(reader, MAX_HEADER)
    fields = line.split(b"\t")
    if len(fields) != 6:
        raise ValueError(f"expected 6 header fields, got {len(fields)}")
    if fields[0].decode("ascii", "replace") != PROTOCOL:
        raise ValueError(f"bad protocol {fields[0]!r}")

    msg_type = fields[1].decode("ascii")
    if msg_type not in TYPES:
        raise ValueError(f"bad type {msg_type!r}")
    seq = int(fields[2])
    sender = fields[3].decode("utf-8")
    timestamp_ms = int(fields[4])
    body_len = int(fields[5])
    if not 0 <= body_len <= 65536:
        raise ValueError(f"bad body length {body_len}")

    body = reader.read(body_len)
    if len(body) != body_len:
        raise ConnectionError("server closed inside body")
    return {
        "type": msg_type,
        "seq": seq,
        "sender": sender,
        "timestamp_ms": timestamp_ms,
        "body": body,
    }


class TestClient(threading.Thread):
    def __init__(self, index: int, host: str, port: int, messages: int,
                 total_clients: int, barrier: threading.Barrier,
                 results: list, errors: list):
        super().__init__(name=f"client-{index}")
        self.index = index
        self.nick = f"Client{index:02d}"
        self.host = host
        self.port = port
        self.messages = messages
        self.total_clients = total_clients
        self.total_expected = messages * total_clients
        self.barrier = barrier
        self.results = results
        self.errors = errors

    def run(self) -> None:
        sock = None
        reader = None
        try:
            sock = socket.create_connection((self.host, self.port), timeout=10)
            sock.settimeout(20)
            reader = sock.makefile("rb")
            send_frame(sock, "HELLO", 0, self.nick, b"", now_ms())

            while True:
                frame = read_frame(reader)
                if frame["type"] == "WELCOME":
                    break
                if frame["type"] == "ERROR":
                    raise RuntimeError(frame["body"].decode("utf-8", "replace"))

            # 屏障释放前，所有客户端都已完成注册。
            self.barrier.wait(timeout=15)

            received = []
            received_lock = threading.Lock()
            done = threading.Event()
            rx_error = []

            def receive():
                try:
                    while not done.is_set():
                        frame = read_frame(reader)
                        if frame["type"] != "MSG":
                            continue
                        with received_lock:
                            received.append(frame)
                            if len(received) >= self.total_expected:
                                done.set()
                                return
                except Exception as exc:  # noqa: BLE001 - 由主线程统一汇总
                    rx_error.append(exc)
                    done.set()

            rx = threading.Thread(target=receive, name=f"{self.name}-rx")
            rx.start()

            for seq in range(1, self.messages + 1):
                text = (
                    f"客户端{self.index} 消息{seq} / client {self.index} "
                    f"msg {seq} 你好，世界!"
                )
                send_frame(sock, "MSG", seq, self.nick, text.encode("utf-8"))

            done.wait(timeout=20)
            with received_lock:
                snapshot = list(received)
            rx.join(timeout=5)

            if rx_error:
                raise RuntimeError(f"receive failed: {rx_error[0]}")
            if len(snapshot) < self.total_expected:
                raise RuntimeError(
                    f"timeout: received {len(snapshot)} of "
                    f"{self.total_expected} messages"
                )

            keys = [(f["sender"], f["seq"]) for f in snapshot]
            counts = Counter(keys)
            expected = {
                (f"Client{i:02d}", seq)
                for i in range(1, self.total_clients + 1)
                for seq in range(1, self.messages + 1)
            }
            actual = set(keys)
            duplicates = sum(1 for n in counts.values() if n > 1)
            missing = sorted(expected - actual)
            unexpected = sorted(actual - expected)

            by_sender = defaultdict(list)
            for sender, seq in actual:
                by_sender[sender].append(seq)
            sequence_gaps = 0
            for sender, seqs in by_sender.items():
                seqs.sort()
                wanted = list(range(1, self.messages + 1))
                if seqs != wanted:
                    sequence_gaps += 1

            bad_timestamp = sum(
                1 for f in snapshot if f["timestamp_ms"] <= 0
            )
            chinese_ok = all(
                "你好，世界!" in f["body"].decode("utf-8", "replace")
                for f in snapshot
            )

            self.results.append({
                "nick": self.nick,
                "received": len(snapshot),
                "expected": self.total_expected,
                "missing": len(missing),
                "duplicates": duplicates,
                "unexpected": len(unexpected),
                "sequence_gap_kinds": sequence_gaps,
                "bad_timestamps": bad_timestamp,
                "chinese_ok": chinese_ok,
            })

            send_frame(sock, "QUIT", 0, self.nick, b"", now_ms())
        except Exception as exc:  # noqa: BLE001 - 在末尾统一汇总
            self.errors.append(f"{self.nick}: {exc}")
        finally:
            if reader is not None:
                try:
                    reader.close()
                except OSError:
                    pass
            if sock is not None:
                try:
                    sock.close()
                except OSError:
                    pass

def wait_for_server(host: str, port: int, proc: subprocess.Popen,
                    timeout: float = 5.0) -> None:
    deadline = time.time() + timeout
    while time.time() < deadline:
        if proc.poll() is not None:
            output = proc.stdout.read().decode("utf-8", "replace")
            raise RuntimeError(f"server exited early:\n{output}")
        try:
            with socket.create_connection((host, port), timeout=0.2):
                return
        except OSError:
            time.sleep(0.05)
    raise RuntimeError("server did not start listening in time")


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--port", type=int, default=19000)
    parser.add_argument("--clients", type=int, default=4)
    parser.add_argument("--messages", type=int, default=200)
    parser.add_argument("--spawn-server", action="store_true")
    args = parser.parse_args()

    if args.clients < 1 or args.messages < 1:
        parser.error("--clients and --messages must be positive")

    server = None
    if args.spawn_server:
        exe = ROOT / "build" / "chat_server"
        if not exe.exists():
            print(f"missing {exe}; run make first", file=sys.stderr)
            return 2
        server = subprocess.Popen(
            [str(exe), str(args.port)],
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
        )
        try:
            wait_for_server(args.host, args.port, server)
        except Exception as exc:  # noqa: BLE001 - 由调用方处理
            print(f"FAIL: {exc}", file=sys.stderr)
            server.kill()
            return 2

    results = []
    errors = []
    barrier = threading.Barrier(args.clients)
    clients = [
        TestClient(i, args.host, args.port, args.messages, args.clients,
                   barrier, results, errors)
        for i in range(1, args.clients + 1)
    ]

    started = time.time()
    for client in clients:
        client.start()
    for client in clients:
        client.join(timeout=40)
    elapsed = time.time() - started

    if any(c.is_alive() for c in clients):
        errors.append("at least one test client did not terminate")

    if server is not None:
        server.terminate()
        try:
            log = server.communicate(timeout=3)[0].decode("utf-8", "replace")
        except subprocess.TimeoutExpired:
            server.kill()
            log = server.communicate()[0].decode("utf-8", "replace")
        if errors:
            print("--- server log ---")
            print(log)

    print("=== CHAT/1 application-level loss test ===")
    print(f"server: {args.host}:{args.port}")
    print(f"clients: {args.clients}, messages/client: {args.messages}")
    print(f"elapsed: {elapsed:.3f}s")
    for result in sorted(results, key=lambda r: r["nick"]):
        print(
            f"  {result['nick']}: recv={result['received']}/"
            f"{result['expected']} missing={result['missing']} "
            f"dup={result['duplicates']} seq_gap_kinds="
            f"{result['sequence_gap_kinds']} "
            f"bad_ts={result['bad_timestamps']} "
            f"utf8_zh={'OK' if result['chinese_ok'] else 'FAIL'}"
        )

    if errors:
        print("errors:", file=sys.stderr)
        for error in errors:
            print(f"  {error}", file=sys.stderr)
        print("RESULT: FAIL")
        return 1

    if len(results) != args.clients:
        print("RESULT: FAIL (missing client results)")
        return 1

    failures = [
        r for r in results
        if r["received"] != r["expected"] or r["missing"] or
        r["duplicates"] or r["unexpected"] or r["sequence_gap_kinds"] or
        r["bad_timestamps"] or not r["chinese_ok"]
    ]
    if failures:
        print("RESULT: FAIL")
        return 1

    print("application-level missing messages: 0")
    print("duplicates/unexpected: 0")
    print("timestamps present: OK")
    print("Chinese UTF-8 round-trip: OK")
    print("RESULT: PASS (no application-level message loss observed)")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
