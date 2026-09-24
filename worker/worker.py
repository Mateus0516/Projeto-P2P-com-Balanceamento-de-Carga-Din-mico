#!/usr/bin/env python3
import argparse
import json
import queue
import socket
import sys
import threading
import time
import uuid
from pathlib import Path
from typing import Dict

ROOT = Path(__file__).resolve().parent.parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from protocol import (
    build_heartbeat_ack_message,
    build_heartbeat_message,
    log_event,
    recv_msgs,
    send_msg,
    validate_message,
)

CONFIG_PATH = Path(__file__).with_name("config.json")


def _ensure_v4_uuid(value: object) -> str:
    try:
        candidate = uuid.UUID(str(value))
    except (TypeError, ValueError, AttributeError):
        return str(uuid.uuid4())

    return str(candidate) if candidate.version == 4 else str(uuid.uuid4())


def _normalize_identity(config: Dict[str, object], role: str, default_label: str, default_host: str, default_port: int) -> Dict[str, object]:
    normalized = dict(config)
    normalized["group_label"] = str(normalized.get("group_label") or "p2p-cluster")
    normalized["node_role"] = str(normalized.get("node_role") or role)
    normalized["node_label"] = str(normalized.get("node_label") or normalized.get("label") or default_label)
    normalized["node_id"] = _ensure_v4_uuid(normalized.get("node_id") or normalized.get("uuid"))
    normalized["host"] = str(normalized.get("host") or default_host)
    normalized["port"] = int(normalized.get("port", default_port))

    normalized["uuid"] = normalized["node_id"]
    normalized["label"] = normalized["node_label"]
    return normalized


def load_or_create_config(path: Path) -> Dict[str, object]:
    if not path.exists():
        config = _normalize_identity(
            {"group_label": "p2p-cluster", "node_role": "worker", "node_label": "worker-a", "host": "127.0.0.1", "port": 9101},
            role="worker",
            default_label="worker-a",
            default_host="127.0.0.1",
            default_port=9101,
        )
        path.write_text(json.dumps(config, indent=2), encoding="utf-8")
        return config

    with path.open("r", encoding="utf-8") as handle:
        data = json.load(handle)

    normalized = _normalize_identity(
        data,
        role="worker",
        default_label="worker-a",
        default_host="127.0.0.1",
        default_port=9101,
    )
    path.write_text(json.dumps(normalized, indent=2), encoding="utf-8")
    return normalized


def load_master_identity() -> Dict[str, object]:
    master_config_path = Path(__file__).resolve().parent.parent / "master" / "config.json"
    if not master_config_path.exists():
        return {
            "group_label": "p2p-cluster",
            "node_id": "00000000-0000-4000-8000-000000000000",
            "node_label": "master",
            "node_role": "master",
        }

    with master_config_path.open("r", encoding="utf-8") as handle:
        data = json.load(handle)

    return {
        "group_label": str(data.get("group_label", "p2p-cluster")),
        "node_id": str(data.get("node_id", "00000000-0000-4000-8000-000000000000")),
        "node_label": str(data.get("node_label", "master")),
        "node_role": str(data.get("node_role", "master")),
    }


class WorkerControlSession:
    def __init__(self, sock: socket.socket):
        self.sock = sock
        self.lock = threading.Lock()
        self.buffer = bytearray()
        self._queue: "queue.Queue[dict]" = queue.Queue()
        self._closed = threading.Event()
        self._reader = threading.Thread(target=self._reader_loop, daemon=True)
        self._reader.start()

    def is_closed(self) -> bool:
        return self._closed.is_set()

    def _mark_closed(self) -> None:
        if self._closed.is_set():
            return
        self._closed.set()
        try:
            self.sock.shutdown(socket.SHUT_RDWR)
        except OSError:
            pass
        try:
            self.sock.close()
        except OSError:
            pass

    def _reader_loop(self) -> None:
        while not self._closed.is_set():
            try:
                messages = recv_msgs(self.sock, self.buffer)
            except (ConnectionAbortedError, ConnectionResetError, BrokenPipeError, OSError):
                self._mark_closed()
                return

            if not messages:
                self._mark_closed()
                return

            for message in messages:
                if not validate_message(message):
                    continue

                if message.get("type") == "heartbeat":
                    ack = build_heartbeat_ack_message(message, source={
                        "group_label": message["destination"]["group_label"],
                        "node_id": message["destination"]["node_id"],
                        "node_label": message["destination"]["node_label"],
                        "node_role": message["destination"]["node_role"],
                    }, destination={
                        "group_label": message["source"]["group_label"],
                        "node_id": message["source"]["node_id"],
                        "node_label": message["source"]["node_label"],
                        "node_role": message["source"]["node_role"],
                    })
                    with self.lock:
                        if self._closed.is_set():
                            return
                        try:
                            send_msg(self.sock, ack)
                        except (ConnectionAbortedError, ConnectionResetError, BrokenPipeError, OSError):
                            self._mark_closed()
                            return
                    log_event("ASD", message["destination"]["node_label"], message["source"]["node_label"], "heartbeat_ack", message["request_id"], "sent")
                    continue

                self._queue.put(message)

    def send_message(self, message: dict) -> None:
        if self._closed.is_set():
            return

        with self.lock:
            if self._closed.is_set():
                return
            try:
                send_msg(self.sock, message)
            except (ConnectionAbortedError, ConnectionResetError, BrokenPipeError, OSError):
                self._mark_closed()

    def close(self) -> None:
        self._mark_closed()

    def wait_for_message(self, *, types: set[str] | None = None, request_id: str | None = None, timeout: float = 5.0) -> dict:
        deadline = time.monotonic() + timeout
        while time.monotonic() < deadline:
            try:
                remaining = max(0.05, deadline - time.monotonic())
                message = self._queue.get(timeout=remaining)
            except queue.Empty:
                continue

            if types is not None and message.get("type") not in types:
                continue
            if request_id is not None and message.get("request_id") != request_id:
                continue
            return message

        raise TimeoutError(f"No matching message received for {request_id or types or 'any'}")

    def close(self) -> None:
        self._closed.set()
        try:
            self.sock.shutdown(socket.SHUT_RDWR)
        except OSError:
            pass
        try:
            self.sock.close()
        except OSError:
            pass


def open_control_session(master_host: str, master_port: int, timeout: float = 5.0) -> WorkerControlSession:
    sock = socket.create_connection((master_host, master_port), timeout=timeout)
    return WorkerControlSession(sock)


def register_worker(master_host: str, master_port: int, config: Dict[str, object], control_session: WorkerControlSession | None = None) -> None:
    node_id = str(config.get("node_id", config.get("uuid", str(uuid.uuid4()))))
    node_label = str(config.get("node_label", config.get("label", "worker-a")))
    node_role = str(config.get("node_role", "worker"))
    master_identity = load_master_identity()
    message = {
        "version": "sd_2026_2_v1",
        "type": "register_worker",
        "message_id": f"register-{uuid.uuid4()}",
        "request_id": f"req-{uuid.uuid4()}",
        "timestamp_ms": int(time.time() * 1000),
        "source": {
            "group_label": str(config.get("group_label", "p2p-cluster")),
            "node_id": node_id,
            "node_label": node_label,
            "node_role": node_role,
        },
        "destination": {
            "group_label": str(master_identity.get("group_label", "p2p-cluster")),
            "node_id": str(master_identity.get("node_id", "00000000-0000-4000-8000-000000000000")),
            "node_label": str(master_identity.get("node_label", "master")),
            "node_role": str(master_identity.get("node_role", "master")),
        },
        "payload": {
            "host": config["host"],
            "port": config["port"],
        },
    }

    session = control_session or open_control_session(master_host, master_port)
    try:
        session.send_message(message)
        log_event("ASD", node_label, "master", message["type"], message["request_id"], "sent")

        try:
            reply = session.wait_for_message(types={"registration_ack"}, request_id=message["request_id"], timeout=5)
        except TimeoutError:
            log_event("ASD", node_label, "master", "registration_ack", message["request_id"], "timeout")
            return

        status = reply.get("payload", {}).get("status", "unknown")
        reason = reply.get("payload", {}).get("reason")
        if status == "accepted":
            log_event("ASD", node_label, "master", reply["type"], reply["request_id"], "accepted")
            return

        log_event("ASD", node_label, "master", reply["type"], reply["request_id"], f"rejected:{reason or 'unknown'}")
    finally:
        if control_session is None:
            session.close()


def send_heartbeat(master_host: str, master_port: int, config: Dict[str, object], sequence: int, control_session: WorkerControlSession | None = None) -> None:
    master_identity = load_master_identity()
    message = build_heartbeat_message(
        source={
            "group_label": str(config.get("group_label", "p2p-cluster")),
            "node_id": str(config.get("node_id", config.get("uuid", str(uuid.uuid4())))),
            "node_label": str(config.get("node_label", config.get("label", "worker-a"))),
            "node_role": str(config.get("node_role", "worker")),
        },
        destination={
            "group_label": str(master_identity.get("group_label", "p2p-cluster")),
            "node_id": str(master_identity.get("node_id", "00000000-0000-4000-8000-000000000000")),
            "node_label": str(master_identity.get("node_label", "master")),
            "node_role": str(master_identity.get("node_role", "master")),
        },
        sequence=sequence,
    )

    session = control_session or open_control_session(master_host, master_port)
    try:
        session.send_message(message)
        log_event("ASD", str(config.get("node_label", "worker-a")), "master", "heartbeat", message["request_id"], "sent")

        try:
            reply = session.wait_for_message(types={"heartbeat_ack"}, request_id=message["request_id"], timeout=5)
        except TimeoutError:
            log_event("ASD", str(config.get("node_label", "worker-a")), "master", "heartbeat_ack", message["request_id"], "timeout")
            return

        payload = reply.get("payload", {}) if isinstance(reply.get("payload", {}), dict) else {}
        if payload.get("sequence") == sequence and payload.get("status") == "alive":
            log_event("ASD", str(config.get("node_label", "worker-a")), "master", "heartbeat_ack", message["request_id"], "received")
            return

        log_event("ASD", str(config.get("node_label", "worker-a")), "master", "heartbeat_ack", message["request_id"], "rejected")
    finally:
        if control_session is None:
            session.close()


def start_heartbeat_loop(master_host: str, master_port: int, config: Dict[str, object], control_session: WorkerControlSession, interval_seconds: float = 10.0):
    sequence = 0

    def loop() -> None:
        nonlocal sequence
        while not control_session.is_closed():
            if control_session.sock is None:
                break
            if control_session._closed.wait(interval_seconds):
                break
            sequence += 1
            if control_session.is_closed():
                break
            send_heartbeat(master_host, master_port, config, sequence, control_session=control_session)
            if control_session.is_closed():
                break

    thread = threading.Thread(target=loop, daemon=True)
    thread.start()
    return thread


def run_listener(host: str, port: int, worker_identity: Dict[str, object]) -> None:
    server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    server.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    server.bind((host, port))
    server.listen(5)

    def handle_connection(conn: socket.socket, addr) -> None:
        buffer = bytearray()
        try:
            while True:
                messages = recv_msgs(conn, buffer)
                if not messages:
                    continue
                for msg in messages:
                    if not validate_message(msg):
                        continue
                    if msg["type"] == "heartbeat":
                        ack = build_heartbeat_ack_message(msg, source=worker_identity, destination=msg["source"])
                        send_msg(conn, ack)
                        log_event("ASD", worker_identity.get("node_label", "worker"), msg["source"]["node_label"], "heartbeat_ack", msg["request_id"], "sent")
                        continue
                    log_event("ASD", msg["source"]["node_label"], worker_identity.get("node_label", "worker"), msg["type"], msg["request_id"], "received")
        except ConnectionResetError:
            pass
        finally:
            conn.close()

    while True:
        conn, addr = server.accept()
        threading.Thread(target=handle_connection, args=(conn, addr), daemon=True).start()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Worker node for NDJSON registration protocol")
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--port", type=int, default=9101)
    parser.add_argument("--master-host", default="127.0.0.1")
    parser.add_argument("--master-port", type=int, default=9000)
    args = parser.parse_args()

    config = load_or_create_config(CONFIG_PATH)
    config["host"] = args.host
    config["port"] = args.port

    node_label = str(config.get("node_label", config.get("label", "worker-a")))
    worker_identity = {
        "group_label": str(config.get("group_label", "p2p-cluster")),
        "node_id": str(config.get("node_id", config.get("uuid", str(uuid.uuid4())))),
        "node_label": node_label,
        "node_role": str(config.get("node_role", "worker")),
    }
    listener_thread = threading.Thread(target=run_listener, args=(config["host"], config["port"], worker_identity), daemon=True)
    listener_thread.start()

    control_session = open_control_session(args.master_host, args.master_port)
    register_worker(args.master_host, args.master_port, config, control_session=control_session)
    start_heartbeat_loop(args.master_host, args.master_port, config, control_session=control_session)
    while True:
        time.sleep(1)
