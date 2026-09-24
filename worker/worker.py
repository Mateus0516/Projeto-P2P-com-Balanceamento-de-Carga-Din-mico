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


def _default_label_from_path(path: Path) -> str:
    stem = (path.stem or "worker-a").strip().replace("_", "-")
    if not stem:
        return "worker-a"
    if stem == "worker":
        return "worker-a"
    return stem


def _default_port_for_label(label: str, default_port: int = 9101) -> int:
    normalized = str(label).lower().replace("_", "-")
    if normalized.endswith("-b"):
        return 9102
    if normalized.endswith("-a"):
        return 9101
    return int(default_port)


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
    config_path = path if path.is_absolute() else (Path.cwd() / path).resolve()
    inferred_label = _default_label_from_path(config_path)
    inferred_port = _default_port_for_label(inferred_label, default_port=9101)

    if not config_path.exists():
        config = _normalize_identity(
            {"group_label": "p2p-cluster", "node_role": "worker", "node_label": inferred_label, "host": "127.0.0.1", "port": inferred_port},
            role="worker",
            default_label=inferred_label,
            default_host="127.0.0.1",
            default_port=inferred_port,
        )
        config_path.parent.mkdir(parents=True, exist_ok=True)
        config_path.write_text(json.dumps(config, indent=2), encoding="utf-8")
        return config

    with config_path.open("r", encoding="utf-8") as handle:
        data = json.load(handle)

    normalized = _normalize_identity(
        data,
        role="worker",
        default_label=inferred_label,
        default_host="127.0.0.1",
        default_port=inferred_port,
    )
    config_path.write_text(json.dumps(normalized, indent=2), encoding="utf-8")
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
        self._close_callbacks = []
        self._reader = threading.Thread(target=self._reader_loop, daemon=True)
        self._reader.start()

    def add_close_callback(self, callback):
        if callback is not None:
            self._close_callbacks.append(callback)

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
        for callback in list(self._close_callbacks):
            callback(self)

    def _reader_loop(self) -> None:
        while not self._closed.is_set():
            try:
                messages = recv_msgs(self.sock, self.buffer)
            except (ConnectionAbortedError, ConnectionResetError, BrokenPipeError, OSError):
                self._mark_closed()
                return

            if messages is None:
                self._mark_closed()
                return
            if not messages:
                continue

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

    def wait_for_message(self, *, types: set[str] | None = None, request_id: str | None = None, timeout: float = 5.0, stop_event: threading.Event | None = None) -> dict:
        deadline = time.monotonic() + timeout
        while time.monotonic() < deadline:
            if stop_event is not None and stop_event.is_set():
                raise TimeoutError(f"Session stopped while waiting for {request_id or types or 'any'}")
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



def compute_retry_delay(attempt_number: int) -> float:
    if attempt_number <= 1:
        return 0.5
    return min(0.5 * (2 ** (attempt_number - 1)), 5.0)


class WorkerSessionManager:
    def __init__(self, master_host: str, master_port: int, config: Dict[str, object], stop_event: threading.Event | None = None, retry_delay_factory=None):
        self.master_host = master_host
        self.master_port = master_port
        self.config = config
        self.stop_event = stop_event or threading.Event()
        self.retry_delay_factory = retry_delay_factory or compute_retry_delay
        self._lock = threading.RLock()
        self._current_session: WorkerControlSession | None = None
        self._current_heartbeat_thread: threading.Thread | None = None
        self._loop_thread: threading.Thread | None = None

    @property
    def current_session(self) -> WorkerControlSession | None:
        with self._lock:
            return self._current_session

    def _handle_session_closed(self, session: WorkerControlSession) -> None:
        with self._lock:
            if self._current_session is session:
                self._current_session = None
        log_event("ASD", str(self.config.get("node_label", "worker-a")), "master", "session", "-", "lost")

    def _set_session(self, session: WorkerControlSession | None) -> None:
        with self._lock:
            previous = self._current_session
            if previous is not None and previous is not session:
                previous.close()
            self._current_session = session
            if session is not None and self._handle_session_closed not in session._close_callbacks:
                session.add_close_callback(self._handle_session_closed)

    def _start_heartbeat_for_session(self, session: WorkerControlSession) -> None:
        thread = start_heartbeat_loop(self.master_host, self.master_port, self.config, control_session=session)
        with self._lock:
            self._current_heartbeat_thread = thread

    def _connect_and_register(self) -> WorkerControlSession | None:
        if self.stop_event.is_set():
            return None
        try:
            session = open_control_session(self.master_host, self.master_port, timeout=1.0)
        except OSError:
            return None

        accepted = register_worker(self.master_host, self.master_port, self.config, control_session=session, stop_event=self.stop_event)
        if not accepted:
            session.close()
            return None
        return session

    def _run_reconnect_loop(self) -> None:
        attempt = 0
        while not self.stop_event.is_set():
            session = self._connect_and_register()
            if session is None:
                attempt += 1
                delay = self.retry_delay_factory(attempt)
                log_event("ASD", str(self.config.get("node_label", "worker-a")), "master", "reconnect", f"attempt-{attempt}", f"retrying in {delay:.2f}s")
                if self.stop_event.wait(delay):
                    break
                continue

            attempt = 0
            self._set_session(session)
            self._start_heartbeat_for_session(session)
            log_event("ASD", str(self.config.get("node_label", "worker-a")), "master", "reconnect", "-", "established")

            while not self.stop_event.is_set():
                if session.is_closed():
                    with self._lock:
                        if self._current_session is session:
                            self._current_session = None
                    break
                self.stop_event.wait(0.1)

    def start(self) -> threading.Thread | None:
        if self.stop_event.is_set():
            return None
        if self._loop_thread is not None and self._loop_thread.is_alive():
            return self._loop_thread
        self._loop_thread = threading.Thread(target=self._run_reconnect_loop, daemon=True)
        self._loop_thread.start()
        return self._loop_thread

    def stop(self) -> None:
        self.stop_event.set()
        with self._lock:
            session = self._current_session
            if session is not None:
                session.close()

            self._current_session = None


def open_control_session(master_host: str, master_port: int, timeout: float = 5.0) -> WorkerControlSession:
    sock = socket.create_connection((master_host, master_port), timeout=timeout)
    sock.settimeout(None)
    return WorkerControlSession(sock)


def register_worker(master_host: str, master_port: int, config: Dict[str, object], control_session: WorkerControlSession | None = None, stop_event: threading.Event | None = None) -> bool:
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
            reply = session.wait_for_message(types={"registration_ack"}, request_id=message["request_id"], timeout=5, stop_event=stop_event)
        except TimeoutError:
            log_event("ASD", node_label, "master", "registration_ack", message["request_id"], "timeout")
            return False

        status = reply.get("payload", {}).get("status", "unknown")
        reason = reply.get("payload", {}).get("reason")
        if status == "accepted":
            log_event("ASD", node_label, "master", reply["type"], reply["request_id"], "accepted")
            return True

        log_event("ASD", node_label, "master", reply["type"], reply["request_id"], f"rejected:{reason or 'unknown'}")
        return False
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


def run_worker(master_host: str, master_port: int, config: Dict[str, object], stop_event: threading.Event | None = None, retry_delay_factory=None):
    stop = stop_event or threading.Event()
    manager = WorkerSessionManager(master_host, master_port, config, stop_event=stop, retry_delay_factory=retry_delay_factory)
    manager.start()
    while not stop.is_set():
        time.sleep(0.1)
    manager.stop()
    return manager


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
                if messages is None:
                    break
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
    parser.add_argument("--config", type=Path, default=CONFIG_PATH)
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--port", type=int, default=9101)
    parser.add_argument("--master-host", default="127.0.0.1")
    parser.add_argument("--master-port", type=int, default=9000)
    args = parser.parse_args()

    config_path = args.config if args.config.is_absolute() else (Path.cwd() / args.config).resolve()
    config = load_or_create_config(config_path)
    config["host"] = args.host
    if args.port != 9101 or config.get("node_label") == "worker-b":
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

    stop_event = threading.Event()
    run_worker(args.master_host, args.master_port, config, stop_event=stop_event)
