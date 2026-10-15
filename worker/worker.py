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
    build_task_request_message,
    build_task_result_message,
    log_event,
    recv_msgs,
    send_msg,
    validate_message,
)

CONFIG_PATH = Path(__file__).with_name("config.json")
DEFAULT_MASTER_IDENTITY = {
    "group_label": "p2p-cluster",
    "node_id": "cf662f72-9291-48fd-bc0e-591eea76e6b1",
    "node_label": "master",
    "node_role": "master",
    "host": "127.0.0.1",
    "port": 9000,
}


def _default_label_from_path(path: Path) -> str:
    stem = (path.stem or "worker-a").strip().replace("_", "-")
    if not stem:
        return "worker-a"
    if stem == "worker":
        return "worker-a"
    return stem


def _coerce_assignment_epoch(value: object) -> int:
    if isinstance(value, bool):
        return 0
    if isinstance(value, int):
        return max(0, value)
    if isinstance(value, str):
        stripped = value.strip()
        if stripped.isdigit():
            return max(0, int(stripped))
    return 0


def _coerce_loan_id(value: object, node_id: str, node_label: str) -> str | None:
    if value is None:
        return None
    if isinstance(value, str):
        stripped = value.strip()
        if stripped:
            return stripped
    raise ValueError("loan_id must be null or a non-empty string")


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


def _normalize_master_identity(config: Dict[str, object] | None, default_host: str = "127.0.0.1", default_port: int = 9000) -> Dict[str, object]:
    source = dict((config or {}).get("master") or {}) if isinstance(config, dict) else {}
    normalized = {
        "group_label": str(source.get("group_label") or DEFAULT_MASTER_IDENTITY["group_label"]),
        "node_id": _ensure_v4_uuid(source.get("node_id") or source.get("uuid") or DEFAULT_MASTER_IDENTITY["node_id"]),
        "node_label": str(source.get("node_label") or source.get("label") or DEFAULT_MASTER_IDENTITY["node_label"]),
        "node_role": str(source.get("node_role") or DEFAULT_MASTER_IDENTITY["node_role"]),
        "host": str(source.get("host") or default_host),
        "port": int(source.get("port", default_port)),
    }
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
        config["assignment_epoch"] = _coerce_assignment_epoch(0)
        config["loan_id"] = None
        config["master"] = _normalize_master_identity({"master": {}}, default_host="127.0.0.1", default_port=9000)
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
    normalized["assignment_epoch"] = _coerce_assignment_epoch(data.get("assignment_epoch", 0))
    if data.get("loan_id") is None:
        normalized["loan_id"] = None
    else:
        normalized["loan_id"] = _coerce_loan_id(data.get("loan_id"), str(normalized["node_id"]), str(normalized["node_label"]))
    normalized["master"] = _normalize_master_identity({"master": data.get("master")}, default_host="127.0.0.1", default_port=9000)
    config_path.write_text(json.dumps(normalized, indent=2), encoding="utf-8")
    return normalized


def load_master_identity(config: Dict[str, object] | None = None) -> Dict[str, object]:
    if config is None:
        config = {}
    return _normalize_master_identity(config, default_host="127.0.0.1", default_port=9000)


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

                request_id = message.get("request_id", "unknown")
                log_event(
                    "ASD",
                    message.get("source", {}).get("node_label", "unknown"),
                    message.get("destination", {}).get("node_label", "unknown"),
                    message.get("type", "unknown"),
                    request_id,
                    "received",
                )

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



def sum_range(start: int, end: int) -> int:
    if not isinstance(start, int) or isinstance(start, bool):
        raise ValueError("start must be an integer")
    if not isinstance(end, int) or isinstance(end, bool):
        raise ValueError("end must be an integer")
    if not (0 <= start <= end <= 10000000):
        raise ValueError("invalid range: 0 <= start <= end <= 10000000 required")
    return sum(range(start, end + 1))


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
        self._task_polling_thread: threading.Thread | None = None
        self._loop_thread: threading.Thread | None = None
        self.busy = False
        self.current_task: Dict[str, object] | None = None
        self.pending_results: Dict[str, Dict[str, object]] = {}
        self.next_task_ready_at = 0.0
        self.result_retry_interval = 0.25

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

    def _set_task_polling_thread(self, thread: threading.Thread | None) -> None:
        with self._lock:
            self._task_polling_thread = thread

    def should_poll_for_task(self) -> bool:
        if self.busy:
            return False
        if self.current_session is None or self.current_session.is_closed():
            return False
        return time.monotonic() >= self.next_task_ready_at

    def request_task(self, session: WorkerControlSession | None = None) -> bool:
        if session is None:
            session = self.current_session
        if session is None or session.is_closed():
            return False
        if self.busy:
            return False
        if time.monotonic() < self.next_task_ready_at:
            return False

        master_identity = load_master_identity(self.config)
        assignment_epoch = _coerce_assignment_epoch(self.config.get("assignment_epoch", 0))
        loan_id = None if self.config.get("loan_id") is None else _coerce_loan_id(self.config.get("loan_id"), str(self.config.get("node_id", "")), str(self.config.get("node_label", "worker-a")))
        message = build_task_request_message(
            source={
                "group_label": str(self.config.get("group_label", "p2p-cluster")),
                "node_id": str(self.config.get("node_id", self.config.get("uuid", str(uuid.uuid4())))),
                "node_label": str(self.config.get("node_label", self.config.get("label", "worker-a"))),
                "node_role": str(self.config.get("node_role", "worker")),
            },
            destination={
                "group_label": str(master_identity.get("group_label", "p2p-cluster")),
                "node_id": str(master_identity.get("node_id", "00000000-0000-4000-8000-000000000000")),
                "node_label": str(master_identity.get("node_label", "master")),
                "node_role": str(master_identity.get("node_role", "master")),
            },
            assignment_epoch=assignment_epoch,
            loan_id=loan_id,
        )
        session.send_message(message)
        log_event("ASD", str(self.config.get("node_label", "worker-a")), "master", "task_request", message["request_id"], "sent")
        return True

    def _pending_result_key(self, task_id: str, attempt_id: str) -> str:
        return f"{str(task_id)}::{str(attempt_id)}"

    def _remember_pending_result(self, task_result: dict) -> None:
        if not isinstance(task_result, dict):
            return
        payload = task_result.get("payload", {}) if isinstance(task_result.get("payload", {}), dict) else {}
        task_id = payload.get("task_id")
        attempt_id = payload.get("attempt_id")
        if not isinstance(task_id, str) or not task_id.strip() or not isinstance(attempt_id, str) or not attempt_id.strip():
            return
        key = self._pending_result_key(task_id, attempt_id)
        with self._lock:
            self.pending_results[key] = {
                "task_id": str(task_id),
                "attempt_id": str(attempt_id),
                "message": dict(task_result),
                "last_sent_at": time.monotonic(),
                "send_count": 1,
            }

    def _drop_pending_result(self, task_id: str, attempt_id: str) -> None:
        key = self._pending_result_key(task_id, attempt_id)
        with self._lock:
            self.pending_results.pop(key, None)

    def retransmit_pending_results(self, session: WorkerControlSession | None = None) -> int:
        if session is None:
            session = self.current_session
        if session is None or session.is_closed():
            return 0

        with self._lock:
            pending = list(self.pending_results.values())

        sent = 0
        for item in pending:
            message = item.get("message")
            if not isinstance(message, dict):
                continue
            session.send_message(message)
            sent += 1
            with self._lock:
                key = self._pending_result_key(str(item.get("task_id", "")), str(item.get("attempt_id", "")))
                existing = self.pending_results.get(key)
                if existing is not None:
                    existing["last_sent_at"] = time.monotonic()
                    existing["send_count"] = int(existing.get("send_count", 1)) + 1
        return sent

    def process_inbound_message(self, message: dict) -> dict | bool:
        if not isinstance(message, dict) or not validate_message(message):
            return False

        msg_type = message.get("type")
        if msg_type == "task_assignment":
            payload = message.get("payload", {}) if isinstance(message.get("payload", {}), dict) else {}
            if not payload:
                return False

            self.busy = True
            self.current_task = {
                "task_id": str(payload.get("task_id", "")),
                "task_label": str(payload.get("task_label", "")),
                "attempt_id": str(payload.get("attempt_id", "")),
                "task_kind": payload.get("task_kind"),
                "parameters": dict(payload.get("parameters", {})),
                "assignment_epoch": int(payload.get("assignment_epoch", 0)),
                "loan_id": payload.get("loan_id"),
            }
            self.config["assignment_epoch"] = int(payload.get("assignment_epoch", self.config.get("assignment_epoch", 0)))
            self.config["loan_id"] = payload.get("loan_id")
            log_event("ASD", str(self.config.get("node_label", "worker-a")), "master", "task_assignment", message.get("request_id", "unknown"), "received")

            start = int(self.current_task["parameters"].get("start", 0))
            end = int(self.current_task["parameters"].get("end", 0))
            try:
                total = sum_range(start, end)
                status = "ok"
                result = {"sum": total}
                error_code = None
            except ValueError:
                status = "nok"
                result = None
                error_code = "invalid_range"

            task_result = build_task_result_message(
                message,
                source={
                    "group_label": str(self.config.get("group_label", "p2p-cluster")),
                    "node_id": str(self.config.get("node_id", self.config.get("uuid", str(uuid.uuid4())))),
                    "node_label": str(self.config.get("node_label", self.config.get("label", "worker-a"))),
                    "node_role": str(self.config.get("node_role", "worker")),
                },
                destination={
                    "group_label": str(message["source"]["group_label"]),
                    "node_id": str(message["source"]["node_id"]),
                    "node_label": str(message["source"]["node_label"]),
                    "node_role": str(message["source"]["node_role"]),
                },
                task_id=str(self.current_task["task_id"]),
                task_label=str(self.current_task["task_label"]),
                attempt_id=str(self.current_task["attempt_id"]),
                assignment_epoch=int(self.current_task["assignment_epoch"]),
                loan_id=self.current_task["loan_id"],
                status=status,
                result=result,
                error_code=error_code,
            )
            self._remember_pending_result(task_result)
            if self.current_session is not None:
                self.current_session.send_message(task_result)
            log_event("ASD", str(self.config.get("node_label", "worker-a")), "master", "task_result", task_result["request_id"], status)
            return task_result

        if msg_type == "no_task":
            payload = message.get("payload", {}) if isinstance(message.get("payload", {}), dict) else {}
            retry_after_ms = int(payload.get("retry_after_ms", 1000))
            self.next_task_ready_at = time.monotonic() + max(0.0, retry_after_ms / 1000.0)
            log_event("ASD", str(self.config.get("node_label", "worker-a")), "master", "no_task", message.get("request_id", "unknown"), f"retry_after_ms={retry_after_ms}")
            return True

        if msg_type == "task_ack":
            return self.process_task_ack(message)

        return False

    def process_task_ack(self, message: dict) -> bool:
        if not isinstance(message, dict) or not validate_message(message):
            return False
        if message.get("type") != "task_ack":
            return False

        payload = message.get("payload", {}) if isinstance(message.get("payload", {}), dict) else {}
        task_id = payload.get("task_id")
        attempt_id = payload.get("attempt_id")
        accepted = payload.get("accepted")
        reason = payload.get("reason")

        if not isinstance(task_id, str) or not task_id.strip() or not isinstance(attempt_id, str) or not attempt_id.strip():
            return False

        if accepted is False and reason == "stale_attempt":
            self._drop_pending_result(task_id, attempt_id)
            if self.current_task is not None and str(task_id) == str(self.current_task.get("task_id")) and str(attempt_id) == str(self.current_task.get("attempt_id")):
                self.busy = False
                self.current_task = None
            log_event("ASD", str(self.config.get("node_label", "worker-a")), "master", "task_ack", message.get("request_id", "unknown"), "stale_attempt")
            return False

        if not isinstance(accepted, bool) or accepted is not True:
            return False

        key = self._pending_result_key(task_id, attempt_id)
        pending_match = key in self.pending_results
        if self.current_task is None:
            if pending_match:
                self._drop_pending_result(task_id, attempt_id)
                log_event("ASD", str(self.config.get("node_label", "worker-a")), "master", "task_ack", message.get("request_id", "unknown"), "completed")
                return True
            return False

        if str(task_id) == str(self.current_task.get("task_id")) and str(attempt_id) == str(self.current_task.get("attempt_id")):
            self._drop_pending_result(task_id, attempt_id)
            self.busy = False
            self.current_task = None
            log_event("ASD", str(self.config.get("node_label", "worker-a")), "master", "task_ack", message.get("request_id", "unknown"), "completed")
            return True
        if pending_match:
            self._drop_pending_result(task_id, attempt_id)
            log_event("ASD", str(self.config.get("node_label", "worker-a")), "master", "task_ack", message.get("request_id", "unknown"), "completed")
            return True
        return False

    def start_task_polling_loop(self, session: WorkerControlSession | None = None, interval_seconds: float = 0.5) -> threading.Thread | None:
        if session is None:
            session = self.current_session
        if session is None or session.is_closed():
            return None

        def loop() -> None:
            while not self.stop_event.is_set() and not session.is_closed():
                if self.busy:
                    if self.pending_results:
                        self.retransmit_pending_results(session)
                    try:
                        message = session.wait_for_message(types={"task_ack"}, timeout=max(0.5, interval_seconds), stop_event=self.stop_event)
                    except TimeoutError:
                        continue
                    self.process_inbound_message(message)
                    continue

                if time.monotonic() < self.next_task_ready_at:
                    remaining = max(0.05, self.next_task_ready_at - time.monotonic())
                    if self.stop_event.wait(min(interval_seconds, remaining)):
                        break
                    continue

                self.request_task(session)
                try:
                    message = session.wait_for_message(types={"task_assignment", "no_task", "task_ack"}, timeout=max(0.5, interval_seconds), stop_event=self.stop_event)
                except TimeoutError:
                    continue
                self.process_inbound_message(message)

        thread = threading.Thread(target=loop, daemon=True)
        thread.start()
        self._set_task_polling_thread(thread)
        return thread

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
            self.retransmit_pending_results(session)
            self.start_task_polling_loop(session)
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
            self.busy = False


def open_control_session(master_host: str, master_port: int, timeout: float = 5.0) -> WorkerControlSession:
    sock = socket.create_connection((master_host, master_port), timeout=timeout)
    sock.settimeout(None)
    return WorkerControlSession(sock)


def send_with_retry(session: WorkerControlSession, message: dict, *, expect_type: str, timeout: float = 5.0, max_attempts: int = 3):
    last_error = None
    for attempt in range(1, max_attempts + 1):
        session.send_message(message)
        try:
            return session.wait_for_message(types={expect_type}, request_id=message["request_id"], timeout=timeout)
        except TimeoutError as exc:
            last_error = exc
            if attempt >= max_attempts:
                raise
    if last_error is not None:
        raise last_error
    raise TimeoutError(f"No matching message received for {message['request_id']}")


def register_worker(master_host: str, master_port: int, config: Dict[str, object], control_session: WorkerControlSession | None = None, stop_event: threading.Event | None = None) -> bool:
    node_id = str(config.get("node_id", config.get("uuid", str(uuid.uuid4()))))
    node_label = str(config.get("node_label", config.get("label", "worker-a")))
    node_role = str(config.get("node_role", "worker"))
    master_identity = load_master_identity(config)
    assignment_epoch = _coerce_assignment_epoch(config.get("assignment_epoch", 0))
    loan_id = None if config.get("loan_id") is None else _coerce_loan_id(config.get("loan_id"), node_id, node_label)
    config["assignment_epoch"] = assignment_epoch
    config["loan_id"] = loan_id
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
            "home_master": {
                "group_label": str(master_identity.get("group_label", "p2p-cluster")),
                "node_id": str(master_identity.get("node_id", "00000000-0000-4000-8000-000000000000")),
                "node_label": str(master_identity.get("node_label", "master")),
                "node_role": str(master_identity.get("node_role", "master")),
                "host": str(master_identity.get("host", "127.0.0.1")),
                "port": int(master_identity.get("port", 9000)),
            },
            "assignment_epoch": assignment_epoch,
            "loan_id": loan_id,
            "host": str(config.get("host", "127.0.0.1")),
            "port": int(config.get("port", 9101)),
        },
    }

    session = control_session or open_control_session(master_host, master_port)
    try:
        try:
            reply = send_with_retry(session, message, expect_type="registration_ack", timeout=5.0, max_attempts=3)
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
    master_identity = load_master_identity(config)
    assignment_epoch = _coerce_assignment_epoch(config.get("assignment_epoch", 0))
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
        assignment_epoch=assignment_epoch,
    )

    session = control_session or open_control_session(master_host, master_port)
    try:
        log_event("ASD", str(config.get("node_label", "worker-a")), "master", "heartbeat", message["request_id"], "sent")
        try:
            reply = send_with_retry(session, message, expect_type="heartbeat_ack", timeout=5.0, max_attempts=3)
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


def main() -> None:
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
    config["master"] = _normalize_master_identity({"master": config.get("master")}, default_host=args.master_host, default_port=args.master_port)

    stop_event = threading.Event()
    run_worker(args.master_host, args.master_port, config, stop_event=stop_event)


if __name__ == "__main__":
    main()
