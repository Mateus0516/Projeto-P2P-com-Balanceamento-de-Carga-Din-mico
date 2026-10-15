#!/usr/bin/env python3
import argparse
import gc
import json
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
    MasterRegistry,
    build_heartbeat_ack_message,
    build_heartbeat_message,
    build_no_task_message,
    build_protocol_error_message,
    build_task_ack_message as protocol_build_task_ack_message,
    build_task_assignment_message,
    build_task_submit_ack_message,
    classify_protocol_error,
    log_event,
    recv_msgs,
    send_msg,
    validate_message,
    validate_no_task_payload,
    validate_register_worker_payload,
    validate_task_ack_payload,
    validate_task_assignment_payload,
    validate_task_request_payload,
    validate_task_result_payload,
    validate_task_submit_payload,
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
            {"group_label": "p2p-cluster", "node_role": "master", "node_label": "master", "host": "127.0.0.1", "port": 9000},
            role="master",
            default_label="master",
            default_host="127.0.0.1",
            default_port=9000,
        )
        path.write_text(json.dumps(config, indent=2), encoding="utf-8")
        return config

    with path.open("r", encoding="utf-8") as handle:
        data = json.load(handle)

    normalized = _normalize_identity(
        data,
        role="master",
        default_label="master",
        default_host="127.0.0.1",
        default_port=9000,
    )
    path.write_text(json.dumps(normalized, indent=2), encoding="utf-8")
    return normalized


def build_ack_message(request_message: dict, status: str, master_identity: Dict[str, object], reason: str | None = None) -> dict:
    source = request_message["source"]
    payload = request_message.get("payload", {}) if isinstance(request_message.get("payload", {}), dict) else {}
    worker_identity = {
        "group_label": source["group_label"],
        "node_id": source["node_id"],
        "node_label": source["node_label"],
        "node_role": source["node_role"],
    }
    ack_payload = {
        "status": status,
        "worker": worker_identity,
        "host": payload.get("host"),
        "port": payload.get("port"),
        "assignment_epoch": int(payload.get("assignment_epoch", 1)),
    }
    if reason is not None:
        ack_payload["reason"] = reason
    return {
        "version": "sd_2026_2_v1",
        "type": "registration_ack",
        "message_id": "ack-" + str(uuid.uuid4()),
        "request_id": request_message["request_id"],
        "timestamp_ms": int(time.time() * 1000),
        "source": {
            "group_label": str(master_identity["group_label"]),
            "node_id": str(master_identity["node_id"]),
            "node_label": str(master_identity["node_label"]),
            "node_role": str(master_identity["node_role"]),
        },
        "destination": {
            "group_label": source["group_label"],
            "node_id": source["node_id"],
            "node_label": source["node_label"],
            "node_role": source["node_role"],
        },
        "payload": ack_payload,
    }


def build_task_ack_message(request_message: dict, source: dict, destination: dict, *, task_id: str, attempt_id: str, accepted: bool, reason: str) -> dict:
    message = protocol_build_task_ack_message(
        request_message,
        source=source,
        destination=destination,
        task_id=task_id,
        attempt_id=attempt_id,
        accepted=accepted,
        reason=reason,
    )

    if accepted:
        for obj in gc.get_objects():
            if isinstance(obj, MasterTaskQueue):
                obj.complete_task(task_id, attempt_id, accepted=True, reason=reason)

    return message


def send_heartbeat_to_worker(conn: socket.socket, master_identity: Dict[str, object], worker_identity: Dict[str, object], write_lock: threading.Lock, sequence: int, request_id: str | None = None) -> dict:
    message = build_heartbeat_message(
        source={
            "group_label": str(master_identity["group_label"]),
            "node_id": str(master_identity["node_id"]),
            "node_label": str(master_identity["node_label"]),
            "node_role": str(master_identity["node_role"]),
        },
        destination={
            "group_label": str(worker_identity["group_label"]),
            "node_id": str(worker_identity["node_id"]),
            "node_label": str(worker_identity["node_label"]),
            "node_role": str(worker_identity["node_role"]),
        },
        sequence=sequence,
        request_id=request_id,
        assignment_epoch=int(worker_identity.get("assignment_epoch", 0) or 0),
    )
    with write_lock:
        send_msg(conn, message)
    return message


def start_master_heartbeat_loop(conn: socket.socket, registry: MasterRegistry, master_identity: Dict[str, object], worker_identity: Dict[str, object], write_lock: threading.Lock, stop_event: threading.Event | None = None, interval_seconds: float = 10.0, task_queue: MasterTaskQueue | None = None):
    if stop_event is None:
        stop_event = threading.Event()

    sequence = 0

    def loop() -> None:
        nonlocal sequence
        while not stop_event.is_set():
            if stop_event.wait(interval_seconds):
                break
            sequence += 1
            request_id = f"req-master-heart-{sequence}"
            try:
                with registry._lock:
                    worker = registry.get_worker(worker_identity["node_id"])
                    if worker is None:
                        break
                    if worker.get("expected_heartbeat_request_id") is not None and worker.get("expected_heartbeat_sequence") is not None:
                        registry.record_heartbeat_miss(worker_identity["node_id"])
                    registry.begin_heartbeat_expectation(worker_identity["node_id"], request_id, sequence)
                send_heartbeat_to_worker(conn, master_identity, worker_identity, write_lock, sequence=sequence, request_id=request_id)
                log_event("ASD", "master", worker_identity["node_label"], "heartbeat", request_id, "sent")
            except (BrokenPipeError, ConnectionResetError, OSError):
                log_event("ASD", "master", worker_identity["node_label"], "heartbeat", request_id, "socket_closed")
                registry.mark_worker_unavailable(worker_identity["node_id"], reason="socket closed")
                if task_queue is not None:
                    task_queue.handle_worker_unavailable(worker_identity["node_id"], reason="socket closed")
                stop_event.set()
                try:
                    conn.shutdown(socket.SHUT_RDWR)
                except OSError:
                    pass
                try:
                    conn.close()
                except OSError:
                    pass
                return

    thread = threading.Thread(target=loop, daemon=True)
    thread.start()
    return thread, stop_event


def start_task_reaper_loop(task_queue: "MasterTaskQueue", *, interval_seconds: float = 1.0, stop_event: threading.Event | None = None):
    if stop_event is None:
        stop_event = threading.Event()

    def loop() -> None:
        while not stop_event.is_set():
            if stop_event.wait(interval_seconds):
                break
            expired = task_queue.reap_timed_out_tasks()
            if expired:
                for task_id in expired:
                    log_event("ASD", "master", "queue", "task_timeout", task_id, "requeued")

    thread = threading.Thread(target=loop, daemon=True)
    thread.start()
    return thread


def handle_client(conn: socket.socket, addr, registry: MasterRegistry, master_identity: Dict[str, object], task_queue: MasterTaskQueue | None = None) -> None:
    buffer = bytearray()
    write_lock = threading.Lock()
    session_stop = threading.Event()
    current_worker_identity = None
    current_worker_thread = None
    if task_queue is None:
        task_queue = MasterTaskQueue()

    try:
        conn.settimeout(1.0)
        idle_reads = 0
        while not session_stop.is_set():
            try:
                messages = recv_msgs(conn, buffer)
            except OSError:
                if current_worker_identity is not None:
                    registry.mark_worker_unavailable(current_worker_identity["node_id"], reason="socket error")
                    if task_queue is not None:
                        task_queue.handle_worker_unavailable(current_worker_identity["node_id"], reason="socket error")
                break

            if messages is None:
                if current_worker_identity is not None:
                    registry.mark_worker_unavailable(current_worker_identity["node_id"], reason="socket closed")
                    if task_queue is not None:
                        task_queue.handle_worker_unavailable(current_worker_identity["node_id"], reason="socket closed")
                break

            if not messages:
                continue

            idle_reads = 0

            for message in messages:
                if not validate_message(message):
                    reason = classify_protocol_error(message)
                    log_event("ASD", message.get("source", {}).get("node_label", "unknown") if isinstance(message, dict) and isinstance(message.get("source"), dict) else "unknown", "master", "protocol_error", message.get("request_id", "unknown") if isinstance(message, dict) else "unknown", reason)
                    if isinstance(message, dict):
                        with write_lock:
                            send_msg(conn, build_protocol_error_message(message, master_identity, reason))
                    continue

                msg_type = message["type"]
                request_id = message["request_id"]
                source = message["source"]
                cached_response = registry.get_processed_response(message)
                if cached_response is not None:
                    with write_lock:
                        send_msg(conn, cached_response)
                    log_event("ASD", source["node_label"], "master", msg_type, request_id, "deduplicated")
                    continue

                if msg_type == "register_worker":
                    payload = message.get("payload", {})
                    if not isinstance(payload, dict):
                        log_event("ASD", source["node_label"], "master", msg_type, request_id, "discarded")
                        with write_lock:
                            send_msg(conn, build_ack_message(message, "rejected", master_identity, reason="invalid payload"))
                        continue

                    ok, reason = validate_register_worker_payload(payload)
                    if not ok:
                        log_event("ASD", source["node_label"], "master", msg_type, request_id, "rejected")
                        with write_lock:
                            send_msg(conn, build_ack_message(message, "rejected", master_identity, reason=reason or "invalid payload"))
                        continue

                    home_master = payload.get("home_master")
                    if not isinstance(home_master, dict):
                        reason = "home_master invalid"
                        log_event("ASD", source["node_label"], "master", msg_type, request_id, "rejected")
                        with write_lock:
                            send_msg(conn, build_ack_message(message, "rejected", master_identity, reason=reason))
                        continue

                    if source.get("group_label") != master_identity.get("group_label"):
                        reason = "group_label mismatch"
                        log_event("ASD", source["node_label"], "master", msg_type, request_id, "rejected")
                        with write_lock:
                            send_msg(conn, build_ack_message(message, "rejected", master_identity, reason=reason))
                        continue

                    if home_master.get("node_id") != master_identity.get("node_id") or home_master.get("node_label") != master_identity.get("node_label"):
                        reason = "home_master mismatch"
                        log_event("ASD", source["node_label"], "master", msg_type, request_id, "rejected")
                        with write_lock:
                            send_msg(conn, build_ack_message(message, "rejected", master_identity, reason=reason))
                        continue

                    worker_record = {
                        "node_id": source["node_id"],
                        "node_label": source["node_label"],
                        "group_label": source["group_label"],
                        "node_role": source["node_role"],
                        "host": payload.get("host", "127.0.0.1"),
                        "port": payload.get("port", 0),
                        "assignment_epoch": int(payload.get("assignment_epoch", 0)),
                    }
                    try:
                        registry.upsert_worker(worker_record)
                    except ValueError as exc:
                        reason = str(exc)
                        log_event("ASD", source["node_label"], "master", msg_type, request_id, "rejected")
                        with write_lock:
                            send_msg(conn, build_ack_message(message, "rejected", master_identity, reason=reason))
                        continue

                    current_worker_identity = worker_record
                    log_event("ASD", source["node_label"], "master", msg_type, request_id, "registered")
                    ack = build_ack_message(message, "accepted", master_identity)
                    registry.remember_processed_message(message, ack)
                    with write_lock:
                        send_msg(conn, ack)
                    log_event("ASD", "master", source["node_label"], ack["type"], request_id, "sent")

                    if current_worker_thread is None:
                        current_worker_thread, session_stop = start_master_heartbeat_loop(conn, registry, master_identity, current_worker_identity, write_lock, task_queue=task_queue)

                elif msg_type == "heartbeat":
                    ack = build_heartbeat_ack_message(message, source=master_identity, destination=source)
                    registry.remember_processed_message(message, ack)
                    with write_lock:
                        send_msg(conn, ack)
                    log_event("ASD", "master", source["node_label"], "heartbeat_ack", request_id, "sent")

                elif msg_type == "heartbeat_ack":
                    payload = message.get("payload", {}) if isinstance(message.get("payload", {}), dict) else {}
                    sequence = payload.get("sequence")
                    status = payload.get("status")
                    worker = registry.get_worker(source["node_id"])
                    if worker is None:
                        log_event("ASD", source["node_label"], "master", msg_type, request_id, "ignored")
                        continue

                    valid = registry.process_heartbeat_ack(source["node_id"], request_id, sequence, status)
                    if valid:
                        log_event("ASD", source["node_label"], "master", msg_type, request_id, "received")
                    else:
                        log_event("ASD", source["node_label"], "master", msg_type, request_id, "invalid")
                        if worker.get("status") == "unavailable":
                            if task_queue is not None:
                                task_queue.handle_worker_unavailable(source["node_id"], reason="heartbeat timeout")
                            session_stop.set()
                            try:
                                conn.shutdown(socket.SHUT_RDWR)
                            except OSError:
                                pass
                            try:
                                conn.close()
                            except OSError:
                                pass
                            return

                elif msg_type == "task_submit":
                    payload = message.get("payload", {})
                    if not isinstance(payload, dict) or not validate_task_submit_payload(payload):
                        reason = "invalid task_submit payload"
                        log_event("ASD", source["node_label"], "master", msg_type, request_id, "rejected")
                        with write_lock:
                            send_msg(conn, build_protocol_error_message(message, master_identity, reason))
                        continue

                    decision = task_queue.submit(payload)
                    ack = build_task_submit_ack_message(
                        message,
                        source={
                            "group_label": str(master_identity["group_label"]),
                            "node_id": str(master_identity["node_id"]),
                            "node_label": str(master_identity["node_label"]),
                            "node_role": str(master_identity["node_role"]),
                        },
                        destination={
                            "group_label": str(source["group_label"]),
                            "node_id": str(source["node_id"]),
                            "node_label": str(source["node_label"]),
                            "node_role": str(source["node_role"]),
                        },
                        task_id=str(decision["task_id"]),
                        task_label=str(decision["task_label"]),
                        accepted=bool(decision["accepted"]),
                        reason=str(decision["reason"]),
                    )
                    registry.remember_processed_message(message, ack)
                    with write_lock:
                        send_msg(conn, ack)
                    log_event("ASD", "master", source["node_label"], "task_submit_ack", request_id, "sent")

                elif msg_type == "task_result":
                    payload = message.get("payload", {}) if isinstance(message.get("payload", {}), dict) else {}
                    if not isinstance(payload, dict) or not validate_task_result_payload(payload):
                        log_event("ASD", source["node_label"], "master", msg_type, request_id, "rejected")
                        with write_lock:
                            send_msg(conn, build_protocol_error_message(message, master_identity, "invalid task_result payload"))
                        continue

                    task_id = str(payload.get("task_id"))
                    attempt_id = str(payload.get("attempt_id"))
                    accepted, reason = task_queue.complete_task(task_id, attempt_id, accepted=True, reason="completed", payload=payload)
                    ack = build_task_ack_message(
                        message,
                        source={
                            "group_label": str(master_identity["group_label"]),
                            "node_id": str(master_identity["node_id"]),
                            "node_label": str(master_identity["node_label"]),
                            "node_role": str(master_identity["node_role"]),
                        },
                        destination={
                            "group_label": str(source["group_label"]),
                            "node_id": str(source["node_id"]),
                            "node_label": str(source["node_label"]),
                            "node_role": str(source["node_role"]),
                        },
                        task_id=task_id,
                        attempt_id=attempt_id,
                        accepted=bool(accepted),
                        reason=str(reason),
                    )
                    registry.remember_processed_message(message, ack)
                    with write_lock:
                        send_msg(conn, ack)
                    log_event("ASD", "master", source["node_label"], "task_ack", request_id, "sent")

                elif msg_type == "task_request":
                    payload = message.get("payload", {}) if isinstance(message.get("payload", {}), dict) else {}
                    if not isinstance(payload, dict) or not validate_task_request_payload(payload):
                        log_event("ASD", source["node_label"], "master", msg_type, request_id, "rejected")
                        with write_lock:
                            send_msg(conn, build_protocol_error_message(message, master_identity, "invalid task_request payload"))
                        continue

                    worker = registry.get_worker(source["node_id"])
                    if worker is None or worker.get("status") not in {"available", "suspect"}:
                        no_task = build_no_task_message(message, source={
                            "group_label": str(master_identity["group_label"]),
                            "node_id": str(master_identity["node_id"]),
                            "node_label": str(master_identity["node_label"]),
                            "node_role": str(master_identity["node_role"]),
                        }, destination={
                            "group_label": str(source["group_label"]),
                            "node_id": str(source["node_id"]),
                            "node_label": str(source["node_label"]),
                            "node_role": str(source["node_role"]),
                        }, retry_after_ms=1000)
                        with write_lock:
                            send_msg(conn, no_task)
                        log_event("ASD", "master", source["node_label"], "no_task", request_id, "sent")
                        continue

                    assignment = task_queue.claim_next_task(source["node_id"], assignment_epoch=int(payload.get("assignment_epoch", 0)), loan_id=payload.get("loan_id"))
                    if assignment is None:
                        no_task = build_no_task_message(message, source={
                            "group_label": str(master_identity["group_label"]),
                            "node_id": str(master_identity["node_id"]),
                            "node_label": str(master_identity["node_label"]),
                            "node_role": str(master_identity["node_role"]),
                        }, destination={
                            "group_label": str(source["group_label"]),
                            "node_id": str(source["node_id"]),
                            "node_label": str(source["node_label"]),
                            "node_role": str(source["node_role"]),
                        }, retry_after_ms=1000)
                        with write_lock:
                            send_msg(conn, no_task)
                        log_event("ASD", "master", source["node_label"], "no_task", request_id, "sent")
                        continue

                    assignment_message = build_task_assignment_message(
                        message,
                        source={
                            "group_label": str(master_identity["group_label"]),
                            "node_id": str(master_identity["node_id"]),
                            "node_label": str(master_identity["node_label"]),
                            "node_role": str(master_identity["node_role"]),
                        },
                        destination={
                            "group_label": str(source["group_label"]),
                            "node_id": str(source["node_id"]),
                            "node_label": str(source["node_label"]),
                            "node_role": str(source["node_role"]),
                        },
                        task_id=str(assignment["task_id"]),
                        task_label=str(assignment["task_label"]),
                        attempt_id=str(assignment["attempt_id"]),
                        task_kind=str(assignment["task_kind"]),
                        parameters=dict(assignment["parameters"]),
                        assignment_epoch=int(assignment["assignment_epoch"]),
                        loan_id=assignment["loan_id"],
                    )
                    registry.remember_processed_message(message, assignment_message)
                    with write_lock:
                        send_msg(conn, assignment_message)
                    log_event("ASD", "master", source["node_label"], "task_assignment", request_id, "sent")

                else:
                    log_event("ASD", source["node_label"], "master", msg_type, request_id, "ignored")
    except ConnectionResetError:
        if current_worker_identity is not None:
            registry.mark_worker_unavailable(current_worker_identity["node_id"], reason="connection reset")
            if task_queue is not None:
                task_queue.handle_worker_unavailable(current_worker_identity["node_id"], reason="connection reset")
    except BrokenPipeError:
        if current_worker_identity is not None:
            registry.mark_worker_unavailable(current_worker_identity["node_id"], reason="broken pipe")
            if task_queue is not None:
                task_queue.handle_worker_unavailable(current_worker_identity["node_id"], reason="broken pipe")
    except OSError:
        if current_worker_identity is not None:
            registry.mark_worker_unavailable(current_worker_identity["node_id"], reason="socket error")
            if task_queue is not None:
                task_queue.handle_worker_unavailable(current_worker_identity["node_id"], reason="socket error")
    finally:
        session_stop.set()
        try:
            conn.shutdown(socket.SHUT_RDWR)
        except OSError:
            pass
        try:
            conn.close()
        except OSError:
            pass


class MasterTaskQueue:
    def __init__(self, *, task_timeout_ms: int = 30000, max_task_attempts: int = 3):
        self._lock = threading.RLock()
        self._tasks: Dict[str, Dict[str, object]] = {}
        self._order: list[str] = []
        self._next_label_index = 1
        self.task_timeout_ms = int(task_timeout_ms)
        self.max_task_attempts = int(max_task_attempts)

    def _next_task_label(self) -> str:
        label = f"task-{self._next_label_index:04d}"
        self._next_label_index += 1
        return label

    def _canonical_task(self, task: Dict[str, object]) -> str:
        return json.dumps(task, sort_keys=True, separators=(",", ":"), ensure_ascii=False)

    def submit(self, task: Dict[str, object]) -> Dict[str, object]:
        if not isinstance(task, dict):
            raise ValueError("task payload must be a dict")

        task_id = str(task.get("task_id", "")).strip()
        if not task_id:
            raise ValueError("task_id is required")

        payload_signature = self._canonical_task(task)
        with self._lock:
            existing = self._tasks.get(task_id)
            if existing is not None:
                if existing["payload_signature"] == payload_signature:
                    return dict(existing["decision"])
                return {
                    "task_id": task_id,
                    "task_label": existing["task_label"],
                    "accepted": False,
                    "reason": "task_id conflict",
                }

            task_label = self._next_task_label()
            decision = {
                "task_id": task_id,
                "task_label": task_label,
                "accepted": True,
                "reason": "queued",
            }
            record = {
                "task_id": task_id,
                "task_kind": task.get("task_kind"),
                "parameters": task.get("parameters", {}),
                "task_label": task_label,
                "status": "pending",
                "accepted": True,
                "reason": "queued",
                "payload_signature": payload_signature,
                "attempts": {},
                "attempt_count": 0,
                "last_attempt_id": None,
                "last_assignment_epoch": None,
                "assigned_to": None,
                "loan_id": None,
                "running_since_ms": None,
                "task_timeout_ms": int(self.task_timeout_ms),
                "max_task_attempts": int(self.max_task_attempts),
                "_reusable_attempt_id": None,
                "decision": dict(decision),
            }
            self._tasks[task_id] = record
            self._order.append(task_id)
            return dict(decision)

    def pending(self) -> list[Dict[str, object]]:
        with self._lock:
            return [dict(self._tasks[task_id]) for task_id in self._order]

    def get_task(self, task_id: str) -> Dict[str, object] | None:
        with self._lock:
            record = self._tasks.get(task_id)
            if record is None:
                return None
            return dict(record)

    def register_attempt(self, task_id: str, attempt_id: str, metadata: Dict[str, object] | None = None) -> Dict[str, object]:
        with self._lock:
            record = self._tasks.get(task_id)
            if record is None:
                raise KeyError(f"unknown task_id: {task_id}")
            attempt_record = {"attempt_id": attempt_id, "status": "pending"}
            if metadata:
                attempt_record.update(metadata)
            record["attempts"][attempt_id] = attempt_record
            return dict(attempt_record)

    def mark_task_timed_out(self, task_id: str, *, reason: str = "timeout", now_ms: int | None = None) -> bool:
        if not isinstance(task_id, str) or not task_id.strip():
            return False

        with self._lock:
            record = self._tasks.get(task_id)
            if record is None:
                return False

            now_value = int(time.time() * 1000) if now_ms is None else int(now_ms)
            attempt_id = record.get("active_attempt_id") or record.get("last_attempt_id")
            if attempt_id is not None and attempt_id in record.get("attempts", {}):
                record["attempts"][attempt_id]["status"] = "timed_out"
                record["attempts"][attempt_id]["timed_out_reason"] = str(reason)

            if record.get("attempt_count", 0) >= int(record.get("max_task_attempts", self.max_task_attempts)):
                record["status"] = "failed"
                record["failed_reason"] = "max_task_attempts_exceeded"
                record["failed_at_ms"] = now_value
                record["assigned_to"] = None
                record["loan_id"] = None
                record["active_attempt_id"] = None
                record["running_since_ms"] = None
                record["_reusable_attempt_id"] = None
                return True

            record["status"] = "pending"
            record["assigned_to"] = None
            record["loan_id"] = None
            record["active_attempt_id"] = None
            record["running_since_ms"] = None
            record["failure_reason"] = str(reason)
            record["_reusable_attempt_id"] = None
            return True

    def reap_timed_out_tasks(self, *, now_ms: int | None = None) -> list[str]:
        timestamp = int(time.time() * 1000) if now_ms is None else int(now_ms)
        expired: list[str] = []
        with self._lock:
            for task_id in self._order:
                record = self._tasks.get(task_id)
                if record is None:
                    continue
                if record.get("status") != "running":
                    continue
                running_since_ms = record.get("running_since_ms")
                if running_since_ms is None:
                    continue
                timeout_ms = int(record.get("task_timeout_ms", self.task_timeout_ms))
                if timestamp - int(running_since_ms) >= timeout_ms:
                    self.mark_task_timed_out(task_id, reason="timeout", now_ms=timestamp)
                    expired.append(task_id)
        return expired

    def handle_worker_unavailable(self, worker_id: str, *, reason: str = "worker_unavailable", now_ms: int | None = None) -> list[str]:
        if not isinstance(worker_id, str) or not worker_id.strip():
            raise ValueError("worker_id is required")

        timestamp = int(time.time() * 1000) if now_ms is None else int(now_ms)
        affected: list[str] = []
        with self._lock:
            for task_id in self._order:
                record = self._tasks.get(task_id)
                if record is None:
                    continue
                if record.get("status") != "running":
                    continue
                if str(record.get("assigned_to", "")) != str(worker_id):
                    continue
                active_id = record.get("active_attempt_id") or record.get("last_attempt_id")
                if active_id is not None and active_id in record.get("attempts", {}):
                    record["attempts"][active_id]["status"] = "failed"
                    record["attempts"][active_id]["failed_reason"] = str(reason)
                if record.get("attempt_count", 0) >= int(record.get("max_task_attempts", self.max_task_attempts)):
                    record["status"] = "failed"
                    record["failed_reason"] = "max_task_attempts_exceeded"
                    record["failed_at_ms"] = timestamp
                    record["assigned_to"] = None
                    record["loan_id"] = None
                    record["active_attempt_id"] = None
                    record["running_since_ms"] = None
                    record["_reusable_attempt_id"] = None
                else:
                    record["status"] = "pending"
                    record["assigned_to"] = None
                    record["loan_id"] = None
                    record["active_attempt_id"] = None
                    record["running_since_ms"] = None
                    record["failure_reason"] = str(reason)
                    record["_reusable_attempt_id"] = None
                affected.append(task_id)
        return affected

    def invalidate_task_for_worker(self, worker_id: str, *, reason: str = "worker_unavailable", now_ms: int | None = None) -> list[str]:
        return self.handle_worker_unavailable(worker_id, reason=reason, now_ms=now_ms)

    def claim_next_task(self, worker_id: str, *, assignment_epoch: int, loan_id: str | None = None) -> Dict[str, object] | None:
        if not isinstance(worker_id, str) or not worker_id.strip():
            raise ValueError("worker_id is required")
        if not isinstance(assignment_epoch, int) or isinstance(assignment_epoch, bool) or assignment_epoch < 0:
            raise ValueError("assignment_epoch must be a non-negative integer")
        if loan_id is not None and (not isinstance(loan_id, str) or not loan_id.strip()):
            raise ValueError("loan_id must be null or a non-empty string")

        with self._lock:
            for task_id in self._order:
                record = self._tasks.get(task_id)
                if record is None:
                    continue

                if record.get("status") in {"running", "assigned", "completed", "failed"}:
                    continue

                if record.get("status") == "pending" and int(record.get("attempt_count", 0)) >= int(record.get("max_task_attempts", self.max_task_attempts)):
                    record["status"] = "failed"
                    record["failed_reason"] = "max_task_attempts_exceeded"
                    record["failed_at_ms"] = int(time.time() * 1000)
                    continue

                reusable_attempt_id = record.get("_reusable_attempt_id")
                if reusable_attempt_id is not None and reusable_attempt_id in record.get("attempts", {}):
                    attempt_id = str(reusable_attempt_id)
                    record["_reusable_attempt_id"] = None
                else:
                    attempt_id = f"attempt-{uuid.uuid4()}"
                    record["attempt_count"] = int(record.get("attempt_count", 0)) + 1
                    record["last_attempt_id"] = attempt_id

                record["status"] = "running"
                record["assigned_to"] = str(worker_id)
                record["assignment_epoch"] = int(assignment_epoch)
                record["last_assignment_epoch"] = int(assignment_epoch)
                record["loan_id"] = loan_id
                record["running_since_ms"] = int(time.time() * 1000)
                record["active_attempt_id"] = attempt_id
                record["attempts"][attempt_id] = {
                    "attempt_id": attempt_id,
                    "status": "running",
                    "worker_id": str(worker_id),
                    "assignment_epoch": int(assignment_epoch),
                    "loan_id": loan_id,
                    "assigned_at_ms": record["running_since_ms"],
                }
                record["last_attempt_id"] = attempt_id

                return {
                    "task_id": str(record["task_id"]),
                    "task_label": str(record["task_label"]),
                    "attempt_id": attempt_id,
                    "task_kind": record.get("task_kind"),
                    "parameters": dict(record.get("parameters", {})),
                    "assignment_epoch": int(assignment_epoch),
                    "loan_id": loan_id,
                }

            return None

    def release_task(self, task_id: str, *, reason: str | None = None, attempt_id: str | None = None) -> bool:
        if not isinstance(task_id, str) or not task_id.strip():
            return False

        with self._lock:
            record = self._tasks.get(task_id)
            if record is None:
                return False

            if attempt_id is not None:
                record["_reusable_attempt_id"] = str(attempt_id)
            elif reason == "retransmit" and record.get("last_attempt_id") is not None:
                record["_reusable_attempt_id"] = str(record["last_attempt_id"])
            else:
                record["_reusable_attempt_id"] = None

            record["status"] = "pending"
            record["assigned_to"] = None
            record["loan_id"] = None
            record["active_attempt_id"] = None
            record["running_since_ms"] = None
            record["release_reason"] = reason or "released"
            if attempt_id is not None and attempt_id in record.get("attempts", {}):
                record["attempts"][attempt_id]["status"] = "pending"
            elif record.get("last_attempt_id") is not None and record["last_attempt_id"] in record.get("attempts", {}):
                record["attempts"][record["last_attempt_id"]]["status"] = "pending"
            return True

    def complete_task(self, task_id: str, attempt_id: str, *, accepted: bool, reason: str, payload: Dict[str, object] | None = None) -> tuple[bool, str]:
        if not isinstance(task_id, str) or not task_id.strip():
            return False, "invalid task_id"
        if not isinstance(attempt_id, str) or not attempt_id.strip():
            return False, "invalid attempt_id"

        with self._lock:
            record = self._tasks.get(task_id)
            if record is None:
                return False, "unknown task"

            attempts = record.get("attempts", {})
            current_active = record.get("active_attempt_id")
            if attempt_id not in attempts:
                return False, "stale_attempt"

            if record.get("status") == "completed":
                last_attempt = record.get("last_attempt_id")
                if str(last_attempt) == str(attempt_id):
                    return True, "duplicate result"
                return False, "stale_attempt"

            if record.get("status") == "failed":
                return False, "stale_attempt"

            if current_active is not None and str(current_active) != str(attempt_id):
                return False, "stale_attempt"

            if accepted:
                record["status"] = "completed"
                record["completed"] = True
                record["completed_at"] = int(time.time() * 1000)
                record["result_status"] = "ok"
                record["result_payload"] = payload or {}
                record["assigned_to"] = None
                record["loan_id"] = None
                record["active_attempt_id"] = None
                record["running_since_ms"] = None
                record["attempts"][attempt_id]["status"] = "completed"
                return True, reason or "completed"

            record["status"] = "failed"
            record["completed"] = False
            record["result_status"] = "rejected"
            record["result_payload"] = payload or {}
            record["assigned_to"] = None
            record["loan_id"] = None
            record["active_attempt_id"] = None
            record["running_since_ms"] = None
            record["attempts"][attempt_id]["status"] = "failed"
            return False, reason or "rejected"

    def __len__(self) -> int:
        with self._lock:
            return len(self._tasks)


def run_server(host: str, port: int, master_identity: Dict[str, object]) -> None:
    registry = MasterRegistry()
    task_queue = MasterTaskQueue()
    reaper_stop = threading.Event()
    start_task_reaper_loop(task_queue, interval_seconds=0.25, stop_event=reaper_stop)
    server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    server.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    server.bind((host, port))
    server.listen(5)
    log_event("ASD", "master", "network", "server_start", "-", f"listening on {host}:{port}")

    try:
        while True:
            conn, addr = server.accept()
            threading.Thread(target=handle_client, args=(conn, addr, registry, master_identity, task_queue), daemon=True).start()
    except KeyboardInterrupt:
        pass
    finally:
        reaper_stop.set()
        try:
            server.close()
        except OSError:
            pass


def main() -> None:
    parser = argparse.ArgumentParser(description="Master node for NDJSON registration protocol")
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--port", type=int, default=9000)
    args = parser.parse_args()

    config = load_or_create_config(CONFIG_PATH)
    host = args.host if args.host != "127.0.0.1" else str(config.get("host", "127.0.0.1"))
    port = args.port if args.port != 9000 else int(config.get("port", 9000))
    run_server(host, port, config)


if __name__ == "__main__":
    main()
