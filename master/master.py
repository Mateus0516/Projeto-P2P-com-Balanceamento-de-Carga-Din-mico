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
    build_command_release_message,
    build_command_redirect_message,
    build_confirm_loan_message,
    build_get_loan_status_message,
    build_heartbeat_ack_message,
    build_heartbeat_message,
    build_loan_committed_message,
    build_loan_status_message,
    build_metrics_response_message,
    build_notify_worker_returned_message,
    build_performance_report_message,
    build_no_task_message,
    build_protocol_error_message,
    build_request_help_message,
    build_response_accepted_message,
    build_response_rejected_message,
    build_return_ack_message,
    build_task_ack_message as protocol_build_task_ack_message,
    build_task_assignment_message,
    build_task_submit_ack_message,
    build_cancel_loan_message,
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
    normalized["peers"] = normalized.get("peers", []) if isinstance(normalized.get("peers", []), list) else []
    normalized["min_home_workers"] = int(normalized.get("min_home_workers", max(0, int(normalized.get("release_threshold", 1)) - 1)))
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
        "accepted": status == "accepted",
        "worker": worker_identity,
        "host": payload.get("host"),
        "port": payload.get("port"),
        "assignment_epoch": int(payload.get("assignment_epoch", 1)),
        "reason": reason,
    }
    return {
        "version": "sd_2026_2_v1",
        "type": "registration_ack",
        "message_id": str(uuid.uuid4()),
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
    return protocol_build_task_ack_message(
        request_message,
        source=source,
        destination=destination,
        task_id=task_id,
        attempt_id=attempt_id,
        accepted=accepted,
        reason=reason,
    )


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
            if not registry.is_active_session(worker_identity["node_id"], conn):
                break
            sequence += 1
            request_id = str(uuid.uuid4())
            try:
                with registry._lock:
                    if not registry.is_active_session(worker_identity["node_id"], conn):
                        break
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
                with registry._lock:
                    if registry.mark_session_unavailable(worker_identity["node_id"], conn, reason="socket closed"):
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


def start_master_snapshot_loop(task_queue: "MasterTaskQueue", *, interval_seconds: float = 1.0, stop_event: threading.Event | None = None):
    if stop_event is None:
        stop_event = threading.Event()
    if interval_seconds <= 0:
        return None

    def loop() -> None:
        while not stop_event.is_set():
            if stop_event.wait(interval_seconds):
                break
            snapshot = task_queue.snapshot()
            print(
                "MASTER SNAPSHOT "
                f"total={snapshot['total_tasks']} "
                f"completed={snapshot['completed']} "
                f"pending={snapshot['pending']} "
                f"running={snapshot['running']} "
                f"failed={snapshot['failed']} "
                f"timeout_attempts={snapshot['timeout_attempts']} "
                f"recovered_after_timeout={snapshot['recovered_after_timeout']} "
                f"tasks_with_multiple_attempts={snapshot['tasks_with_multiple_attempts']} "
                f"pending_ids={snapshot['pending_task_ids']} "
                f"running_ids={snapshot['running_task_ids']} "
                f"failed_ids={snapshot['failed_task_ids']}"
            )

    thread = threading.Thread(target=loop, daemon=True)
    thread.start()
    return thread


class PeerLoanManager:
    VALID_STATES = {"offered", "committed", "closed", "cancelled", "expired"}

    def __init__(self, registry: MasterRegistry, master_identity: Dict[str, object], *, ledger_path: str | Path | None = None, reservation_timeout_ms: int = 15000, task_queue=None):
        self.registry = registry
        self.task_queue = task_queue
        self.master_identity = dict(master_identity)
        self.reservation_timeout_ms = int(reservation_timeout_ms)
        self._lock = threading.RLock()
        self._loans: Dict[str, Dict[str, object]] = {}
        self._worker_to_loan: Dict[str, str] = {}
        master_label = str(self.master_identity.get("node_label", "master"))
        base_path = (
            Path(ledger_path)
            if ledger_path is not None
            else ROOT / "master" / f"loan_ledger_{master_label}.json"
        )
        self.ledger_path = Path(base_path)
        self.ledger_path.parent.mkdir(parents=True, exist_ok=True)
        self._load_ledger()
        self._refresh_worker_loan_index()

    def _refresh_worker_loan_index(self) -> None:
        stale_ids: list[str] = []
        for worker_id, loan_id in list(self._worker_to_loan.items()):
            worker = self.registry.get_worker(worker_id)
            loan = self._loans.get(str(loan_id)) if isinstance(loan_id, str) else None
            if worker is None:
                stale_ids.append(str(worker_id))
                continue
            if worker.get("status") == "available":
                stale_ids.append(str(worker_id))
                continue
            if loan is not None and str(loan.get("loan_state", "")) not in {"offered", "committed"}:
                stale_ids.append(str(worker_id))
                continue
            if loan is None:
                stale_ids.append(str(worker_id))
        for worker_id in stale_ids:
            self._worker_to_loan.pop(str(worker_id), None)
            worker = self.registry.get_worker(worker_id)
            if isinstance(worker, dict):
                worker.pop("loan_id", None)

    def _worker_snapshot(self) -> list[dict]:
        with self.registry._lock:
            self._refresh_worker_loan_index()
            return [dict(worker) for worker in self.registry.workers.values() if isinstance(worker, dict)]

    def _get_home_reserve(self) -> int:
        reserve = int(self.master_identity.get("min_home_workers", max(0, int(self.master_identity.get("release_threshold", 1)) - 1)))
        return max(0, reserve)

    def _can_offer_worker(self, workers_needed: int, tasks_pending: int) -> tuple[bool, str | None]:
        workers = self._worker_snapshot()
        available_workers = [
            worker for worker in workers
            if str(worker.get("node_role", "")).lower() == "worker"
            and worker.get("status") == "available"
            and str(worker.get("node_id")) not in self._worker_to_loan
        ]
        if not available_workers:
            return False, "no_workers_available"
        if len(available_workers) < int(workers_needed):
            return False, "no_workers_available"
        if len(available_workers) - int(workers_needed) < self._get_home_reserve():
            return False, "min_home_workers"
        # A carga informada em request_help pertence ao solicitante.
        # Nunca usar esse numero para classificar a carga do emprestador.
        local_queue = getattr(self, "task_queue", None)
        if local_queue is not None:
            local_pending = int(local_queue.monitor_pending_count()) if hasattr(local_queue, "monitor_pending_count") else len(local_queue.pending())
            if local_pending > int(self.master_identity.get("saturation_threshold", 10)):
                return False, "high_load"
        return True, None

    def _reserve_worker(self, worker_id: str, loan_id: str) -> dict | None:
        with self.registry._lock:
            worker = self.registry.get_worker(worker_id)
            if worker is None:
                return None
            if worker.get("status") != "available":
                return None
            worker["status"] = "reserved"
            worker["loan_id"] = str(loan_id)
            self._worker_to_loan[str(worker_id)] = str(loan_id)
            return dict(worker)

    def _release_worker(self, worker_id: str) -> None:
        with self.registry._lock:
            worker = self.registry.get_worker(worker_id)
            if worker is not None:
                if worker.get("status") == "reserved":
                    worker["status"] = "available"
                if worker.get("loan_id") == self._worker_to_loan.get(worker_id):
                    worker.pop("loan_id", None)
            self._worker_to_loan.pop(str(worker_id), None)

    def _load_ledger(self) -> None:
        if not self.ledger_path.exists():
            self._save_ledger()
            return
        try:
            raw = json.loads(self.ledger_path.read_text(encoding="utf-8"))
        except (OSError, ValueError, TypeError):
            self._save_ledger()
            return

        records = raw.get("loans", []) if isinstance(raw, dict) else []
        now_monotonic = time.monotonic()
        for item in records:
            if not isinstance(item, dict):
                continue
            loan_id = item.get("loan_id")
            if not isinstance(loan_id, str):
                continue
            state = str(item.get("loan_state", "unknown"))
            if state == "unknown":
                continue
            reservation_deadline = item.get("reservation_deadline_monotonic")
            if isinstance(reservation_deadline, (int, float)) and now_monotonic > float(reservation_deadline):
                item["loan_state"] = "expired"
                item["closed_at"] = int(time.time() * 1000)
            self._loans[loan_id] = dict(item)
            worker_id = item.get("worker", {}).get("node_id") if isinstance(item.get("worker"), dict) else None
            if worker_id and item.get("loan_state") in {"offered", "committed"}:
                self._worker_to_loan[str(worker_id)] = str(loan_id)

    def _save_ledger(self) -> None:
        payload = {"loans": [dict(record) for record in self._loans.values()]}
        temp_path = self.ledger_path.with_suffix(".tmp")
        temp_path.write_text(json.dumps(payload, indent=2, sort_keys=True), encoding="utf-8")
        temp_path.replace(self.ledger_path)

    def _new_loan_record(self, *, loan_id: str, requester_master: dict, target_master: dict, home_master: dict, worker: dict, assignment_epoch: int, loan_state: str, requester_source: dict | None = None) -> Dict[str, object]:
        now_ms = int(time.time() * 1000)
        deadline = time.monotonic() + (self.reservation_timeout_ms / 1000.0)
        record = {
            "loan_id": str(loan_id),
            "loan_label": f"loan-{str(loan_id)[:8]}",
            "worker": dict(worker),
            "home_master": dict(home_master),
            "target_master": dict(target_master),
            "assignment_epoch": int(assignment_epoch),
            "loan_state": str(loan_state),
            "requester_master": dict(requester_master),
            "created_at": now_ms,
            "reservation_deadline": now_ms + self.reservation_timeout_ms,
            "reservation_deadline_monotonic": deadline,
            "committed_at": None,
            "closed_at": None,
            "requester_source": dict(requester_source) if isinstance(requester_source, dict) else None,
        }
        self._loans[str(loan_id)] = record
        self._persist_if_needed()
        return dict(record)

    def _persist_if_needed(self) -> None:
        self._save_ledger()

    def _get_loan_record(self, loan_id: str) -> Dict[str, object] | None:
        if not isinstance(loan_id, str):
            return None
        record = self._loans.get(str(loan_id))
        if record is None:
            return None
        if record.get("loan_state") == "offered" and time.monotonic() > float(record.get("reservation_deadline_monotonic", time.monotonic())):
            record["loan_state"] = "expired"
            record["closed_at"] = int(time.time() * 1000)
            self._release_worker(str(record.get("worker", {}).get("node_id", "")))
            self._persist_if_needed()
        return dict(record)

    def process_request_help(self, payload: dict, *, source: dict | None = None) -> dict:
        if not isinstance(payload, dict):
            return {"status": "rejected", "reason": {"code": "invalid_payload", "message": "request_help payload must be a dict"}, "loan_id": str(uuid.uuid4())}

        requester_master = payload.get("requester_master")
        workers_needed = payload.get("workers_needed")
        tasks_pending = payload.get("tasks_pending")
        if not isinstance(requester_master, dict) or requester_master.get("node_role") != "master":
            return {"status": "rejected", "loan_id": str(uuid.uuid4()), "reason": {"code": "invalid_identity", "message": "requester_master must identify a master"}}
        if not isinstance(workers_needed, int) or isinstance(workers_needed, bool) or workers_needed < 1:
            return {"status": "rejected", "loan_id": str(uuid.uuid4()), "reason": {"code": "invalid_payload", "message": "workers_needed must be a positive integer"}}
        if not isinstance(tasks_pending, int) or isinstance(tasks_pending, bool) or tasks_pending < 0:
            return {"status": "rejected", "loan_id": str(uuid.uuid4()), "reason": {"code": "invalid_payload", "message": "tasks_pending must be a non-negative integer"}}

        allowed, reason_code = self._can_offer_worker(workers_needed, tasks_pending)
        if not allowed:
            return {"status": "rejected", "loan_id": str(uuid.uuid4()), "reason": {"code": reason_code or "refused", "message": self._reason_message(reason_code or "refused")}}

        worker_candidates = [
            worker for worker in self._worker_snapshot()
            if str(worker.get("node_role", "")).lower() == "worker"
            and worker.get("status") == "available"
            and str(worker.get("node_id")) not in self._worker_to_loan
        ]
        if not worker_candidates:
            return {"status": "rejected", "loan_id": str(uuid.uuid4()), "reason": {"code": "no_workers_available", "message": "No available workers matched the request"}}

        selected = dict(worker_candidates[0])
        loan_id = str(uuid.uuid4())
        with self._lock:
            reserved = self._reserve_worker(str(selected["node_id"]), loan_id)
            if reserved is None:
                return {"status": "rejected", "loan_id": str(uuid.uuid4()), "reason": {"code": "no_workers_available", "message": "No workers could be reserved atomically"}}
            record = self._new_loan_record(
                loan_id=loan_id,
                requester_master=dict(requester_master),
                target_master=dict(self.master_identity),
                home_master=dict(self.master_identity),
                worker=dict(selected),
                assignment_epoch=int(int(selected.get("assignment_epoch", 0)) if isinstance(selected.get("assignment_epoch"), int) else 0),
                loan_state="offered",
                requester_source=dict(source) if isinstance(source, dict) else None,
            )
            record["loan_state"] = "offered"
            record["reservation_deadline"] = int(time.time() * 1000) + self.reservation_timeout_ms
            record["reservation_deadline_monotonic"] = time.monotonic() + (self.reservation_timeout_ms / 1000.0)
            self._loans[loan_id] = record
            self._worker_to_loan[str(selected["node_id"])] = loan_id
            self._persist_if_needed()

        authoritative_requester = self._loans.get(str(loan_id), {}).get("requester_master") if isinstance(self._loans.get(str(loan_id)), dict) else None
        stored_requester_master = dict(authoritative_requester or requester_master or {})
        return {
            "status": "accepted",
            "loan_id": loan_id,
            "loan_state": "offered",
            "requester_master": stored_requester_master,
            "target_master": dict(self.master_identity),
            "home_master": dict(self.master_identity),
            "worker": dict(selected),
            "assignment_epoch": int(selected.get("assignment_epoch", 0) if isinstance(selected.get("assignment_epoch"), int) else 0),
            "reservation_timeout_ms": 15000,
            "reason": None,
        }

    def confirm_loan(self, loan_id: str, *, requester_master: dict | None, request_id: str | None = None) -> dict:
        if not isinstance(loan_id, str):
            return {"status": "rejected", "loan_id": str(uuid.uuid4()), "reason": {"code": "invalid_loan_id", "message": "loan_id must be present"}}
        loan = self._get_loan_record(loan_id)
        if loan is None:
            return {"status": "rejected", "loan_id": str(loan_id), "reason": {"code": "unknown_loan", "message": "loan does not exist"}}
        authoritative_requester = loan.get("requester_master") if isinstance(loan.get("requester_master"), dict) else None
        if requester_master is None:
            requester_master = authoritative_requester
        if loan.get("loan_state") == "committed":
            return {"status": "accepted", "loan_id": str(loan_id), "loan_state": "committed", "worker": loan.get("worker"), "requester_master": authoritative_requester or loan.get("requester_master"), "reservation_timeout_ms": 15000, "request_id": request_id}
        if loan.get("loan_state") == "expired":
            return {"status": "rejected", "loan_id": str(loan_id), "reason": {"code": "reservation_expired", "message": "reservation expired"}, "loan_state": "expired"}
        if loan.get("loan_state") != "offered":
            return {"status": "rejected", "loan_id": str(loan_id), "reason": {"code": "invalid_state", "message": "loan is not in offered state"}, "loan_state": loan.get("loan_state", "unknown")}
        if not isinstance(requester_master, dict) or requester_master.get("node_id") != (authoritative_requester or {}).get("node_id"):
            return {"status": "rejected", "loan_id": str(loan_id), "reason": {"code": "identity_conflict", "message": "requester_master does not match the original offer"}, "loan_state": "offered"}

        if time.monotonic() > float(loan.get("reservation_deadline_monotonic", time.monotonic())):
            loan["loan_state"] = "expired"
            loan["closed_at"] = int(time.time() * 1000)
            self._loans[str(loan_id)] = loan
            self._release_worker(str(loan.get("worker", {}).get("node_id", "")))
            self._persist_if_needed()
            return {"status": "rejected", "loan_id": str(loan_id), "reason": {"code": "reservation_expired", "message": "reservation expired"}, "loan_state": "expired"}

        loan["loan_state"] = "committed"
        loan["committed_at"] = int(time.time() * 1000)
        self._loans[str(loan_id)] = loan
        self._persist_if_needed()
        return {
            "status": "accepted",
            "loan_id": str(loan_id),
            "loan_state": "committed",
            "worker": loan.get("worker"),
            "requester_master": loan.get("requester_master"),
            "target_master": loan.get("target_master"),
            "home_master": loan.get("home_master"),
            "assignment_epoch": loan.get("assignment_epoch"),
            "reservation_timeout_ms": 15000,
            "request_id": request_id,
        }

    def get_loan_status(self, loan_id: str) -> dict:
        loan = self._get_loan_record(loan_id)
        if loan is None:
            return {"loan_id": str(loan_id), "loan_state": "unknown"}
        return {
            "loan_id": str(loan_id),
            "loan_state": loan.get("loan_state", "unknown"),
            "requester_master": loan.get("requester_master"),
            "target_master": loan.get("target_master"),
            "home_master": loan.get("home_master"),
            "worker": loan.get("worker"),
            "assignment_epoch": loan.get("assignment_epoch"),
            "reservation_timeout_ms": 15000,
        }

    def cancel_loan(self, loan_id: str, *, reason: str = "cancelled") -> dict:
        loan = self._get_loan_record(loan_id)
        if loan is None:
            return {"status": "rejected", "loan_id": str(loan_id), "reason": {"code": "unknown_loan", "message": "loan does not exist"}}
        if loan.get("loan_state") not in {"offered"}:
            return {"status": "rejected", "loan_id": str(loan_id), "reason": {"code": "invalid_state", "message": "cancel_loan is only allowed for offered loans"}, "loan_state": loan.get("loan_state", "unknown")}
        loan["loan_state"] = "cancelled"
        loan["closed_at"] = int(time.time() * 1000)
        self._loans[str(loan_id)] = loan
        worker_id = str(loan.get("worker", {}).get("node_id", ""))
        self._release_worker(worker_id)
        self._persist_if_needed()
        return {"status": "accepted", "loan_id": str(loan_id), "loan_state": "cancelled", "reason": {"code": "cancelled", "message": str(reason)}}

    def _reason_message(self, reason_code: str) -> str:
        mapping = {
            "high_load": "Target master is saturated",
            "no_workers_available": "No workers available for loan",
            "min_home_workers": "Not enough workers left at home master",
            "refused": "Loan request refused",
            "invalid_payload": "Invalid request_help payload",
            "invalid_identity": "Requester master identity is invalid",
        }
        return mapping.get(reason_code, str(reason_code))

    def issue_redirect_command(self, loan_id: str, *, target_master: dict, reason: str = "redirect to peer", target_host: str | None = None, target_port: int | None = None, conn: socket.socket | None = None) -> dict:
        if not isinstance(loan_id, str) or not loan_id.strip():
            return {"status": "rejected", "reason": {"code": "invalid_loan_id", "message": "loan_id must be a non-empty string"}, "message": None}
        if not isinstance(target_master, dict):
            return {"status": "rejected", "reason": {"code": "invalid_target_master", "message": "target_master must be a valid master identity"}, "message": None}

        loan = self._get_loan_record(loan_id)
        if loan is None:
            return {"status": "rejected", "reason": {"code": "unknown_loan", "message": "loan does not exist"}, "message": None}
        if loan.get("loan_state") != "committed":
            return {"status": "rejected", "reason": {"code": "invalid_state", "message": "only committed loans can be redirected"}, "message": None}

        worker = loan.get("worker") or {}
        worker_id = str(worker.get("node_id") or "")
        if not worker_id:
            return {"status": "rejected", "reason": {"code": "worker_missing", "message": "loan has no associated worker"}, "message": None}
        if str(self._worker_to_loan.get(worker_id)) != str(loan_id):
            return {"status": "rejected", "reason": {"code": "worker_mismatch", "message": "worker is not associated with the expected loan"}, "message": None}

        target_master_identity = dict(target_master)
        if target_master_identity.get("node_role") != "master":
            return {"status": "rejected", "reason": {"code": "invalid_target_master", "message": "target_master must identify a master"}, "message": None}

        destination = {
            "group_label": str(worker.get("group_label")),
            "node_id": str(worker.get("node_id")),
            "node_label": str(worker.get("node_label")),
            "node_role": "worker",
        }
        source_identity = {
            "group_label": str(self.master_identity.get("group_label", "p2p-cluster")),
            "node_id": str(self.master_identity.get("node_id")),
            "node_label": str(self.master_identity.get("node_label")),
            "node_role": str(self.master_identity.get("node_role", "master")),
        }
        resolved_target_host = str(target_host or target_master_identity.get("host") or worker.get("host") or self.master_identity.get("host") or "127.0.0.1")
        resolved_target_port = int(target_port if target_port is not None else int(target_master_identity.get("port") or 9000))
        message = build_command_redirect_message(
            {"request_id": str(uuid.uuid4()), "source": source_identity, "destination": destination},
            source=source_identity,
            destination=destination,
            loan_id=str(loan_id),
            worker=dict(worker),
            target_host=resolved_target_host,
            target_port=resolved_target_port,
            assignment_epoch=int(loan.get("assignment_epoch", 0) or 0),
            reason=str(reason or "redirect to peer"),
            target_master=dict(target_master_identity),
            home_master=dict(self.master_identity),
            home=dict(self.master_identity),
            request_id=str(uuid.uuid4()),
        )
        # A conexao recebida do peer nao e a sessao do Worker.
        # Enviar diretamente pelo socket registrado para o Worker emprestado.
        with self.registry._lock:
            worker_conn = self.registry.active_sessions.get(worker_id)
        if worker_conn is None:
            return {"status": "rejected", "reason": {"code": "worker_disconnected", "message": "worker has no active session"}, "message": message}
        try:
            send_msg(worker_conn, message)
        except (BrokenPipeError, ConnectionResetError, OSError):
            return {"status": "rejected", "reason": {"code": "socket_error", "message": "unable to send command_redirect to worker"}, "message": message}
        loan["redirect_target"] = dict(target_master_identity)
        loan["redirected_at"] = int(time.time() * 1000)
        self._loans[str(loan_id)] = loan
        self._persist_if_needed()
        return {"status": "accepted", "reason": None, "message": message}

    redirect_committed_loan = issue_redirect_command

    def issue_release_command(self, loan_id: str, *, reason: str = "return to home", conn: socket.socket | None = None) -> dict:
        if not isinstance(loan_id, str) or not loan_id.strip():
            return {"status": "rejected", "reason": {"code": "invalid_loan_id", "message": "loan_id must be a non-empty string"}, "message": None}

        loan = self._get_loan_record(loan_id)
        if loan is None:
            return {"status": "rejected", "reason": {"code": "unknown_loan", "message": "loan does not exist"}, "message": None}
        if loan.get("loan_state") != "committed":
            return {"status": "rejected", "reason": {"code": "invalid_state", "message": "only committed loans can be released"}, "message": None}

        worker = loan.get("worker") or {}
        worker_id = str(worker.get("node_id") or "")
        if not worker_id:
            return {"status": "rejected", "reason": {"code": "worker_missing", "message": "loan has no associated worker"}, "message": None}
        if str(self._worker_to_loan.get(worker_id)) != str(loan_id):
            return {"status": "rejected", "reason": {"code": "worker_mismatch", "message": "worker is not associated with the expected loan"}, "message": None}

        message = build_command_release_message(
            {"request_id": str(uuid.uuid4())},
            source={
                "group_label": str(self.master_identity.get("group_label", "p2p-cluster")),
                "node_id": str(self.master_identity.get("node_id")),
                "node_label": str(self.master_identity.get("node_label")),
                "node_role": str(self.master_identity.get("node_role", "master")),
            },
            destination={
                "group_label": str(worker.get("group_label", self.master_identity.get("group_label", "p2p-cluster"))),
                "node_id": str(worker.get("node_id")),
                "node_label": str(worker.get("node_label")),
                "node_role": str(worker.get("node_role", "worker")),
            },
            loan_id=str(loan_id),
            assignment_epoch=int(loan.get("assignment_epoch", 0) or 0),
            reason=str(reason or "return to home"),
            request_id=str(uuid.uuid4()),
        )
        if conn is None:
            with self.registry._lock:
                conn = self.registry.active_sessions.get(worker_id)
        if conn is not None and (hasattr(conn, "sendall") or hasattr(conn, "send")):
            try:
                send_msg(conn, message)
            except (BrokenPipeError, ConnectionResetError, OSError):
                return {"status": "rejected", "reason": {"code": "socket_error", "message": "unable to send command_release"}, "message": message}

        loan["release_requested_at"] = int(time.time() * 1000)
        loan["release_reason"] = str(reason or "return to home")
        self._loans[str(loan_id)] = loan
        self._persist_if_needed()
        return {"status": "accepted", "reason": None, "message": message}

    def process_notify_worker_returned(self, loan_id: str, *, worker: dict, assignment_epoch: int, source: dict | None = None) -> dict:
        if not isinstance(loan_id, str) or not loan_id.strip():
            return {"status": "rejected", "reason": {"code": "invalid_loan_id", "message": "loan_id must be a non-empty string"}, "loan_id": str(loan_id)}
        if not isinstance(worker, dict) or worker.get("node_role") != "worker":
            return {"status": "rejected", "reason": {"code": "invalid_worker", "message": "worker identity is invalid"}, "loan_id": str(loan_id)}
        if not isinstance(assignment_epoch, int) or isinstance(assignment_epoch, bool) or assignment_epoch < 0:
            return {"status": "rejected", "reason": {"code": "invalid_assignment_epoch", "message": "assignment_epoch must be a non-negative integer"}, "loan_id": str(loan_id)}

        loan = self._get_loan_record(loan_id)
        if loan is None:
            return {"status": "rejected", "reason": {"code": "unknown_loan", "message": "loan does not exist"}, "loan_id": str(loan_id)}

        loan_worker = loan.get("worker") or {}
        if str(worker.get("node_id", "")) != str(loan_worker.get("node_id", "")):
            return {"status": "rejected", "reason": {"code": "worker_mismatch", "message": "worker does not match the loan"}, "loan_id": str(loan_id)}
        if int(loan.get("assignment_epoch", 0) or 0) != int(assignment_epoch):
            return {"status": "rejected", "reason": {"code": "stale_epoch", "message": "assignment_epoch does not match the committed loan"}, "loan_id": str(loan_id)}
        if loan.get("loan_state") not in {"committed", "closed"}:
            return {"status": "rejected", "reason": {"code": "invalid_state", "message": "loan must be committed before return confirmation"}, "loan_id": str(loan_id)}

        if loan.get("loan_state") == "closed":
            return {"status": "accepted", "reason": {"code": "duplicate_return", "message": "worker return already confirmed"}, "loan_id": str(loan_id)}

        worker_id = str(worker.get("node_id"))
        loan["loan_state"] = "closed"
        loan["closed_at"] = int(time.time() * 1000)
        loan["return_confirmed_at"] = int(time.time() * 1000)
        loan["return_confirmed_by"] = "origin_master"
        self._loans[str(loan_id)] = loan
        self._worker_to_loan.pop(worker_id, None)
        registry_worker = self.registry.get_worker(worker_id)
        if registry_worker is not None:
            registry_worker["status"] = "available"
            registry_worker.pop("loan_id", None)
        self._persist_if_needed()
        return {"status": "accepted", "reason": None, "loan_id": str(loan_id), "loan_state": "closed"}

    def process_return_ack(self, loan_id: str, *, worker: dict, assignment_epoch: int, status: str = "returned") -> dict:
        if not isinstance(loan_id, str) or not loan_id.strip():
            return {"status": "rejected", "reason": {"code": "invalid_loan_id", "message": "loan_id must be a non-empty string"}, "loan_id": str(loan_id)}
        if not isinstance(worker, dict) or worker.get("node_role") != "worker":
            return {"status": "rejected", "reason": {"code": "invalid_worker", "message": "worker identity is invalid"}, "loan_id": str(loan_id)}
        if not isinstance(assignment_epoch, int) or isinstance(assignment_epoch, bool) or assignment_epoch < 0:
            return {"status": "rejected", "reason": {"code": "invalid_assignment_epoch", "message": "assignment_epoch must be a non-negative integer"}, "loan_id": str(loan_id)}
        if status not in {"returned", "accepted"}:
            return {"status": "rejected", "reason": {"code": "invalid_status", "message": "status must be returned or accepted"}, "loan_id": str(loan_id)}

        loan = self._get_loan_record(loan_id)
        if loan is None:
            return {"status": "rejected", "reason": {"code": "unknown_loan", "message": "loan does not exist"}, "loan_id": str(loan_id)}

        loan_worker = loan.get("worker") or {}
        if str(worker.get("node_id", "")) != str(loan_worker.get("node_id", "")):
            return {"status": "rejected", "reason": {"code": "worker_mismatch", "message": "worker does not match the loan"}, "loan_id": str(loan_id)}
        if int(loan.get("assignment_epoch", 0) or 0) != int(assignment_epoch):
            return {"status": "rejected", "reason": {"code": "stale_epoch", "message": "assignment_epoch does not match the committed loan"}, "loan_id": str(loan_id)}

        if loan.get("loan_state") == "closed":
            return {"status": "accepted", "reason": {"code": "duplicate_return_ack", "message": "return already acknowledged"}, "loan_id": str(loan_id), "loan_state": "closed"}
        if loan.get("loan_state") != "committed":
            return {"status": "rejected", "loan_id": str(loan_id), "reason": {"code": "invalid_state", "message": "loan must be committed before acknowledging return"}, "loan_id": str(loan_id)}

        loan["loan_state"] = "closed"
        loan["closed_at"] = int(time.time() * 1000)
        loan["return_ack_at"] = int(time.time() * 1000)
        loan["return_status"] = str(status)
        self._loans[str(loan_id)] = loan
        self._worker_to_loan.pop(str(worker.get("node_id", "")), None)
        registry_worker = self.registry.get_worker(str(worker.get("node_id", "")))
        if registry_worker is not None:
            registry_worker["status"] = "available"
            registry_worker.pop("loan_id", None)
        self._persist_if_needed()
        return {"status": "accepted", "reason": None, "loan_id": str(loan_id), "loan_state": "closed"}

    return_committed_loan = process_return_ack

    def mirror_committed_loan(
        self,
        *,
        loan_id: str,
        requester_master: dict,
        target_master: dict,
        home_master: dict,
        worker: dict,
        assignment_epoch: int,
    ) -> dict:
        if not isinstance(loan_id, str):
            return {"status": "rejected", "loan_id": str(uuid.uuid4()), "reason": {"code": "invalid_loan_id", "message": "loan_id must be a string"}}
        try:
            parsed = uuid.UUID(loan_id)
        except (ValueError, AttributeError, TypeError):
            return {"status": "rejected", "loan_id": str(uuid.uuid4()), "reason": {"code": "invalid_loan_id", "message": "loan_id must be a UUIDv4"}}
        if parsed.version != 4:
            return {"status": "rejected", "loan_id": str(uuid.uuid4()), "reason": {"code": "invalid_loan_id", "message": "loan_id must be a UUIDv4"}}
        if not isinstance(requester_master, dict) or requester_master.get("node_role") != "master":
            return {"status": "rejected", "loan_id": loan_id, "reason": {"code": "invalid_identity", "message": "requester_master must identify a master"}}
        if not isinstance(target_master, dict) or target_master.get("node_role") != "master":
            return {"status": "rejected", "loan_id": loan_id, "reason": {"code": "invalid_identity", "message": "target_master must identify a master"}}
        if not isinstance(home_master, dict) or home_master.get("node_role") != "master":
            return {"status": "rejected", "loan_id": loan_id, "reason": {"code": "invalid_identity", "message": "home_master must identify a master"}}
        if not isinstance(worker, dict) or worker.get("node_role") != "worker":
            return {"status": "rejected", "loan_id": loan_id, "reason": {"code": "invalid_worker", "message": "worker identity is invalid"}}
        if not isinstance(assignment_epoch, int) or isinstance(assignment_epoch, bool) or assignment_epoch < 0:
            return {"status": "rejected", "loan_id": loan_id, "reason": {"code": "invalid_assignment_epoch", "message": "assignment_epoch must be non-negative"}}

        existing = self._loans.get(loan_id)
        if existing is not None:
            if existing.get("loan_state") == "committed":
                return {"status": "accepted", "loan_id": loan_id, "loan_state": "committed", "duplicate": True}

        record = self._new_loan_record(
            loan_id=loan_id,
            requester_master=dict(requester_master),
            target_master=dict(target_master),
            home_master=dict(home_master),
            worker=dict(worker),
            assignment_epoch=int(assignment_epoch),
            loan_state="committed",
        )
        record["committed_at"] = int(time.time() * 1000)
        record["reservation_deadline_monotonic"] = time.monotonic() + (self.reservation_timeout_ms / 1000.0)
        self._loans[loan_id] = record

        worker_id = str(worker["node_id"])
        try:
            reserved_worker = dict(worker)
            reserved_worker["status"] = "reserved"
            reserved_worker["loan_id"] = loan_id
            reserved_worker["assignment_epoch"] = int(assignment_epoch)
            reserved_worker["home_master"] = dict(home_master)
            self.registry.upsert_worker(reserved_worker)
            self._worker_to_loan[worker_id] = loan_id
        except ValueError:
            return {"status": "rejected", "loan_id": loan_id, "reason": {"code": "identity_conflict", "message": "worker identity conflict"}}

        self._persist_if_needed()
        return {"status": "accepted", "loan_id": loan_id, "loan_state": "committed"}

    def list_committed_loans(self) -> list[dict]:
        with self._lock:
            return [dict(record) for record in self._loans.values() if record.get("loan_state") == "committed"]
def handle_client(conn: socket.socket, addr, registry: MasterRegistry, master_identity: Dict[str, object], task_queue: MasterTaskQueue | None = None, peer_manager: PeerLoanManager | None = None) -> None:
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
            except ValueError as exc:
                log_event(
                    "ASD", "unknown", "master",
                    "invalid_message", "unknown",
                    f"connection closed: {exc}"
                )
                if current_worker_identity is not None:
                    with registry._lock:
                        if registry.is_active_session(current_worker_identity["node_id"], conn):
                            registry.mark_session_unavailable(
                                current_worker_identity["node_id"],
                                conn,
                                reason="NDJSON buffer limit exceeded"
                            )
                            if task_queue is not None:
                                task_queue.handle_worker_unavailable(
                                    current_worker_identity["node_id"],
                                    reason="NDJSON buffer limit exceeded"
                                )
                break
            except OSError:
                if current_worker_identity is not None:
                    with registry._lock:
                        if registry.is_active_session(current_worker_identity["node_id"], conn):
                            registry.mark_session_unavailable(current_worker_identity["node_id"], conn, reason="socket error")
                            if task_queue is not None:
                                task_queue.handle_worker_unavailable(current_worker_identity["node_id"], reason="socket error")
                break

            if messages is None:
                if current_worker_identity is not None:
                    with registry._lock:
                        if registry.is_active_session(current_worker_identity["node_id"], conn):
                            registry.mark_session_unavailable(current_worker_identity["node_id"], conn, reason="socket closed")
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
                    if isinstance(message, dict) and message.get("type") != "protocol_error":
                        sender = message.get("source")
                        if isinstance(sender, dict) and all(
                            isinstance(sender.get(field), str) and sender.get(field)
                            for field in ("group_label", "node_id", "node_label", "node_role")
                        ):
                            response = build_protocol_error_message(message, master_identity, reason)
                            if validate_message(response):
                                with write_lock:
                                    send_msg(conn, response)
                    continue

                msg_type = message["type"]
                request_id = message["request_id"]
                source = message["source"]

                if msg_type in {"heartbeat", "heartbeat_ack", "task_request", "task_result"}:
                    worker_id = source["node_id"]
                    with registry._lock:
                        active_session = registry.active_sessions.get(worker_id)
                        session_allowed = (
                            source.get("node_role") == "worker"
                            and (
                                registry.is_active_session(worker_id, conn)
                                or (
                                    active_session is None
                                    and msg_type in {"heartbeat", "task_request"}
                                )
                            )
                        )
                    if not session_allowed:
                        log_event(
                            "ASD",
                            source["node_label"],
                            "master",
                            msg_type,
                            request_id,
                            "inactive_worker_session",
                        )
                        return

                if registry.has_request_conflict(message):
                    error_response = build_protocol_error_message(
                        message,
                        master_identity,
                        "request_conflict",
                    )
                    error_response["payload"]["error_code"] = "request_conflict"
                    error_response["payload"]["error_message"] = (
                        "request_id reused with different content"
                    )
                    with write_lock:
                        send_msg(conn, error_response)
                    log_event(
                        "ASD", source["node_label"], "master",
                        "protocol_error", request_id, "request_conflict"
                    )
                    continue

                cached_response = registry.get_processed_response(message)
                if cached_response is not None:
                    if msg_type == "register_worker" and cached_response.get("payload", {}).get("accepted") is True:
                        worker = registry.get_worker(source["node_id"])
                        if worker is not None:
                            with registry._lock:
                                worker = dict(worker)
                                worker["status"] = "available"
                                worker["heartbeat_misses"] = 0
                                registry.upsert_worker(worker)
                                previous_conn = registry.bind_session(source["node_id"], conn)
                                current_worker_identity = worker
                            if previous_conn is not None:
                                try:
                                    previous_conn.shutdown(socket.SHUT_RDWR)
                                except OSError:
                                    pass
                                try:
                                    previous_conn.close()
                                except OSError:
                                    pass
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

                    loan_id = payload.get("loan_id")
                    loan = peer_manager._get_loan_record(loan_id) if peer_manager is not None and loan_id else None

                    valid_loan = (
                        loan is not None
                        and loan.get("loan_state") == "committed"
                        and loan.get("assignment_epoch") == payload.get("assignment_epoch")
                        and all(
                            loan.get("worker", {}).get(key) == source.get(key)
                            and loan.get("home_master", {}).get(key) == home_master.get(key)
                            and loan.get("requester_master", {}).get(key) == master_identity.get(key)
                            for key in ("node_id", "node_label", "group_label", "node_role")
                        )
                    )

                    if (home_master.get("node_id") != master_identity.get("node_id") or home_master.get("node_label") != master_identity.get("node_label") ) and not valid_loan:
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

                    previous_conn = registry.bind_session(source["node_id"], conn)
                    current_worker_identity = worker_record
                    if previous_conn is not None:
                        try:
                            previous_conn.shutdown(socket.SHUT_RDWR)
                        except OSError:
                            pass
                        try:
                            previous_conn.close()
                        except OSError:
                            pass
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

                elif msg_type == "request_help":
                    payload = message.get("payload", {}) if isinstance(message.get("payload", {}), dict) else {}
                    if not isinstance(payload, dict) or not validate_message(message):
                        log_event("ASD", source["node_label"], "master", msg_type, request_id, "rejected")
                        with write_lock:
                            send_msg(conn, build_protocol_error_message(message, master_identity, "invalid request_help payload"))
                        continue

                    if peer_manager is None: peer_manager = PeerLoanManager(registry, master_identity, task_queue=task_queue)
                    decision = peer_manager.process_request_help(payload, source=source)
                    message_source = {
                        "group_label": str(master_identity["group_label"]),
                        "node_id": str(master_identity["node_id"]),
                        "node_label": str(master_identity["node_label"]),
                        "node_role": str(master_identity["node_role"]),
                    }
                    if decision["status"] == "accepted":
                        response = build_response_accepted_message(
                            message,
                            source=message_source,
                            destination={
                                "group_label": str(source["group_label"]),
                                "node_id": str(source["node_id"]),
                                "node_label": str(source["node_label"]),
                                "node_role": str(source["node_role"]),
                            },
                            loan_id=str(decision["loan_id"]),
                            requester_master=dict(payload.get("requester_master")),
                            target_master=dict(master_identity),
                            home_master=dict(master_identity),
                            worker=dict(decision["worker"]),
                            assignment_epoch=int(decision.get("assignment_epoch", 0)),
                            reservation_timeout_ms=15000,
                            loan_state="offered",
                        )
                    else:
                        response = build_response_rejected_message(
                            message,
                            source=message_source,
                            destination={
                                "group_label": str(source["group_label"]),
                                "node_id": str(source["node_id"]),
                                "node_label": str(source["node_label"]),
                                "node_role": str(source["node_role"]),
                            },
                            loan_id=str(decision.get("loan_id") or uuid.uuid4()),
                            reason=dict(decision.get("reason", {"code": "refused", "message": "Loan request refused"})),
                        )
                    registry.remember_processed_message(message, response)
                    with write_lock:
                        send_msg(conn, response)
                    log_event("ASD", "master", source["node_label"], msg_type, request_id, decision["status"])

                elif msg_type == "confirm_loan":
                    payload = message.get("payload", {}) if isinstance(message.get("payload", {}), dict) else {}
                    if not isinstance(payload, dict):
                        log_event("ASD", source["node_label"], "master", msg_type, request_id, "rejected")
                        with write_lock:
                            send_msg(conn, build_protocol_error_message(message, master_identity, "invalid confirm_loan payload"))
                        continue
                    if peer_manager is None: peer_manager = PeerLoanManager(registry, master_identity, task_queue=task_queue)
                    decision = peer_manager.confirm_loan(payload.get("loan_id"), requester_master=payload.get("requester_master"), request_id=request_id)
                    if decision["status"] == "accepted":
                        loan = peer_manager.get_loan_status(str(payload.get("loan_id")))
                        response = build_loan_committed_message(
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
                            loan_id=str(payload.get("loan_id")),
                            requester_master=dict(payload.get("requester_master")),
                            target_master=dict(master_identity),
                            home_master=dict(master_identity),
                            worker=dict(loan.get("worker")),
                            assignment_epoch=int(loan.get("assignment_epoch", 0)),
                            reservation_timeout_ms=15000,
                            loan_state="committed",
                        )
                        redirect = peer_manager.issue_redirect_command(
                            str(payload.get("loan_id")),
                            target_master=dict(loan["requester_master"]),
                            reason="redirect to peer",
                        )
                        if redirect.get("status") == "accepted":
                            log_event("ASD", "master", str(loan.get("worker", {}).get("node_label", "worker")), "command_redirect", request_id, "sent")
                    else:
                        response = build_protocol_error_message(message, master_identity, str(decision.get("reason", {}).get("message") or "invalid_confirm_loan"))
                        response["payload"]["error_code"] = str(decision.get("reason", {}).get("code") or "invalid_state")
                    registry.remember_processed_message(message, response)
                    with write_lock:
                        send_msg(conn, response)
                    log_event("ASD", "master", source["node_label"], msg_type, request_id, decision["status"])

                elif msg_type == "get_loan_status":
                    payload = message.get("payload", {}) if isinstance(message.get("payload", {}), dict) else {}
                    if peer_manager is None: peer_manager = PeerLoanManager(registry, master_identity, task_queue=task_queue)
                    loan = peer_manager.get_loan_status(str(payload.get("loan_id")))
                    response = build_loan_status_message(
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
                        loan_id=str(payload.get("loan_id")),
                        requester_master=dict(loan.get("requester_master", master_identity)),
                        target_master=dict(master_identity),
                        home_master=dict(master_identity),
                        worker=dict(loan.get("worker", {"group_label": str(master_identity["group_label"]), "node_id": str(uuid.uuid4()), "node_label": "unknown-worker", "node_role": "worker"})),
                        assignment_epoch=int(loan.get("assignment_epoch", 0) or 0),
                        loan_state=str(loan.get("loan_state", "unknown")),
                    )
                    registry.remember_processed_message(message, response)
                    with write_lock:
                        send_msg(conn, response)
                    log_event("ASD", "master", source["node_label"], "loan_status", request_id, str(loan.get("loan_state", "unknown")))

                elif msg_type == "cancel_loan":
                    payload = message.get("payload", {}) if isinstance(message.get("payload", {}), dict) else {}
                    if peer_manager is None: peer_manager = PeerLoanManager(registry, master_identity, task_queue=task_queue)
                    decision = peer_manager.cancel_loan(str(payload.get("loan_id")), reason=str(payload.get("reason") or "cancelled"))
                    response = build_loan_status_message(
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
                        loan_id=str(payload.get("loan_id")),
                        requester_master=dict(master_identity),
                        target_master=dict(master_identity),
                        home_master=dict(master_identity),
                        worker={"group_label": str(master_identity["group_label"]), "node_id": str(uuid.uuid4()), "node_label": "cancelled-worker", "node_role": "worker"},
                        assignment_epoch=int(payload.get("assignment_epoch", 0) or 0),
                        loan_state="cancelled" if decision["status"] == "accepted" else "unknown",
                    )
                    registry.remember_processed_message(message, response)
                    with write_lock:
                        send_msg(conn, response)
                    log_event("ASD", "master", source["node_label"], msg_type, request_id, decision["status"])

                elif msg_type == "metrics_request":
                    payload = message.get("payload", {}) if isinstance(message.get("payload", {}), dict) else {}
                    if not isinstance(payload, dict) or not validate_message(message):
                        log_event("ASD", source["node_label"], "master", msg_type, request_id, "rejected")
                        with write_lock:
                            send_msg(conn, build_protocol_error_message(message, master_identity, "invalid metrics_request payload"))
                        continue

                    metrics = task_queue.snapshot() if task_queue is not None else {
                        "total_tasks": 0,
                        "completed": 0,
                        "pending": 0,
                        "running": 0,
                        "failed": 0,
                        "timeout_attempts": 0,
                        "recovered_after_timeout": 0,
                        "tasks_with_multiple_attempts": 0,
                        "pending_task_ids": [],
                        "running_task_ids": [],
                        "failed_task_ids": [],
                        "completed_task_ids": [],
                    }
                    response = build_metrics_response_message(
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
                        metrics=metrics,
                    )
                    registry.remember_processed_message(message, response)
                    with write_lock:
                        send_msg(conn, response)
                    log_event("ASD", "master", source["node_label"], "metrics_response", request_id, "sent")

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
                    result_status = str(payload.get("status") or "").lower()
                    if result_status not in {"ok", "nok"}:
                        result_status = "ok" if str(payload.get("result")) != "None" else "nok"

                    with registry._lock:
                        if not registry.is_active_session(str(source["node_id"]), conn):
                            log_event("ASD", source["node_label"], "master", msg_type, request_id, "inactive_worker_session")
                            return

                        if registry.has_request_conflict(message):
                            response_to_send = build_protocol_error_message(message, master_identity, "request_conflict")
                            response_to_send["payload"]["error_code"] = "request_conflict"
                            response_to_send["payload"]["error_message"] = "request_id reused with different content"
                        else:
                            response_to_send = registry.get_processed_response(message)

                        if response_to_send is None:
                            if result_status == "ok":
                                accepted, reason = task_queue.complete_task(task_id, attempt_id, accepted=True, reason="completed", payload=payload, worker_id=str(source["node_id"]))
                            else:
                                accepted, reason = task_queue.complete_task(task_id, attempt_id, accepted=False, reason="attempt_failed", payload=payload, worker_id=str(source["node_id"]))

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
                            response_to_send = ack

                    with write_lock:
                        send_msg(conn, response_to_send)
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
                        no_task = build_no_task_message(message, source={"group_label": str(master_identity["group_label"]), "node_id": str(master_identity["node_id"]), "node_label": str(master_identity["node_label"]), "node_role": str(master_identity["node_role"])}, destination={"group_label": str(source["group_label"]), "node_id": str(source["node_id"]), "node_label": str(source["node_label"]), "node_role": str(source["node_role"])}, retry_after_ms=1000)
                        with write_lock:
                            send_msg(conn, no_task)
                        log_event("ASD", "master", source["node_label"], "no_task", request_id, "sent")
                        continue

                    assignment = task_queue.claim_next_task(source["node_id"], assignment_epoch=int(payload.get("assignment_epoch", 0)), loan_id=payload.get("loan_id"))
                    if assignment is None:
                        no_task = build_no_task_message(message, source={"group_label": str(master_identity["group_label"]), "node_id": str(master_identity["node_id"]), "node_label": str(master_identity["node_label"]), "node_role": str(master_identity["node_role"])}, destination={"group_label": str(source["group_label"]), "node_id": str(source["node_id"]), "node_label": str(source["node_label"]), "node_role": str(source["node_role"])}, retry_after_ms=1000)
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

                elif msg_type == "loan_committed":
                    payload = message.get("payload", {}) if isinstance(message.get("payload", {}), dict) else {}
                    if peer_manager is None:
                        peer_manager = PeerLoanManager(registry, master_identity, task_queue=task_queue)
                    mirror = peer_manager.mirror_committed_loan(
                        loan_id=str(payload.get("loan_id")),
                        requester_master=dict(payload.get("requester_master") or master_identity),
                        target_master=dict(payload.get("target_master") or master_identity),
                        home_master=dict(payload.get("home_master") or master_identity),
                        worker=dict(payload.get("worker") or {}),
                        assignment_epoch=int(payload.get("assignment_epoch", 0)),
                    )
                    log_event("ASD", "master", source["node_label"], msg_type, request_id, mirror["status"])

                elif msg_type == "notify_worker_returned":
                    payload = message.get("payload", {}) if isinstance(message.get("payload", {}), dict) else {}
                    if peer_manager is None:
                        peer_manager = PeerLoanManager(registry, master_identity, task_queue=task_queue)
                    decision = peer_manager.process_notify_worker_returned(
                        str(payload.get("loan_id", "")),
                        worker=dict(payload.get("worker") or {}),
                        assignment_epoch=int(payload.get("assignment_epoch", 0)),
                        source=source,
                    )
                    ack = build_return_ack_message(
                        message,
                        source=master_identity,
                        destination=source,
                        loan_id=str(payload.get("loan_id", "")),
                        worker=dict(payload.get("worker") or {}),
                        assignment_epoch=int(payload.get("assignment_epoch", 0)),
                        status="returned" if decision["status"] == "accepted" else "rejected",
                    )
                    registry.remember_processed_message(message, ack)
                    with write_lock:
                        send_msg(conn, ack)
                    log_event("ASD", "master", source["node_label"], msg_type, request_id, decision["status"])

                    if decision["status"] == "accepted":
                        loan = peer_manager._get_loan_record(str(payload.get("loan_id", "")))
                        requester = (loan or {}).get("requester_master", {})

                        if requester.get("node_id") != master_identity.get("node_id") and requester.get("host") and requester.get("port"):
                            try:
                                peer_ack = build_return_ack_message(
                                    message,
                                    source=master_identity,
                                    destination=requester,
                                    loan_id=str(payload.get("loan_id", "")),
                                    worker=dict(payload.get("worker") or {}),
                                    assignment_epoch=int(payload.get("assignment_epoch", 0)),
                                    status="returned",
                                    request_id=str(uuid.uuid4()),
                                )
                                with socket.create_connection(
                                    (requester["host"], int(requester["port"])),
                                    timeout=2.0,
                                ) as peer_conn:
                                    send_msg(peer_conn, peer_ack)
                                log_event("ASD", "master", requester["node_label"], "return_ack", peer_ack["request_id"], "sent")
                            except (OSError, ValueError) as exc:
                                log_event("ASD", "master", requester["node_label"], "return_ack", request_id, f"send_failed: {exc}")

                elif msg_type == "return_ack":
                    payload = message.get("payload", {}) if isinstance(message.get("payload", {}), dict) else {}
                    if peer_manager is None:
                        peer_manager = PeerLoanManager(registry, master_identity, task_queue=task_queue)
                    decision = peer_manager.process_return_ack(
                        str(payload.get("loan_id", "")),
                        worker=dict(payload.get("worker") or {}),
                        assignment_epoch=int(payload.get("assignment_epoch", 0)),
                        status=str(payload.get("status", "returned")),
                    )
                    log_event("ASD", "master", source["node_label"], msg_type, request_id, decision["status"])
    except ConnectionResetError:
        if current_worker_identity is not None:
            with registry._lock:
                if registry.is_active_session(current_worker_identity["node_id"], conn):
                    registry.mark_session_unavailable(current_worker_identity["node_id"], conn, reason="connection reset")
                    if task_queue is not None:
                        task_queue.handle_worker_unavailable(current_worker_identity["node_id"], reason="connection reset")
    except BrokenPipeError:
        if current_worker_identity is not None:
            with registry._lock:
                if registry.is_active_session(current_worker_identity["node_id"], conn):
                    registry.mark_session_unavailable(current_worker_identity["node_id"], conn, reason="broken pipe")
                    if task_queue is not None:
                        task_queue.handle_worker_unavailable(current_worker_identity["node_id"], reason="broken pipe")
    except OSError:
        if current_worker_identity is not None:
            with registry._lock:
                if registry.is_active_session(current_worker_identity["node_id"], conn):
                    registry.mark_session_unavailable(current_worker_identity["node_id"], conn, reason="socket error")
                    if task_queue is not None:
                        task_queue.handle_worker_unavailable(current_worker_identity["node_id"], reason="socket error")
    finally:
        if current_worker_identity is not None:
            registry.release_session(current_worker_identity["node_id"], conn)
        session_stop.set()
        try:
            conn.shutdown(socket.SHUT_RDWR)
        except OSError:
            pass
        try:
            conn.close()
        except OSError:
            pass


class LoadMonitor:
    def __init__(
        self,
        task_queue: "MasterTaskQueue",
        *,
        saturation_threshold: int = 10,
        release_threshold: int = 3,
        stable_samples: int = 3,
        monitor_interval_ms: int = 1000,
        on_saturated=None,
        on_released=None,
    ):
        if not isinstance(saturation_threshold, int) or isinstance(saturation_threshold, bool):
            raise ValueError("saturation_threshold must be an integer")
        if not isinstance(release_threshold, int) or isinstance(release_threshold, bool):
            raise ValueError("release_threshold must be an integer")
        if not isinstance(stable_samples, int) or isinstance(stable_samples, bool):
            raise ValueError("stable_samples must be an integer")
        if not isinstance(monitor_interval_ms, int) or isinstance(monitor_interval_ms, bool):
            raise ValueError("monitor_interval_ms must be an integer")
        if release_threshold >= saturation_threshold:
            raise ValueError("release_threshold must be lower than saturation_threshold")
        if saturation_threshold <= 0:
            raise ValueError("saturation_threshold must be positive")
        if release_threshold < 0:
            raise ValueError("release_threshold must be non-negative")
        if stable_samples <= 0:
            raise ValueError("stable_samples must be positive")
        if monitor_interval_ms <= 0:
            raise ValueError("monitor_interval_ms must be positive")

        self.task_queue = task_queue
        self.saturation_threshold = int(saturation_threshold)
        self.release_threshold = int(release_threshold)
        self.stable_samples = int(stable_samples)
        self.monitor_interval_ms = int(monitor_interval_ms)
        self.state = "normal"
        self.pending_tasks = 0
        self.consecutive_above = 0
        self.consecutive_below = 0
        self.last_transition = None
        self.on_saturated = on_saturated
        self.on_released = on_released
        self._lock = threading.RLock()

    def _pending_count(self) -> int:
        if hasattr(self.task_queue, "monitor_pending_count"):
            return int(self.task_queue.monitor_pending_count())

        total = 0
        for record in self.task_queue.pending():
            status = str(record.get("status", "pending")).lower()
            if status == "pending":
                total += 1
        return total

    def sample(self) -> str:
        with self._lock:
            self.pending_tasks = self._pending_count()

            if self.pending_tasks > self.saturation_threshold:
                self.consecutive_above += 1
                self.consecutive_below = 0
                if self.consecutive_above >= self.stable_samples:
                    candidate_state = "saturated"
                    if self.state != candidate_state:
                        self.last_transition = int(time.time() * 1000)
                        self.state = candidate_state
                        log_event("ASD", "master", "queue", "load_state", str(self.pending_tasks), self.state)
                        if self.on_saturated is not None:
                            try:
                                self.on_saturated()
                            except Exception:
                                log_event("ASD", "master", "queue", "saturation_callback_error", "-", "error in on_saturated callback")
                return self.state

            if self.pending_tasks <= self.release_threshold:
                self.consecutive_below += 1
                self.consecutive_above = 0
                if self.consecutive_below >= self.stable_samples:
                    candidate_state = "released"
                    if self.state != candidate_state:
                        self.last_transition = int(time.time() * 1000)
                        self.state = candidate_state
                        log_event("ASD", "master", "queue", "load_state", str(self.pending_tasks), self.state)
                        if self.on_released is not None:
                            try:
                                self.on_released()
                            except Exception:
                                log_event("ASD", "master", "queue", "release_callback_error", "-", "error in on_released callback")
                return self.state

            self.consecutive_above = 0
            self.consecutive_below = 0
            if self.state in {"saturated", "released"}:
                self.state = "normal"
                self.last_transition = int(time.time() * 1000)
                log_event("ASD", "master", "queue", "load_state", str(self.pending_tasks), self.state)
            return self.state

    def start(self, *, stop_event: threading.Event | None = None, interval_seconds: float | None = None) -> threading.Thread:
        if stop_event is None:
            stop_event = threading.Event()
        if interval_seconds is None:
            interval_seconds = max(self.monitor_interval_ms / 1000.0, 0.05)

        def loop() -> None:
            while not stop_event.is_set():
                self.sample()
                if stop_event.wait(interval_seconds):
                    break

        thread = threading.Thread(target=loop, daemon=True)
        thread.start()
        return thread


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

    def pending_count(self) -> int:
        with self._lock:
            total = 0
            for task_id in self._order:
                record = self._tasks.get(task_id)
                if record is None:
                    continue
                status = str(record.get("status", "pending")).lower()
                if status in {"pending", "running", "assigned"}:
                    total += 1
            return total

    def monitor_pending_count(self) -> int:
        with self._lock:
            total = 0
            for task_id in self._order:
                record = self._tasks.get(task_id)
                if record is None:
                    continue
                if str(record.get("status", "pending")).lower() == "pending":
                    total += 1
            return total

    def snapshot(self) -> Dict[str, object]:
        with self._lock:
            total_tasks = len(self._tasks)
            counts = {"completed": 0, "pending": 0, "running": 0, "failed": 0}
            pending_task_ids: list[str] = []
            running_task_ids: list[str] = []
            failed_task_ids: list[str] = []
            completed_task_ids: list[str] = []
            completed_by_worker: dict[str, int] = {}
            timeout_attempts = 0
            recovered_after_timeout = 0
            tasks_with_multiple_attempts = 0

            for task_id in list(self._order):
                record = self._tasks.get(task_id)
                if record is None:
                    continue

                status = str(record.get("status", "pending")).lower()
                counts[status] = counts.get(status, 0) + 1

                if status == "pending":
                    pending_task_ids.append(task_id)
                elif status == "running":
                    running_task_ids.append(task_id)
                elif status == "failed":
                    failed_task_ids.append(task_id)
                elif status == "completed":
                    completed_task_ids.append(task_id)

                attempts = record.get("attempts", {}) if isinstance(record.get("attempts", {}), dict) else {}
                if status == "completed":
                    for attempt in attempts.values():
                        if isinstance(attempt, dict) and attempt.get("status") == "completed":
                            worker_id = attempt.get("worker_id")
                            if isinstance(worker_id, str) and worker_id:
                                completed_by_worker[worker_id] = completed_by_worker.get(worker_id, 0) + 1
                            break
                task_timeout_attempts = 0
                for attempt in attempts.values():
                    if str(attempt.get("status", "")).lower() == "timed_out":
                        task_timeout_attempts += 1
                timeout_attempts += task_timeout_attempts

                if int(record.get("attempt_count", 0)) > 1:
                    tasks_with_multiple_attempts += 1
                if task_timeout_attempts > 0 and int(record.get("attempt_count", 0)) > 1 and status != "failed":
                    recovered_after_timeout += 1

            return {
                "total_tasks": int(total_tasks),
                "completed": int(counts.get("completed", 0)),
                "pending": int(counts.get("pending", 0)),
                "running": int(counts.get("running", 0)),
                "failed": int(counts.get("failed", 0)),
                "timeout_attempts": int(timeout_attempts),
                "recovered_after_timeout": int(recovered_after_timeout),
                "tasks_with_multiple_attempts": int(tasks_with_multiple_attempts),
                "pending_task_ids": pending_task_ids,
                "running_task_ids": running_task_ids,
                "failed_task_ids": failed_task_ids,
                "completed_task_ids": completed_task_ids,
                "completed_by_worker": completed_by_worker,
            }

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
            if str(record.get("status", "pending")).lower() in {"completed", "failed"}:
                return False
            if str(record.get("status", "pending")).lower() != "running":
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
                if str(record.get("status", "pending")).lower() in {"completed", "failed"}:
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
            has_running_assignment = False
            for task_id in self._order:
                record = self._tasks.get(task_id)
                if record is None:
                    continue
                if str(record.get("status", "")).lower() == "running" and str(record.get("assigned_to", "")) == str(worker_id):
                    has_running_assignment = True
                    break

            if has_running_assignment:
                return None

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

    def complete_task(self, task_id: str, attempt_id: str, *, accepted: bool, reason: str, payload: Dict[str, object] | None = None, worker_id: str | None = None) -> tuple[bool, str]:
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

            attempt_record = attempts.get(attempt_id, {})

            if worker_id is not None:
                if str(attempt_record.get("worker_id")) != worker_id:
                    return False, "wrong_worker"

                result_payload = payload if isinstance(payload, dict) else {}

                epoch = result_payload.get("assignment_epoch")
                if type(epoch) is not int:
                    return False, "invalid_assignment_epoch"

                if epoch != attempt_record.get("assignment_epoch"):
                    return False, "stale_assignment_epoch"

                if result_payload.get("loan_id") != attempt_record.get("loan_id"):
                    return False, "invalid_loan_id"

            if bool(attempt_record.get("result_seen")):
                return True, "duplicate result"

            attempt_status = str(attempt_record.get("status", "")).lower()
            if attempt_status in {"timed_out", "failed"}:
                return False, "stale_attempt"

            if record.get("status") == "completed" and str(record.get("last_attempt_id")) == str(attempt_id):
                return True, "duplicate result"

            if record.get("status") == "failed" and str(record.get("last_attempt_id")) == str(attempt_id):
                return False, "stale_attempt"

            if current_active is not None and str(current_active) != str(attempt_id):
                return False, "stale_attempt"

            payload_dict = dict(payload or {})
            result_status = str(payload_dict.get("status") or "").lower()
            if result_status not in {"ok", "nok"}:
                result_status = "ok" if bool(accepted) and str(reason or "").lower() in {"completed", "ok"} else "nok"

            if result_status == "ok":
                record["status"] = "completed"
                record["completed"] = True
                record["completed_at"] = int(time.time() * 1000)
                record["result_status"] = "ok"
                record["result_payload"] = payload_dict
                record["assigned_to"] = None
                record["loan_id"] = None
                record["active_attempt_id"] = None
                record["running_since_ms"] = None
                record["attempts"][attempt_id]["status"] = "completed"
                record["attempts"][attempt_id]["result_seen"] = True
                record["attempts"][attempt_id]["result_status"] = "ok"
                return True, reason or "completed"

            if int(record.get("attempt_count", 0)) >= int(record.get("max_task_attempts", self.max_task_attempts)):
                record["status"] = "failed"
                record["failed_reason"] = "max_task_attempts_exceeded"
                record["failed_at_ms"] = int(time.time() * 1000)
                record["completed"] = False
                record["result_status"] = "nok"
                record["result_payload"] = payload_dict
                record["assigned_to"] = None
                record["loan_id"] = None
                record["active_attempt_id"] = None
                record["running_since_ms"] = None
                record["attempts"][attempt_id]["status"] = "failed"
                record["attempts"][attempt_id]["result_seen"] = True
                record["attempts"][attempt_id]["result_status"] = "nok"
                return True, "attempt_failed"

            record["status"] = "pending"
            record["completed"] = False
            record["result_status"] = "nok"
            record["result_payload"] = payload_dict
            record["assigned_to"] = None
            record["loan_id"] = None
            record["active_attempt_id"] = None
            record["running_since_ms"] = None
            record["failure_reason"] = "attempt_failed"
            record["attempts"][attempt_id]["status"] = "failed"
            record["attempts"][attempt_id]["result_seen"] = True
            record["attempts"][attempt_id]["result_status"] = "nok"
            return True, "attempt_failed"

    def __len__(self) -> int:
        with self._lock:
            return len(self._tasks)



def build_master_performance_metrics(
    registry: MasterRegistry,
    task_queue: MasterTaskQueue,
    master_identity: Dict[str, object],
    *,
    uptime_seconds: float = 0.0,
    peer_manager: PeerLoanManager | None = None,
) -> dict:
    snapshot = task_queue.snapshot()

    with registry._lock:
        workers = {node_id: dict(worker) for node_id, worker in registry.workers.items()}
        active_ids = set(registry.active_sessions)

    with task_queue._lock:
        busy_ids = {
            str(task.get("assigned_to"))
            for task in task_queue._tasks.values()
            if task.get("status") == "running" and task.get("assigned_to")
        }

    available_ids = {
        node_id
        for node_id, worker in workers.items()
        if node_id in active_ids and worker.get("status") == "available"
    }

    busy = len(available_ids & busy_ids)
    idle = len(available_ids - busy_ids)

    loans: list[dict] = []
    workers_reserved = 0
    workers_lent = 0
    workers_received = 0
    if peer_manager is not None:
        loans = peer_manager.list_committed_loans()
        # Include offered loans too by reading ledger snapshot through the peer manager ledger.
        loans = [dict(record) for record in peer_manager._loans.values()]
        for loan in loans:
            state = str(loan.get("loan_state", ""))
            if state in {"offered", "committed"}:
                workers_reserved += 1
                requester = loan.get("requester_master") or {}
                target = loan.get("target_master") or {}
                if str(requester.get("node_id", "")) == str(master_identity.get("node_id", "")):
                    workers_received += 1
                if str(target.get("node_id", "")) == str(master_identity.get("node_id", "")):
                    workers_lent += 1

    return {
        "uptime_seconds": max(0.0, float(uptime_seconds)),
        "tasks_pending": int(snapshot["pending"]),
        "tasks_running": int(snapshot["running"]),
        "tasks_completed": int(snapshot["completed"]),
        "tasks_failed": int(snapshot["failed"]),
        "workers_owned": len(workers),
        "workers_active_here": busy + idle,
        "workers_busy": busy,
        "workers_idle": idle,
        "workers_reserved": workers_reserved,
        "workers_lent": workers_lent,
        "workers_received": workers_received,
        "workers_unavailable": sum(
            worker.get("status") == "unavailable"
            for worker in workers.values()
        ),
        "thresholds": {
            "saturation_threshold": int(master_identity.get("saturation_threshold", 10)),
            "release_threshold": int(master_identity.get("release_threshold", 3)),
            "stable_samples": int(master_identity.get("stable_samples", 3)),
            "monitor_interval_ms": int(master_identity.get("monitor_interval_ms", 1000)),
        },
        "loans": loans,
        "neighbors": [dict(peer) for peer in master_identity.get("peers", [])] if isinstance(master_identity.get("peers", []), list) else [],
    }



def send_master_performance_report(
    registry: MasterRegistry,
    task_queue: MasterTaskQueue,
    master_identity: Dict[str, object],
    *,
    collector_host: str = "127.0.0.1",
    collector_port: int = 9100,
    uptime_seconds: float = 0.0,
    timeout_seconds: float = 2.0,
    peer_manager: PeerLoanManager | None = None,
) -> bool:
    collector_identity = {
        "group_label": str(master_identity.get("group_label", "p2p-cluster")),
        "node_id": "33333333-3333-4333-8333-333333333333",
        "node_label": "collector",
        "node_role": "collector",
    }

    try:
        metrics = build_master_performance_metrics(
            registry,
            task_queue,
            master_identity,
            uptime_seconds=uptime_seconds,
        )

        message = build_performance_report_message(
            master_identity,
            collector_identity,
            metrics=metrics,
        )

        with socket.create_connection(
            (collector_host, int(collector_port)),
            timeout=timeout_seconds,
        ) as conn:
            conn.settimeout(timeout_seconds)
            send_msg(conn, message)

        return True

    except (OSError, ValueError, TypeError) as exc:
        log_event(
            "ASD",
            "master",
            "collector",
            "performance_report",
            "-",
            f"send_failed: {exc}",
        )
        return False



def start_master_performance_report_loop(
    registry: MasterRegistry,
    task_queue: MasterTaskQueue,
    master_identity: Dict[str, object],
    *,
    collector_host: str = "127.0.0.1",
    collector_port: int = 9100,
    interval_seconds: float = 10.0,
    stop_event: threading.Event | None = None,
    peer_manager: PeerLoanManager | None = None,
) -> threading.Thread:
    if interval_seconds <= 0:
        raise ValueError("interval_seconds deve ser positivo")

    if stop_event is None:
        stop_event = threading.Event()

    started_at = time.monotonic()

    def loop() -> None:
        while not stop_event.wait(interval_seconds):
            send_master_performance_report(
                registry,
                task_queue,
                master_identity,
                collector_host=collector_host,
                collector_port=collector_port,
                uptime_seconds=time.monotonic() - started_at,
                peer_manager=peer_manager,
            )

    thread = threading.Thread(
        target=loop,
        name="master-performance-report",
        daemon=True,
    )
    thread.start()
    return thread



def run_server(host: str, port: int, master_identity: Dict[str, object], *, snapshot_interval_seconds: float = 0.0) -> None:
    registry = MasterRegistry()
    task_queue = MasterTaskQueue()
    reaper_stop = threading.Event()
    monitor_stop = threading.Event()
    snapshot_stop = threading.Event()
    performance_stop = threading.Event()
    peer_manager = PeerLoanManager(registry, master_identity, task_queue=task_queue)
    load_monitor = LoadMonitor(
        task_queue,
        saturation_threshold=int(master_identity.get("saturation_threshold", 10)),
        release_threshold=int(master_identity.get("release_threshold", 3)),
        stable_samples=int(master_identity.get("stable_samples", 3)),
        monitor_interval_ms=int(master_identity.get("monitor_interval_ms", 1000)),
        on_saturated=lambda: negotiate_peer_loans(registry, task_queue, master_identity, peer_manager),
        on_released=lambda: release_peer_loans(peer_manager),
    )
    load_monitor.start(
        stop_event=monitor_stop,
        interval_seconds=load_monitor.monitor_interval_ms / 1000.0,
    )
    start_task_reaper_loop(task_queue, interval_seconds=0.25, stop_event=reaper_stop)
    if snapshot_interval_seconds > 0:
        start_master_snapshot_loop(task_queue, interval_seconds=snapshot_interval_seconds, stop_event=snapshot_stop)
    server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    server.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    server.bind((host, port))
    server.listen(5)
    log_event("ASD", "master", "network", "server_start", "-", f"listening on {host}:{port}")

    start_master_performance_report_loop(
        registry,
        task_queue,
        master_identity,
        collector_host=str(master_identity.get("collector_host", "127.0.0.1")),
        collector_port=int(master_identity.get("collector_port", 9100)),
        interval_seconds=float(
            master_identity.get("performance_interval_seconds", 10.0)
        ),
        stop_event=performance_stop,
        peer_manager=peer_manager,
    )

    try:
        while True:
            conn, addr = server.accept()
            threading.Thread(target=handle_client, args=(conn, addr, registry, master_identity, task_queue, peer_manager), daemon=True).start()
    except KeyboardInterrupt:
        pass
    finally:
        reaper_stop.set()
        monitor_stop.set()
        snapshot_stop.set()
        performance_stop.set()
        try:
            server.close()
        except OSError:
            pass


def main() -> None:
    parser = argparse.ArgumentParser(description="Master node for NDJSON registration protocol")
    parser.add_argument("--host", default=None)
    parser.add_argument("--port", type=int, default=None)
    parser.add_argument("--config", type=Path, default=CONFIG_PATH, help="Caminho do arquivo JSON de configuraÃƒÆ’Ã†â€™Ãƒâ€ Ã¢â‚¬â„¢ÃƒÆ’Ã¢â‚¬Å¡Ãƒâ€šÃ‚Â§ÃƒÆ’Ã†â€™Ãƒâ€ Ã¢â‚¬â„¢ÃƒÆ’Ã¢â‚¬Å¡Ãƒâ€šÃ‚Â£o do Master")
    parser.add_argument("--snapshot-interval-seconds", type=float, default=0.0, help="Intervalo em segundos para imprimir o snapshot do Master; 0 desabilita")
    args = parser.parse_args()

    config = load_or_create_config(args.config)
    host = args.host if args.host is not None else str(config.get("host", "127.0.0.1"))
    port = args.port if args.port is not None else int(config.get("port", 9000))
    run_server(host, port, config, snapshot_interval_seconds=args.snapshot_interval_seconds)




#TODO:
def negotiate_peer_loans(
    registry: MasterRegistry,
    task_queue: MasterTaskQueue,
    master_identity: dict,
    peer_manager: PeerLoanManager,
) -> None:
    peers = master_identity.get("peers")
    if not isinstance(peers, list):
        return

    workers_needed = 1
    tasks_pending = int(task_queue.monitor_pending_count()) if hasattr(task_queue, "monitor_pending_count") else len(task_queue.pending())

    for peer in peers:
        if not isinstance(peer, dict):
            continue
        if peer.get("node_role") != "master":
            continue
        try:
            peer_identity = {
                "group_label": str(peer.get("group_label", master_identity.get("group_label", "p2p-cluster"))),
                "node_id": str(peer["node_id"]),
                "node_label": str(peer.get("node_label", "master-peer")),
                "node_role": "master",
                "host": str(peer.get("host", "127.0.0.1")),
                "port": int(peer["port"]),
            }
            with socket.create_connection((peer_identity["host"], peer_identity["port"]), timeout=2.0) as conn:
                conn.settimeout(2.0)
                request = build_request_help_message(
                    source=master_identity,
                    destination=peer_identity,
                    requester_master=master_identity,
                    workers_needed=workers_needed,
                    tasks_pending=tasks_pending,
                )
                send_msg(conn, request)
                buffer = bytearray()
                responses = recv_msgs(conn, buffer)
                if responses is None:
                    continue
                response = next((item for item in responses if item.get("request_id") == request["request_id"]), None)
                if not isinstance(response, dict) or response.get("type") != "response_accepted":
                    continue

                payload = response.get("payload", {})
                confirm = build_confirm_loan_message(
                    response,
                    source=master_identity,
                    destination=peer_identity,
                    loan_id=str(payload["loan_id"]),
                    requester_master=master_identity,
                    target_master=peer_identity,
                    home_master=peer_identity,
                    worker=dict(payload["worker"]),
                    assignment_epoch=int(payload["assignment_epoch"]),
                    reservation_timeout_ms=15000,
                    loan_state="committed",
                )
                send_msg(conn, confirm)
                buffer = bytearray()
                replies = recv_msgs(conn, buffer)
                if replies:
                    committed = next((item for item in replies if item.get("type") == "loan_committed"), None)
                    if isinstance(committed, dict):
                        committed_payload = committed.get("payload", {})
                        peer_manager.mirror_committed_loan(
                            loan_id=str(committed_payload["loan_id"]),
                            requester_master=dict(committed_payload["requester_master"]),
                            target_master=dict(committed_payload["target_master"]),
                            home_master=dict(committed_payload["home_master"]),
                            worker=dict(committed_payload["worker"]),
                            assignment_epoch=int(committed_payload["assignment_epoch"]),
                        )
                        log_event("ASD", master_identity["node_label"], str(committed_payload.get("worker", {}).get("node_label", "worker")), "loan_committed", str(committed.get("request_id", "-")), "mirrored")
        except (OSError, ValueError, KeyError, TypeError) as exc:
            log_event("ASD", master_identity["node_label"], str(peer.get("node_label", "peer")), "request_help", "-", f"peer_error:{exc}")


def release_peer_loans(peer_manager: PeerLoanManager) -> None:
    for loan in peer_manager.list_committed_loans():
        if str(loan.get("requester_master", {}).get("node_id", "")) == str(peer_manager.master_identity.get("node_id", "")):
            peer_manager.issue_release_command(str(loan["loan_id"]), reason="load normalized")

if __name__ == "__main__":
    main()
