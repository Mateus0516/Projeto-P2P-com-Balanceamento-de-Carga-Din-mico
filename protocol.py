import json
import math
import re
import socket
import threading
import time
import uuid
from typing import Any, List

MAX_NDJSON_MESSAGE_BYTES = 65536

REQUIRED_MESSAGE_FIELDS = (
    "version",
    "type",
    "message_id",
    "request_id",
    "timestamp_ms",
    "source",
    "destination",
    "payload",
)

COLLECTOR_NODE_ID = "c011ec70-0000-4000-8000-000000000001"

REQUIRED_NODE_FIELDS = (
    "group_label",
    "node_id",
    "node_label",
    "node_role",
)


def log_event(group: str, origin: str, destination: str, msg_type: str, request_id: str, result: str) -> None:
    print(f"{group} | {origin} | {destination} | {msg_type} | {request_id} | {result}")


def _reject_duplicate_keys(pairs: list[tuple[str, Any]]) -> dict:
    result = {}
    for key, value in pairs:
        if key in result:
            raise ValueError(f"Duplicate JSON key: {key}")
        result[key] = value
    return result


def _reject_special_float(value: str) -> None:
    raise ValueError(f"Non-finite JSON number is not allowed: {value}")


def _validate_node_identity(node: Any) -> bool:
    if not isinstance(node, dict):
        return False

    for field in REQUIRED_NODE_FIELDS:
        if field not in node or not isinstance(node[field], str) or not node[field].strip():
            return False

    try:
        parsed_uuid = uuid.UUID(node["node_id"])
    except (ValueError, TypeError, AttributeError):
        return False

    if parsed_uuid.version != 4:
        return False

    if node["node_role"] not in {"master", "worker", "collector"}:
        return False

    return True


def validate_register_worker_payload(payload: Any, *, require_address: bool = True) -> tuple[bool, str | None]:
    if not isinstance(payload, dict):
        return False, "invalid payload"

    required_fields = ("home_master", "assignment_epoch", "loan_id")
    if require_address:
        required_fields += ("host", "port")
    missing = [field for field in required_fields if field not in payload]
    if missing:
        return False, f"missing required field(s): {', '.join(missing)}"

    home_master = payload.get("home_master")
    if not isinstance(home_master, dict) or not _validate_node_identity(home_master):
        return False, "home_master must be a valid node identity"
    if home_master.get("node_role") != "master":
        return False, "home_master must identify the master"

    assignment_epoch = payload.get("assignment_epoch")
    if not isinstance(assignment_epoch, int) or isinstance(assignment_epoch, bool) or assignment_epoch < 0:
        return False, "assignment_epoch must be a non-negative integer"

    loan_id = payload.get("loan_id")
    if loan_id is None:
        pass
    elif isinstance(loan_id, str):
        if not loan_id.strip():
            return False, "loan_id must be null or a non-empty string"
    else:
        return False, "loan_id must be null or a non-empty string"

    host = payload.get("host")
    if "host" in payload and (not isinstance(host, str) or not host.strip()):
        return False, "host must be a non-empty string"

    port = payload.get("port")
    if "port" in payload and (type(port) is not int or not 1 <= port <= 65535):
        return False, "port must be an integer between 1 and 65535"

    return True, None


def validate_task_submit_payload(payload: Any) -> bool:
    if not isinstance(payload, dict):
        return False

    required_fields = ("task_id", "task_kind", "parameters")
    if any(field not in payload for field in required_fields):
        return False

    task_id = payload.get("task_id")
    task_kind = payload.get("task_kind")
    parameters = payload.get("parameters")

    if not isinstance(task_id, str) or not task_id.strip():
        return False
    if task_kind != "sum_range":
        return False
    if not isinstance(parameters, dict):
        return False

    start = parameters.get("start")
    end = parameters.get("end")
    if not isinstance(start, int) or isinstance(start, bool):
        return False
    if not isinstance(end, int) or isinstance(end, bool):
        return False
    if not (0 <= start <= end <= 10000000):
        return False

    return True


def validate_task_submit_ack_payload(payload: Any) -> bool:
    if not isinstance(payload, dict):
        return False

    required_fields = ("task_id", "task_label", "accepted", "reason")
    if any(field not in payload for field in required_fields):
        return False

    task_id = payload.get("task_id")
    task_label = payload.get("task_label")
    accepted = payload.get("accepted")
    reason = payload.get("reason")

    if not isinstance(task_id, str) or not task_id.strip():
        return False
    if not isinstance(task_label, str) or not task_label.strip():
        return False
    if not isinstance(accepted, bool):
        return False
    if not isinstance(reason, str) or not reason.strip():
        return False

    return True


def validate_task_request_payload(payload: Any) -> bool:
    if not isinstance(payload, dict):
        return False

    required_fields = ("assignment_epoch", "loan_id")
    if any(field not in payload for field in required_fields):
        return False

    assignment_epoch = payload.get("assignment_epoch")
    if not isinstance(assignment_epoch, int) or isinstance(assignment_epoch, bool) or assignment_epoch < 0:
        return False

    loan_id = payload.get("loan_id")
    if loan_id is not None and (not isinstance(loan_id, str) or not loan_id.strip()):
        return False

    return True


def validate_task_assignment_payload(payload: Any) -> bool:
    if not isinstance(payload, dict):
        return False

    required_fields = ("task_id", "task_label", "attempt_id", "task_kind", "parameters", "assignment_epoch", "loan_id")
    if any(field not in payload for field in required_fields):
        return False

    task_id = payload.get("task_id")
    task_label = payload.get("task_label")
    attempt_id = payload.get("attempt_id")
    task_kind = payload.get("task_kind")
    parameters = payload.get("parameters")
    assignment_epoch = payload.get("assignment_epoch")
    loan_id = payload.get("loan_id")

    if not isinstance(task_id, str) or not task_id.strip():
        return False
    if not isinstance(task_label, str) or not task_label.strip():
        return False
    if not isinstance(attempt_id, str) or not attempt_id.strip():
        return False
    if task_kind != "sum_range":
        return False
    if not isinstance(parameters, dict):
        return False
    if not isinstance(assignment_epoch, int) or isinstance(assignment_epoch, bool) or assignment_epoch < 0:
        return False
    if loan_id is not None and (not isinstance(loan_id, str) or not loan_id.strip()):
        return False
    if not validate_task_submit_payload({"task_id": task_id, "task_kind": task_kind, "parameters": parameters}):
        return False

    return True


def validate_no_task_payload(payload: Any) -> bool:
    if not isinstance(payload, dict):
        return False

    if "retry_after_ms" not in payload:
        return False

    retry_after_ms = payload.get("retry_after_ms")
    if not isinstance(retry_after_ms, int) or isinstance(retry_after_ms, bool) or retry_after_ms < 0:
        return False
    return True


def validate_metrics_request_payload(payload: Any) -> bool:
    return isinstance(payload, dict)


REPORT_HOME_LOAN_STATES = {"offered", "committed", "closed", "cancelled", "expired"}
REPORT_TARGET_LOAN_STATES = {"waiting_worker", "active", "returning", "unknown"}


def validate_official_node(node: Any, role: str, *, address: bool = False) -> bool:
    if not _validate_node_identity(node) or node["node_role"] != role:
        return False
    group = node["group_label"]
    suffix = r"_master_[0-9]{2}_worker_[0-9]{2}" if role == "worker" else rf"_{role}_[0-9]{{2}}"
    if not re.fullmatch(r"grupo_[0-9]{2}", group) or not re.fullmatch(re.escape(group) + suffix, node["node_label"]):
        return False
    return not address or (
        isinstance(node.get("host"), str) and bool(node["host"].strip())
        and type(node.get("port")) is int and 1 <= node["port"] <= 65535
    )


def validate_performance_report_payload(payload: Any) -> bool:
    if not isinstance(payload, dict):
        return False

    required_counters = (
        "uptime_seconds",
        "tasks_pending",
        "tasks_running",
        "tasks_completed",
        "tasks_failed",
        "workers_owned",
        "workers_active_here",
        "workers_busy",
        "workers_idle",
        "workers_reserved",
        "workers_lent",
        "workers_received",
        "workers_unavailable",
    )

    for field in required_counters:
        value = payload.get(field)
        if type(value) is not int or value < 0:
            return False

    if payload["workers_active_here"] != (
        payload["workers_busy"] + payload["workers_idle"]
    ):
        return False
    if payload["workers_received"] > payload["workers_active_here"]:
        return False

    thresholds = payload.get("thresholds")
    if not isinstance(thresholds, dict):
        return False
    for field in ("saturation_threshold", "release_threshold", "stable_samples"):
        value = thresholds.get(field)
        if type(value) is not int or value < 0:
            return False
    if thresholds["stable_samples"] < 1:
        return False
    if thresholds["release_threshold"] >= thresholds["saturation_threshold"]:
        return False

    loans = payload.get("loans")
    if not isinstance(loans, list):
        return False
    for loan in loans:
        if not isinstance(loan, dict) or not _validate_loan_id(loan.get("loan_id")):
            return False
        label = loan.get("loan_label")
        state = loan.get("loan_state")
        if not isinstance(label, str) or not re.fullmatch(r"[a-z0-9]+(?:_[a-z0-9]+)*", label):
            return False
        if not isinstance(state, str) or state not in REPORT_HOME_LOAN_STATES | REPORT_TARGET_LOAN_STATES:
            return False
        if not validate_official_node(loan.get("worker"), "worker"):
            return False
        for field in ("home_master", "target_master"):
            if not validate_official_node(loan.get(field), "master", address=True):
                return False

    neighbors = payload.get("neighbors")
    if not isinstance(neighbors, list):
        return False
    for neighbor in neighbors:
        if not isinstance(neighbor, dict) or not validate_official_node(neighbor.get("node"), "master", address=True):
            return False
        if neighbor.get("status") not in ("unknown", "available", "unavailable") or "last_seen_ms" not in neighbor:
            return False
        last_seen = neighbor["last_seen_ms"]
        if last_seen is not None and (type(last_seen) is not int or last_seen < 0):
            return False

    if "system" in payload:
        system = payload["system"]
        if not isinstance(system, dict):
            return False
        for value in system.values():
            if value is not None and (
                type(value) not in (int, float) or value < 0 or (type(value) is float and not math.isfinite(value))
            ):
                return False

    return True


def validate_metrics_response_payload(payload: Any) -> bool:
    if not isinstance(payload, dict):
        return False

    required_fields = (
        "total_tasks",
        "completed",
        "pending",
        "running",
        "failed",
        "timeout_attempts",
        "recovered_after_timeout",
        "tasks_with_multiple_attempts",
        "pending_task_ids",
        "running_task_ids",
        "failed_task_ids",
        "completed_task_ids",
    )
    if any(field not in payload for field in required_fields):
        return False

    int_fields = (
        "total_tasks",
        "completed",
        "pending",
        "running",
        "failed",
        "timeout_attempts",
        "recovered_after_timeout",
        "tasks_with_multiple_attempts",
    )
    for field in int_fields:
        value = payload.get(field)
        if not isinstance(value, int) or isinstance(value, bool) or value < 0:
            return False

    for field in ("pending_task_ids", "running_task_ids", "failed_task_ids", "completed_task_ids"):
        value = payload.get(field)
        if not isinstance(value, list):
            return False
        if any(not isinstance(item, str) or not item.strip() for item in value):
            return False

    return True


def validate_task_result_payload(payload: Any) -> bool:
    if not isinstance(payload, dict):
        return False

    required_fields = ("task_id", "task_label", "attempt_id", "assignment_epoch", "loan_id", "status", "result", "error_code")
    if any(field not in payload for field in required_fields):
        return False

    task_id = payload.get("task_id")
    task_label = payload.get("task_label")
    attempt_id = payload.get("attempt_id")
    assignment_epoch = payload.get("assignment_epoch")
    loan_id = payload.get("loan_id")
    status = payload.get("status")
    result = payload.get("result")
    error_code = payload.get("error_code")

    if not isinstance(task_id, str) or not task_id.strip():
        return False
    if not isinstance(task_label, str) or not task_label.strip():
        return False
    if not isinstance(attempt_id, str) or not attempt_id.strip():
        return False
    if not isinstance(assignment_epoch, int) or isinstance(assignment_epoch, bool) or assignment_epoch < 0:
        return False
    if loan_id is not None and (not isinstance(loan_id, str) or not loan_id.strip()):
        return False
    if status not in {"ok", "nok"}:
        return False
    if status == "ok":
        if not isinstance(result, dict):
            return False
        if "sum" not in result or not isinstance(result["sum"], int) or isinstance(result["sum"], bool):
            return False
        if error_code is not None:
            return False
    else:
        if result is not None:
            return False
        if error_code is not None and (not isinstance(error_code, str) or not error_code.strip()):
            return False

    return True


def validate_task_ack_payload(payload: Any) -> bool:
    if not isinstance(payload, dict):
        return False

    required_fields = ("task_id", "attempt_id", "accepted", "reason")
    if any(field not in payload for field in required_fields):
        return False

    task_id = payload.get("task_id")
    attempt_id = payload.get("attempt_id")
    accepted = payload.get("accepted")
    reason = payload.get("reason")

    if not isinstance(task_id, str) or not task_id.strip():
        return False
    if not isinstance(attempt_id, str) or not attempt_id.strip():
        return False
    if not isinstance(accepted, bool):
        return False
    if not isinstance(reason, str) or not reason.strip():
        return False

    return True


VALID_LOAN_STATES = {"offered", "committed", "closed", "cancelled", "expired", "unknown"}


def _validate_loan_id(value: Any) -> bool:
    if not isinstance(value, str) or not value.strip():
        return False
    try:
        parsed = uuid.UUID(value)
    except (ValueError, TypeError, AttributeError):
        return False
    return parsed.version == 4


def _validate_master_identity(node: Any) -> bool:
    if not _validate_node_identity(node):
        return False
    return isinstance(node.get("node_role"), str) and node.get("node_role") == "master"


def _validate_worker_identity(node: Any) -> bool:
    if not _validate_node_identity(node):
        return False
    return isinstance(node.get("node_role"), str) and node.get("node_role") == "worker"


def _validate_loan_state(value: Any) -> bool:
    return isinstance(value, str) and value in VALID_LOAN_STATES


def _validate_loan_label(value: Any) -> bool:
    return isinstance(value, str) and re.fullmatch(r"[a-z][a-z0-9]*(?:_[a-z0-9]+)*_loan_[0-9]{6}", value) is not None


def _validate_worker_ref(value: Any) -> bool:
    if not isinstance(value, dict):
        return False
    if set(value) != set(REQUIRED_NODE_FIELDS):
        return False
    return _validate_worker_identity(value)


def _validate_loan_ref(value: Any) -> bool:
    if not isinstance(value, dict):
        return False
    required = {"loan_id", "loan_label", "worker", "home_master", "target_master", "assignment_epoch"}
    if set(value) != required:
        return False
    if not _validate_loan_id(value.get("loan_id")):
        return False
    if not _validate_loan_label(value.get("loan_label")):
        return False
    if not _validate_worker_ref(value.get("worker")):
        return False
    if not _validate_master_identity(value.get("home_master")):
        return False
    if not _validate_master_identity(value.get("target_master")):
        return False
    epoch = value.get("assignment_epoch")
    return isinstance(epoch, int) and not isinstance(epoch, bool) and epoch >= 0


def validate_request_help_payload(payload: Any) -> bool:
    if not isinstance(payload, dict):
        return False

    required_fields = ("requester_master", "workers_needed", "tasks_pending")
    if any(field not in payload for field in required_fields):
        return False

    requester_master = payload.get("requester_master")
    if not _validate_master_identity(requester_master):
        return False

    workers_needed = payload.get("workers_needed")
    if not isinstance(workers_needed, int) or isinstance(workers_needed, bool) or workers_needed < 1:
        return False

    tasks_pending = payload.get("tasks_pending")
    if not isinstance(tasks_pending, int) or isinstance(tasks_pending, bool) or tasks_pending < 0:
        return False

    return True


def validate_response_accepted_payload(payload: Any) -> bool:
    if not isinstance(payload, dict):
        return False
    if set(payload) != {"loan", "reservation_timeout_ms"}:
        return False
    if not _validate_loan_ref(payload.get("loan")):
        return False
    return payload.get("reservation_timeout_ms") == 15000


def validate_response_rejected_payload(payload: Any) -> bool:
    if not isinstance(payload, dict) or set(payload) != {"reason"}:
        return False
    return payload.get("reason") in ("high_load", "no_workers_available", "min_home_workers", "refused")


def validate_confirm_loan_payload(payload: Any) -> bool:
    if not isinstance(payload, dict) or set(payload) != {"loan_id", "assignment_epoch"}:
        return False
    if not _validate_loan_id(payload.get("loan_id")):
        return False
    epoch = payload.get("assignment_epoch")
    return isinstance(epoch, int) and not isinstance(epoch, bool) and epoch >= 0


def validate_loan_committed_payload(payload: Any) -> bool:
    if not isinstance(payload, dict) or set(payload) != {"loan", "loan_state"}:
        return False
    return _validate_loan_ref(payload.get("loan")) and payload.get("loan_state") == "committed"


def validate_command_redirect_payload(payload: Any) -> bool:
    return isinstance(payload, dict) and set(payload) == {"loan"} and _validate_loan_ref(payload.get("loan"))


def validate_register_temporary_worker_payload(payload: Any) -> bool:
    return isinstance(payload, dict) and set(payload) == {"loan"} and _validate_loan_ref(payload.get("loan"))


def validate_command_release_payload(payload: Any) -> bool:
    if not isinstance(payload, dict) or set(payload) != {"loan_id", "assignment_epoch", "reason"}:
        return False
    epoch = payload.get("assignment_epoch")
    return _validate_loan_id(payload.get("loan_id")) and isinstance(epoch, int) and not isinstance(epoch, bool) and epoch >= 0 and isinstance(payload.get("reason"), str) and bool(payload["reason"].strip())


def validate_notify_worker_returned_payload(payload: Any) -> bool:
    if not isinstance(payload, dict) or set(payload) != {"loan_id", "loan_label", "worker", "assignment_epoch", "loan_state"}:
        return False
    epoch = payload.get("assignment_epoch")
    return _validate_loan_id(payload.get("loan_id")) and _validate_loan_label(payload.get("loan_label")) and _validate_worker_ref(payload.get("worker")) and isinstance(epoch, int) and not isinstance(epoch, bool) and epoch >= 0 and payload.get("loan_state") == "closed"


def validate_return_ack_payload(payload: Any) -> bool:
    return isinstance(payload, dict) and set(payload) == {"loan_id", "loan_state"} and _validate_loan_id(payload.get("loan_id")) and payload.get("loan_state") == "closed"


def validate_get_loan_status_payload(payload: Any) -> bool:
    if not isinstance(payload, dict) or "loan_id" not in payload:
        return False
    return _validate_loan_id(payload.get("loan_id")) and all(
        field not in payload or _validate_master_identity(payload[field])
        for field in ("requester_master", "target_master")
    )


def validate_loan_status_payload(payload: Any) -> bool:
    if not isinstance(payload, dict) or set(payload) != {"loan_id", "loan_state", "loan"}:
        return False
    return _validate_loan_id(payload.get("loan_id")) and _validate_loan_state(payload.get("loan_state")) and (payload.get("loan") is None or _validate_loan_ref(payload.get("loan")))


def validate_cancel_loan_payload(payload: Any) -> bool:
    return isinstance(payload, dict) and set(payload) == {"loan_id"} and _validate_loan_id(payload.get("loan_id"))


def validate_message(message: Any) -> bool:
    if not isinstance(message, dict):
        return False

    if not set(REQUIRED_MESSAGE_FIELDS).issubset(message):
        return False

    if any(not isinstance(key, str) or not key.isascii() or not key.isidentifier() or key.lower() != key or "__" in key for key in message):
        return False

    if not isinstance(message.get("type"), str):
        return False

    if message["version"] != "sd_2026_2_v1":
        return False

    if message.get("type") not in {"register_worker", "registration_ack", "heartbeat", "heartbeat_ack", "protocol_error", "task_submit", "task_submit_ack", "task_request", "task_assignment", "no_task", "task_result", "task_ack", "metrics_request", "metrics_response", "performance_report", "request_help", "response_accepted", "response_rejected", "confirm_loan", "loan_committed", "command_redirect", "register_temporary_worker", "command_release", "notify_worker_returned", "return_ack", "get_loan_status", "loan_status", "cancel_loan"}:
        return False

    for field in ("type", "message_id", "request_id"):
        if not isinstance(message[field], str) or not message[field].strip():
            return False

    for field in ("message_id", "request_id"):
        try:
            identifier = uuid.UUID(message[field])
            if identifier.version != 4:
                return False
        except (ValueError, TypeError, AttributeError):
            return False

    if not isinstance(message["timestamp_ms"], int) or isinstance(message["timestamp_ms"], bool):
        return False
    if message["timestamp_ms"] <= 0:
        return False

    if not _validate_node_identity(message["source"]):
        return False
    if not _validate_node_identity(message["destination"]):
        return False

    if message["type"] == "command_redirect":
        if message["source"].get("node_role") != "master":
            return False
        if message["destination"].get("node_role") != "worker":
            return False

    if message["type"] in {"request_help", "response_accepted", "response_rejected", "confirm_loan", "loan_committed", "get_loan_status", "loan_status", "cancel_loan"}:
        if message["source"].get("node_role") != "master":
            return False
        if message["destination"].get("node_role") != "master":
            return False

    if message["type"] in {"command_release", "notify_worker_returned", "return_ack"}:
        if message["source"].get("node_role") not in {"master", "worker"}:
            return False
        if message["destination"].get("node_role") not in {"master", "worker"}:
            return False

    payload = message["payload"]
    if payload is None:
        return False

    if message["type"] == "protocol_error":
        if not isinstance(payload, dict):
            return False
        valid_error_codes = {
            "invalid_message",
            "invalid_state",
            "stale_epoch",
            "reservation_expired",
            "identity_conflict",
            "request_conflict",
            "unsupported_version",
            "unsupported_type",
        }
        if payload.get("error_code") not in valid_error_codes:
            return False
        if not isinstance(payload.get("error_message"), str) or not payload["error_message"].strip():
            return False

    if message["type"] == "registration_ack":
        if not isinstance(payload, dict):
            return False
        if not isinstance(payload.get("accepted"), bool):
            return False
        epoch = payload.get("assignment_epoch")
        if not isinstance(epoch, int) or isinstance(epoch, bool) or epoch < 0:
            return False
        if "reason" not in payload:
            return False
        if payload["accepted"]:
            if payload["reason"] is not None:
                return False
        elif not isinstance(payload["reason"], str) or not payload["reason"].strip():
            return False

    if message["type"] == "register_worker":
        ok, _ = validate_register_worker_payload(
            payload, require_address=not validate_official_node(message["source"], "worker"),
        )
        if not ok:
            return False

    if message["type"] in {"heartbeat", "heartbeat_ack"}:
        if not isinstance(payload, dict):
            return False
        sequence = payload.get("sequence")
        if not isinstance(sequence, int) or isinstance(sequence, bool):
            return False
        if message["type"] == "heartbeat":
            assignment_epoch = payload.get("assignment_epoch")
            if not isinstance(assignment_epoch, int) or isinstance(assignment_epoch, bool) or assignment_epoch < 0:
                return False
        if message["type"] == "heartbeat_ack" and payload.get("status") not in {"alive", "rejected"}:
            return False

    if message["type"] == "task_submit":
        if not validate_task_submit_payload(payload):
            return False

    if message["type"] == "task_submit_ack":
        if not validate_task_submit_ack_payload(payload):
            return False

    if message["type"] == "task_request":
        if not validate_task_request_payload(payload):
            return False

    if message["type"] == "task_assignment":
        if not validate_task_assignment_payload(payload):
            return False

    if message["type"] == "no_task":
        if not validate_no_task_payload(payload):
            return False

    if message["type"] == "task_result":
        if not validate_task_result_payload(payload):
            return False

    if message["type"] == "task_ack":
        if not validate_task_ack_payload(payload):
            return False

    if message["type"] == "metrics_request":
        if not validate_metrics_request_payload(payload):
            return False

    if message["type"] == "performance_report":
        identity_fields = set(REQUIRED_NODE_FIELDS)
        if not isinstance(message["source"], dict) or set(message["source"]) != identity_fields:
            return False
        if not isinstance(message["destination"], dict) or set(message["destination"]) != identity_fields:
            return False
        source = message["source"]
        destination = message["destination"]
        if not validate_official_node(source, "master") or not validate_official_node(destination, "collector"):
            return False
        if destination["node_id"] != COLLECTOR_NODE_ID or not validate_performance_report_payload(payload):
            return False
        for loan in payload["loans"]:
            if source["node_id"] == loan["home_master"]["node_id"]:
                states = REPORT_HOME_LOAN_STATES
            elif source["node_id"] == loan["target_master"]["node_id"]:
                states = REPORT_TARGET_LOAN_STATES
            else:
                return False
            if loan["loan_state"] not in states:
                return False

    if message["type"] == "metrics_response":
        if not validate_metrics_response_payload(payload):
            return False

    if message["type"] == "request_help":
        if not validate_request_help_payload(payload):
            return False

    if message["type"] == "response_accepted":
        if not validate_response_accepted_payload(payload):
            return False

    if message["type"] == "response_rejected":
        if not validate_response_rejected_payload(payload):
            return False

    if message["type"] == "confirm_loan":
        if not validate_confirm_loan_payload(payload):
            return False

    if message["type"] == "loan_committed":
        if not validate_loan_committed_payload(payload):
            return False

    if message["type"] == "command_redirect":
        if not validate_command_redirect_payload(payload):
            return False

    if message["type"] == "register_temporary_worker":
        if not validate_register_temporary_worker_payload(payload):
            return False
        loan = payload["loan"]
        if not all(message["source"].get(key) == loan["worker"][key]
                   and message["destination"].get(key) == loan["target_master"][key]
                   for key in REQUIRED_NODE_FIELDS):
            return False

    if message["type"] == "command_release":
        if not validate_command_release_payload(payload):
            return False

    if message["type"] == "notify_worker_returned":
        if not validate_notify_worker_returned_payload(payload):
            return False

    if message["type"] == "return_ack":
        if not validate_return_ack_payload(payload):
            return False

    if message["type"] == "get_loan_status":
        if not validate_get_loan_status_payload(payload):
            return False

    if message["type"] == "loan_status":
        if not validate_loan_status_payload(payload):
            return False

    if message["type"] == "cancel_loan":
        if not validate_cancel_loan_payload(payload):
            return False

    return True


def build_request_help_message(source: dict, destination: dict, *, requester_master: dict, workers_needed: int, tasks_pending: int, request_id: str | None = None) -> dict:
    if not _validate_master_identity(requester_master):
        raise ValueError("requester_master must be a valid master identity")
    if not isinstance(workers_needed, int) or isinstance(workers_needed, bool) or workers_needed < 1:
        raise ValueError("workers_needed must be a positive integer")
    if not isinstance(tasks_pending, int) or isinstance(tasks_pending, bool) or tasks_pending < 0:
        raise ValueError("tasks_pending must be a non-negative integer")

    message = {
        "version": "sd_2026_2_v1",
        "type": "request_help",
        "message_id": str(uuid.uuid4()),
        "request_id": request_id or str(uuid.uuid4()),
        "timestamp_ms": int(time.time() * 1000),
        "source": dict(source),
        "destination": dict(destination),
        "payload": {
            "requester_master": dict(requester_master),
            "workers_needed": int(workers_needed),
            "tasks_pending": int(tasks_pending),
        },
    }
    if not validate_message(message):
        raise ValueError("request_help message is invalid")
    return message


def build_response_accepted_message(request_message: dict, source: dict, destination: dict, *, loan: dict | None = None, reservation_timeout_ms: int = 15000, loan_id: str | None = None, requester_master: dict | None = None, target_master: dict | None = None, home_master: dict | None = None, worker: dict | None = None, assignment_epoch: int | None = None, loan_state: str = "offered", request_id: str | None = None) -> dict:
    if loan is None:
        loan = {
            "loan_id": str(loan_id),
            "loan_label": f"{re.sub(r'[^a-z0-9]+', '_', str(home_master['node_label']).lower()).strip('_')}_loan_000001",
            "worker": dict(worker or {}),
            "home_master": dict(home_master or {}),
            "target_master": dict(target_master or {}),
            "assignment_epoch": int(assignment_epoch or 0),
        }
    if not _validate_loan_ref(loan):
        raise ValueError("loan must be a valid loan_ref")
    if reservation_timeout_ms != 15000:
        raise ValueError("reservation_timeout_ms must be 15000")

    message = {
        "version": "sd_2026_2_v1",
        "type": "response_accepted",
        "message_id": str(uuid.uuid4()),
        "request_id": request_id or str(request_message.get("request_id") or uuid.uuid4()),
        "timestamp_ms": int(time.time() * 1000),
        "source": dict(source),
        "destination": dict(destination),
        "payload": {"loan": dict(loan), "reservation_timeout_ms": int(reservation_timeout_ms)},
    }
    if not validate_message(message):
        raise ValueError("response_accepted message is invalid")
    return message


def build_response_rejected_message(request_message: dict, source: dict, destination: dict, *, reason: str | dict, loan_id: str | None = None, request_id: str | None = None) -> dict:
    if isinstance(reason, dict):
        code = reason.get("code")
        reason = "refused" if code in {"capacity_exceeded", "refused"} else code
    if reason not in {"high_load", "no_workers_available", "min_home_workers", "refused"}:
        raise ValueError("reason must be high_load, no_workers_available, min_home_workers or refused")

    message = {
        "version": "sd_2026_2_v1",
        "type": "response_rejected",
        "message_id": str(uuid.uuid4()),
        "request_id": request_id or str(request_message.get("request_id") or uuid.uuid4()),
        "timestamp_ms": int(time.time() * 1000),
        "source": dict(source),
        "destination": dict(destination),
        "payload": {"reason": str(reason)},
    }
    if not validate_message(message):
        raise ValueError("response_rejected message is invalid")
    return message


def build_confirm_loan_message(request_message: dict, source: dict, destination: dict, *, loan_id: str, assignment_epoch: int, requester_master: dict | None = None, target_master: dict | None = None, home_master: dict | None = None, worker: dict | None = None, reservation_timeout_ms: int = 15000, loan_state: str = "committed", request_id: str | None = None) -> dict:
    if not _validate_loan_id(loan_id):
        raise ValueError("loan_id must be a valid UUIDv4")
    if not isinstance(assignment_epoch, int) or isinstance(assignment_epoch, bool) or assignment_epoch < 0:
        raise ValueError("assignment_epoch must be a non-negative integer")

    message = {
        "version": "sd_2026_2_v1",
        "type": "confirm_loan",
        "message_id": str(uuid.uuid4()),
        "request_id": request_id or str(uuid.uuid4()),
        "timestamp_ms": int(time.time() * 1000),
        "source": dict(source),
        "destination": dict(destination),
        "payload": {"loan_id": str(loan_id), "assignment_epoch": int(assignment_epoch)},
    }
    if not validate_message(message):
        raise ValueError("confirm_loan message is invalid")
    return message


def build_loan_committed_message(request_message: dict, source: dict, destination: dict, *, loan: dict | None = None, loan_state: str = "committed", loan_id: str | None = None, requester_master: dict | None = None, target_master: dict | None = None, home_master: dict | None = None, worker: dict | None = None, assignment_epoch: int | None = None, reservation_timeout_ms: int = 15000, request_id: str | None = None) -> dict:
    if loan is None:
        loan = {
            "loan_id": str(loan_id),
            "loan_label": f"{re.sub(r'[^a-z0-9]+', '_', str(home_master['node_label']).lower()).strip('_')}_loan_000001",
            "worker": dict(worker or {}),
            "home_master": dict(home_master or {}),
            "target_master": dict(target_master or {}),
            "assignment_epoch": int(assignment_epoch or 0),
        }
    if not _validate_loan_ref(loan) or loan_state != "committed":
        raise ValueError("loan_committed must contain a valid loan and committed state")

    message = {
        "version": "sd_2026_2_v1",
        "type": "loan_committed",
        "message_id": str(uuid.uuid4()),
        "request_id": request_id or str(request_message.get("request_id") or uuid.uuid4()),
        "timestamp_ms": int(time.time() * 1000),
        "source": dict(source),
        "destination": dict(destination),
        "payload": {"loan": dict(loan), "loan_state": "committed"},
    }
    if not validate_message(message):
        raise ValueError("loan_committed message is invalid")
    return message


def build_command_redirect_message(request_message: dict, source: dict, destination: dict, *, loan: dict | None = None, loan_id: str | None = None, worker: dict | None = None, target_host: str | None = None, target_port: int | None = None, assignment_epoch: int | None = None, reason: str | None = None, target_master: dict | None = None, home_master: dict | None = None, home: dict | None = None, request_id: str | None = None) -> dict:
    if loan is None:
        loan = {
            "loan_id": str(loan_id),
            "loan_label": f"{re.sub(r'[^a-z0-9]+', '_', str((home_master or home or source)['node_label']).lower()).strip('_')}_loan_000001",
            "worker": dict(worker or {}),
            "home_master": dict(home_master or home or source),
            "target_master": dict(target_master or destination),
            "assignment_epoch": int(assignment_epoch or 0),
        }
    if not _validate_loan_ref(loan):
        raise ValueError("loan must be a valid loan_ref")

    message = {
        "version": "sd_2026_2_v1",
        "type": "command_redirect",
        "message_id": str(uuid.uuid4()),
        "request_id": request_id or str(request_message.get("request_id") or uuid.uuid4()),
        "timestamp_ms": int(time.time() * 1000),
        "source": dict(source),
        "destination": dict(destination),
        "payload": {"loan": dict(loan)},
    }
    if not validate_message(message):
        raise ValueError("command_redirect message is invalid")
    return message


def build_register_temporary_worker_message(request_message: dict, source: dict, destination: dict, *, loan: dict | None = None, loan_id: str | None = None, requester_master: dict | None = None, target_master: dict | None = None, home_master: dict | None = None, worker: dict | None = None, assignment_epoch: int | None = None, reservation_timeout_ms: int = 15000, loan_state: str = "offered", request_id: str | None = None) -> dict:
    if loan is None:
        loan = {
            "loan_id": str(loan_id),
            "loan_label": f"{re.sub(r'[^a-z0-9]+', '_', str(home_master['node_label']).lower()).strip('_')}_loan_000001",
            "worker": dict(worker or {}),
            "home_master": dict(home_master or {}),
            "target_master": dict(target_master or {}),
            "assignment_epoch": int(assignment_epoch or 0),
        }
    if not _validate_loan_ref(loan):
        raise ValueError("loan must be a valid loan_ref")

    message = {
        "version": "sd_2026_2_v1",
        "type": "register_temporary_worker",
        "message_id": str(uuid.uuid4()),
        "request_id": request_id or str(request_message.get("request_id") or uuid.uuid4()),
        "timestamp_ms": int(time.time() * 1000),
        "source": dict(source),
        "destination": dict(destination),
        "payload": {"loan": dict(loan)},
    }
    if not validate_message(message):
        raise ValueError("register_temporary_worker message is invalid")
    return message


def build_command_release_message(request_message: dict, source: dict, destination: dict, *, loan_id: str, assignment_epoch: int | None = None, reason: str | None = None, request_id: str | None = None) -> dict:
    if not isinstance(request_message, dict):
        raise ValueError("request_message must be a dict")
    if not _validate_loan_id(loan_id):
        raise ValueError("loan_id must be a valid UUIDv4")
    if not isinstance(assignment_epoch, int) or isinstance(assignment_epoch, bool) or assignment_epoch < 0:
        raise ValueError("assignment_epoch must be a non-negative integer")
    if not isinstance(reason, str) or not reason.strip():
        raise ValueError("reason is required")

    message = {
        "version": "sd_2026_2_v1",
        "type": "command_release",
        "message_id": str(uuid.uuid4()),
        "request_id": request_id or str(request_message.get("request_id") or uuid.uuid4()),
        "timestamp_ms": int(time.time() * 1000),
        "source": dict(source),
        "destination": dict(destination),
        "payload": {
            "loan_id": str(loan_id),
            "assignment_epoch": int(assignment_epoch),
            "reason": str(reason),
        },
    }
    if not validate_message(message):
        raise ValueError("command_release message is invalid")
    return message


def build_notify_worker_returned_message(request_message: dict, source: dict, destination: dict, *, loan_id: str, loan_label: str, worker: dict, assignment_epoch: int, loan_state: str = "closed", request_id: str | None = None) -> dict:
    if not _validate_loan_id(loan_id):
        raise ValueError("loan_id must be a valid UUIDv4")
    if not _validate_loan_label(loan_label):
        raise ValueError("loan_label must be lower_snake_case")
    if not _validate_worker_ref(worker):
        raise ValueError("worker must be a valid worker_ref")
    if not isinstance(assignment_epoch, int) or isinstance(assignment_epoch, bool) or assignment_epoch < 0:
        raise ValueError("assignment_epoch must be a non-negative integer")

    message = {
        "version": "sd_2026_2_v1",
        "type": "notify_worker_returned",
        "message_id": str(uuid.uuid4()),
        "request_id": request_id or str(request_message.get("request_id") or uuid.uuid4()),
        "timestamp_ms": int(time.time() * 1000),
        "source": dict(source),
        "destination": dict(destination),
        "payload": {
            "loan_id": str(loan_id),
            "loan_label": str(loan_label),
            "worker": dict(worker),
            "assignment_epoch": int(assignment_epoch),
            "loan_state": "closed",
        },
    }
    if not validate_message(message):
        raise ValueError("notify_worker_returned message is invalid")
    return message


def build_return_ack_message(request_message: dict, source: dict, destination: dict, *, loan_id: str, loan_state: str = "closed", worker: dict | None = None, assignment_epoch: int | None = None, status: str | None = None, request_id: str | None = None) -> dict:
    if not _validate_loan_id(loan_id):
        raise ValueError("loan_id must be a valid UUIDv4")

    message = {
        "version": "sd_2026_2_v1",
        "type": "return_ack",
        "message_id": str(uuid.uuid4()),
        "request_id": request_id or str(request_message.get("request_id") or uuid.uuid4()),
        "timestamp_ms": int(time.time() * 1000),
        "source": dict(source),
        "destination": dict(destination),
        "payload": {"loan_id": str(loan_id), "loan_state": "closed"},
    }
    if not validate_message(message):
        raise ValueError("return_ack message is invalid")
    return message


def build_get_loan_status_message(source: dict, destination: dict, *, loan_id: str, requester_master: dict | None = None, target_master: dict | None = None, request_id: str | None = None) -> dict:
    if not _validate_loan_id(loan_id):
        raise ValueError("loan_id must be a valid UUIDv4")
    if requester_master is not None and not _validate_master_identity(requester_master):
        raise ValueError("requester_master must be a valid master identity")
    if target_master is not None and not _validate_master_identity(target_master):
        raise ValueError("target_master must be a valid master identity")

    payload = {"loan_id": str(loan_id)}
    if requester_master is not None:
        payload["requester_master"] = dict(requester_master)
    if target_master is not None:
        payload["target_master"] = dict(target_master)

    message = {
        "version": "sd_2026_2_v1",
        "type": "get_loan_status",
        "message_id": str(uuid.uuid4()),
        "request_id": request_id or str(uuid.uuid4()),
        "timestamp_ms": int(time.time() * 1000),
        "source": dict(source),
        "destination": dict(destination),
        "payload": payload,
    }
    if not validate_message(message):
        raise ValueError("get_loan_status message is invalid")
    return message


def build_loan_status_message(request_message: dict, source: dict, destination: dict, *, loan_id: str, loan_state: str, loan: dict | None = None, requester_master: dict | None = None, target_master: dict | None = None, home_master: dict | None = None, worker: dict | None = None, assignment_epoch: int | None = None, request_id: str | None = None) -> dict:
    if not _validate_loan_id(loan_id):
        raise ValueError("loan_id must be a valid UUIDv4")
    if loan_state not in {"offered", "committed", "closed", "cancelled", "expired", "unknown"}:
        raise ValueError("loan_state is invalid")
    if loan is not None and not _validate_loan_ref(loan):
        raise ValueError("loan must be a valid loan_ref or null")

    message = {
        "version": "sd_2026_2_v1",
        "type": "loan_status",
        "message_id": str(uuid.uuid4()),
        "request_id": request_id or str(request_message.get("request_id") or uuid.uuid4()),
        "timestamp_ms": int(time.time() * 1000),
        "source": dict(source),
        "destination": dict(destination),
        "payload": {"loan_id": str(loan_id), "loan_state": str(loan_state), "loan": dict(loan) if loan is not None else None},
    }
    if not validate_message(message):
        raise ValueError("loan_status message is invalid")
    return message


def build_cancel_loan_message(request_message: dict, source: dict, destination: dict, *, loan_id: str, assignment_epoch: int | None = None, reason: str | None = None, request_id: str | None = None) -> dict:
    if not _validate_loan_id(loan_id):
        raise ValueError("loan_id must be a valid UUIDv4")

    message = {
        "version": "sd_2026_2_v1",
        "type": "cancel_loan",
        "message_id": str(uuid.uuid4()),
        "request_id": request_id or str(uuid.uuid4()),
        "timestamp_ms": int(time.time() * 1000),
        "source": dict(source),
        "destination": dict(destination),
        "payload": {"loan_id": str(loan_id)},
    }
    if not validate_message(message):
        raise ValueError("cancel_loan message is invalid")
    return message


def build_task_submit_message(source: dict, destination: dict, task: dict, request_id: str | None = None) -> dict:
    if not isinstance(task, dict):
        raise ValueError("task payload must be a dict")
    if not validate_task_submit_payload(task):
        raise ValueError("task payload does not match sum_range contract")

    return {
        "version": "sd_2026_2_v1",
        "type": "task_submit",
        "message_id": str(uuid.uuid4()),
        "request_id": request_id or str(uuid.uuid4()),
        "timestamp_ms": int(time.time() * 1000),
        "source": dict(source),
        "destination": dict(destination),
        "payload": dict(task),
    }


def build_task_submit_ack_message(request_message: dict, source: dict, destination: dict, *, task_id: str, task_label: str, accepted: bool, reason: str) -> dict:
    if not isinstance(request_message, dict):
        raise ValueError("request_message must be a dict")
    if not isinstance(task_id, str) or not task_id.strip():
        raise ValueError("task_id is required")
    if not isinstance(task_label, str) or not task_label.strip():
        raise ValueError("task_label is required")
    if not isinstance(accepted, bool):
        raise ValueError("accepted must be a bool")
    if not isinstance(reason, str) or not reason.strip():
        raise ValueError("reason is required")

    return {
        "version": "sd_2026_2_v1",
        "type": "task_submit_ack",
        "message_id": str(uuid.uuid4()),
        "request_id": str(request_message["request_id"]),
        "timestamp_ms": int(time.time() * 1000),
        "source": dict(source),
        "destination": dict(destination),
        "payload": {
            "task_id": str(task_id),
            "task_label": str(task_label),
            "accepted": bool(accepted),
            "reason": str(reason),
        },
    }


def build_task_request_message(source: dict, destination: dict, *, assignment_epoch: int, loan_id: str | None, request_id: str | None = None) -> dict:
    if not isinstance(assignment_epoch, int) or isinstance(assignment_epoch, bool) or assignment_epoch < 0:
        raise ValueError("assignment_epoch must be a non-negative integer")
    if loan_id is not None and (not isinstance(loan_id, str) or not loan_id.strip()):
        raise ValueError("loan_id must be null or a non-empty string")

    return {
        "version": "sd_2026_2_v1",
        "type": "task_request",
        "message_id": str(uuid.uuid4()),
        "request_id": request_id or str(uuid.uuid4()),
        "timestamp_ms": int(time.time() * 1000),
        "source": dict(source),
        "destination": dict(destination),
        "payload": {"assignment_epoch": int(assignment_epoch), "loan_id": loan_id},
    }


def build_task_assignment_message(request_message: dict, source: dict, destination: dict, *, task_id: str, task_label: str, attempt_id: str, task_kind: str, parameters: dict, assignment_epoch: int, loan_id: str | None) -> dict:
    if not validate_task_submit_payload({"task_id": task_id, "task_kind": task_kind, "parameters": parameters}):
        raise ValueError("invalid task assignment payload")
    if not isinstance(attempt_id, str) or not attempt_id.strip():
        raise ValueError("attempt_id is required")
    if not isinstance(assignment_epoch, int) or isinstance(assignment_epoch, bool) or assignment_epoch < 0:
        raise ValueError("assignment_epoch must be a non-negative integer")
    if loan_id is not None and (not isinstance(loan_id, str) or not loan_id.strip()):
        raise ValueError("loan_id must be null or a non-empty string")

    return {
        "version": "sd_2026_2_v1",
        "type": "task_assignment",
        "message_id": str(uuid.uuid4()),
        "request_id": str(request_message["request_id"]),
        "timestamp_ms": int(time.time() * 1000),
        "source": dict(source),
        "destination": dict(destination),
        "payload": {
            "task_id": str(task_id),
            "task_label": str(task_label),
            "attempt_id": str(attempt_id),
            "task_kind": str(task_kind),
            "parameters": dict(parameters),
            "assignment_epoch": int(assignment_epoch),
            "loan_id": loan_id,
        },
    }


def build_no_task_message(request_message: dict, source: dict, destination: dict, *, retry_after_ms: int = 1000) -> dict:
    if not isinstance(retry_after_ms, int) or isinstance(retry_after_ms, bool) or retry_after_ms < 0:
        raise ValueError("retry_after_ms must be a non-negative integer")

    return {
        "version": "sd_2026_2_v1",
        "type": "no_task",
        "message_id": str(uuid.uuid4()),
        "request_id": str(request_message["request_id"]),
        "timestamp_ms": int(time.time() * 1000),
        "source": dict(source),
        "destination": dict(destination),
        "payload": {"retry_after_ms": int(retry_after_ms)},
    }


def build_performance_report_message(
    source: dict,
    destination: dict,
    *,
    metrics: dict,
    request_id: str | None = None,
) -> dict:
    identity_fields = set(REQUIRED_NODE_FIELDS)
    if not isinstance(source, dict) or set(source) != identity_fields:
        raise ValueError("performance_report source must contain exactly four identity fields")
    if not isinstance(destination, dict) or set(destination) != identity_fields:
        raise ValueError("performance_report destination must contain exactly four identity fields")
    if not validate_performance_report_payload(metrics):
        raise ValueError("performance_report payload inv?lido")

    message = {
        "version": "sd_2026_2_v1",
        "type": "performance_report",
        "message_id": str(uuid.uuid4()),
        "request_id": request_id or str(uuid.uuid4()),
        "timestamp_ms": int(time.time() * 1000),
        "source": dict(source),
        "destination": dict(destination),
        "payload": dict(metrics),
    }

    if not validate_message(message):
        raise ValueError("performance_report message inv?lida")

    return message


def build_metrics_request_message(source: dict, destination: dict, *, request_id: str | None = None) -> dict:
    return {
        "version": "sd_2026_2_v1",
        "type": "metrics_request",
        "message_id": str(uuid.uuid4()),
        "request_id": request_id or str(uuid.uuid4()),
        "timestamp_ms": int(time.time() * 1000),
        "source": dict(source),
        "destination": dict(destination),
        "payload": {},
    }


def build_metrics_response_message(request_message: dict, source: dict, destination: dict, *, metrics: dict) -> dict:
    if not validate_metrics_response_payload(metrics):
        raise ValueError("metrics payload does not match the snapshot contract")

    return {
        "version": "sd_2026_2_v1",
        "type": "metrics_response",
        "message_id": str(uuid.uuid4()),
        "request_id": str(request_message["request_id"]),
        "timestamp_ms": int(time.time() * 1000),
        "source": dict(source),
        "destination": dict(destination),
        "payload": dict(metrics),
    }


def build_task_result_message(request_message: dict, source: dict, destination: dict, *, task_id: str, task_label: str, attempt_id: str, assignment_epoch: int, loan_id: str | None, status: str, result: dict | None, error_code: str | None) -> dict:
    if not isinstance(task_id, str) or not task_id.strip():
        raise ValueError("task_id is required")
    if not isinstance(task_label, str) or not task_label.strip():
        raise ValueError("task_label is required")
    if not isinstance(attempt_id, str) or not attempt_id.strip():
        raise ValueError("attempt_id is required")
    if not isinstance(assignment_epoch, int) or isinstance(assignment_epoch, bool) or assignment_epoch < 0:
        raise ValueError("assignment_epoch must be a non-negative integer")
    if loan_id is not None and (not isinstance(loan_id, str) or not loan_id.strip()):
        raise ValueError("loan_id must be null or a non-empty string")
    if status not in {"ok", "nok"}:
        raise ValueError("status must be 'ok' or 'nok'")
    if status == "ok":
        if not isinstance(result, dict) or "sum" not in result or not isinstance(result["sum"], int):
            raise ValueError("successful task_result requires a dict result with int sum")
        error_code = None
    else:
        if result is not None:
            raise ValueError("failed task_result requires result to be null")
        if error_code is not None and (not isinstance(error_code, str) or not error_code.strip()):
            raise ValueError("error_code must be null or a non-empty string")

    return {
        "version": "sd_2026_2_v1",
        "type": "task_result",
        "message_id": str(uuid.uuid4()),
        "request_id": str(request_message["request_id"]),
        "timestamp_ms": int(time.time() * 1000),
        "source": dict(source),
        "destination": dict(destination),
        "payload": {
            "task_id": str(task_id),
            "task_label": str(task_label),
            "attempt_id": str(attempt_id),
            "assignment_epoch": int(assignment_epoch),
            "loan_id": loan_id,
            "status": str(status),
            "result": None if status == "nok" else {"sum": int(result["sum"])},
            "error_code": error_code,
        },
    }


def build_task_ack_message(request_message: dict, source: dict, destination: dict, *, task_id: str, attempt_id: str, accepted: bool, reason: str) -> dict:
    if not isinstance(task_id, str) or not task_id.strip():
        raise ValueError("task_id is required")
    if not isinstance(attempt_id, str) or not attempt_id.strip():
        raise ValueError("attempt_id is required")
    if not isinstance(accepted, bool):
        raise ValueError("accepted must be a bool")
    if not isinstance(reason, str) or not reason.strip():
        raise ValueError("reason is required")

    return {
        "version": "sd_2026_2_v1",
        "type": "task_ack",
        "message_id": str(uuid.uuid4()),
        "request_id": str(request_message["request_id"]),
        "timestamp_ms": int(time.time() * 1000),
        "source": dict(source),
        "destination": dict(destination),
        "payload": {
            "task_id": str(task_id),
            "attempt_id": str(attempt_id),
            "accepted": bool(accepted),
            "reason": str(reason),
        },
    }


def build_heartbeat_message(source: dict, destination: dict, sequence: int, request_id: str | None = None, assignment_epoch: int = 0) -> dict:
    if not isinstance(sequence, int) or isinstance(sequence, bool):
        raise ValueError("heartbeat sequence must be an integer")
    if not isinstance(assignment_epoch, int) or isinstance(assignment_epoch, bool) or assignment_epoch < 0:
        raise ValueError("heartbeat assignment_epoch must be a non-negative integer")

    message_id = str(uuid.uuid4())
    return {
        "version": "sd_2026_2_v1",
        "type": "heartbeat",
        "message_id": message_id,
        "request_id": request_id or str(uuid.uuid4()),
        "timestamp_ms": int(time.time() * 1000),
        "source": dict(source),
        "destination": dict(destination),
        "payload": {"sequence": int(sequence), "assignment_epoch": int(assignment_epoch)},
    }


def build_heartbeat_ack_message(request_message: dict, source: dict, destination: dict) -> dict:
    payload = request_message.get("payload", {}) if isinstance(request_message.get("payload", {}), dict) else {}
    sequence = payload.get("sequence", 0)
    return {
        "version": "sd_2026_2_v1",
        "type": "heartbeat_ack",
        "message_id": str(uuid.uuid4()),
        "request_id": request_message["request_id"],
        "timestamp_ms": int(time.time() * 1000),
        "source": dict(source),
        "destination": dict(destination),
        "payload": {"sequence": int(sequence), "status": "alive"},
    }


def build_protocol_error_message(message: dict, master_identity: dict, reason: str) -> dict:
    if not isinstance(message, dict):
        raise ValueError("protocol_error requires a message dict")

    original_request_id = message.get("request_id")
    try:
        parsed_request_id = uuid.UUID(original_request_id)
        if (
            not isinstance(original_request_id, str)
            or parsed_request_id.version != 4
            or str(parsed_request_id) != original_request_id
        ):
            raise ValueError("invalid request_id")
        request_id = original_request_id
    except (ValueError, TypeError, AttributeError):
        request_id = str(uuid.uuid4())

    source = message.get("source") if isinstance(message.get("source"), dict) else {}
    destination = message.get("destination") if isinstance(message.get("destination"), dict) else {}
    unsafe_source = {
        "group_label": str(source.get("group_label", "unknown")),
        "node_id": str(source.get("node_id", "00000000-0000-4000-8000-000000000000")),
        "node_label": str(source.get("node_label", "unknown")),
        "node_role": str(source.get("node_role", "unknown")),
    }
    unsafe_destination = {
        "group_label": str(destination.get("group_label", "unknown")),
        "node_id": str(destination.get("node_id", "00000000-0000-4000-8000-000000000000")),
        "node_label": str(destination.get("node_label", "unknown")),
        "node_role": str(destination.get("node_role", "unknown")),
    }
    return {
        "version": "sd_2026_2_v1",
        "type": "protocol_error",
        "message_id": str(uuid.uuid4()),
        "request_id": request_id,
        "timestamp_ms": int(time.time() * 1000),
        "source": {
            "group_label": str(master_identity.get("group_label", "p2p-cluster")),
            "node_id": str(master_identity.get("node_id", "00000000-0000-4000-8000-000000000000")),
            "node_label": str(master_identity.get("node_label", "master")),
            "node_role": str(master_identity.get("node_role", "master")),
        },
        "destination": unsafe_source or unsafe_destination,
        "payload": {
            "error_code": (
                "unsupported_version" if "unknown protocol version" in str(reason)
                else "unsupported_type" if "unknown message type" in str(reason)
                else "invalid_message"
            ),
            "error_message": str(reason),
        },
    }


def classify_protocol_error(message: dict) -> str:
    if not isinstance(message, dict):
        return "invalid message payload"

    required_fields = [field for field in REQUIRED_MESSAGE_FIELDS if field not in message]
    if required_fields:
        return f"missing required field(s): {', '.join(required_fields)}"

    if "Type" in message and "type" not in message:
        return "invalid field capitalization: use 'type' instead of 'Type'"
    if "Version" in message and "version" not in message:
        return "invalid field capitalization: use 'version' instead of 'Version'"

    if "type" in message and isinstance(message["type"], str):
        if message["type"] not in {"register_worker", "registration_ack", "heartbeat", "heartbeat_ack", "protocol_error", "task_submit", "task_submit_ack", "task_request", "task_assignment", "no_task", "task_result", "task_ack", "metrics_request", "metrics_response", "performance_report", "request_help", "response_accepted", "response_rejected", "confirm_loan", "loan_committed", "command_redirect", "register_temporary_worker", "command_release", "notify_worker_returned", "return_ack", "get_loan_status", "loan_status", "cancel_loan"}:
            return f"unknown message type: {message['type']}"

    if "version" in message and isinstance(message["version"], str):
        if message["version"] != "sd_2026_2_v1":
            return f"unknown protocol version: {message['version']}"

    if message.get("type") == "register_worker":
        payload = message.get("payload")
        ok, reason = validate_register_worker_payload(payload)
        if not ok:
            return reason or "invalid register_worker payload"

    return "invalid protocol envelope"


def extract_complete_messages(buffer: bytearray) -> List[dict]:
    messages: List[dict] = []

    while True:
        newline_index = buffer.find(b"\n")
        if newline_index == -1:
            break

        raw_line = bytes(buffer[:newline_index])
        del buffer[: newline_index + 1]

        if not raw_line.strip():
            continue

        if len(raw_line) > MAX_NDJSON_MESSAGE_BYTES:
            log_event("ASD", "unknown", "unknown", "invalid_json", "unknown", "discarded")
            continue

        try:
            decoded = raw_line.decode("utf-8")
        except UnicodeDecodeError:
            log_event("ASD", "unknown", "unknown", "invalid_json", "unknown", "discarded")
            continue

        try:
            payload = json.loads(decoded, object_pairs_hook=_reject_duplicate_keys, parse_constant=_reject_special_float)
        except (json.JSONDecodeError, ValueError):
            log_event("ASD", "unknown", "unknown", "invalid_json", "unknown", "discarded")
            continue

        if not isinstance(payload, dict):
            log_event("ASD", "unknown", "unknown", "invalid_message", "unknown", "discarded")
            continue

        source = payload.get("source", {}) if isinstance(payload, dict) else {}
        source_label = source.get("node_label", "unknown") if isinstance(source, dict) else "unknown"
        msg_type = payload.get("type", "invalid_message") if isinstance(payload, dict) else "invalid_message"
        request_id = payload.get("request_id", "unknown") if isinstance(payload, dict) else "unknown"

        if not validate_message(payload):
            log_event("ASD", source_label, "unknown", msg_type, request_id, "discarded")

        messages.append(payload)

    return messages


def send_msg(sock: socket.socket, message: dict) -> None:
    encoded = (json.dumps(message, separators=(",", ":"), ensure_ascii=False) + "\n").encode("utf-8")
    sock.sendall(encoded)


def recv_msgs(sock: socket.socket, buffer: bytearray) -> List[dict] | None:
    try:
        chunk = sock.recv(4096)
    except socket.timeout:
        return []

    if not chunk:
        return None

    buffer.extend(chunk)

    messages = extract_complete_messages(buffer)

    if len(buffer) > MAX_NDJSON_MESSAGE_BYTES:
        buffer.clear()
        raise ValueError(
            "NDJSON buffer exceeded 65536 bytes without newline"
        )

    return messages


class MasterRegistry:
    def __init__(self):
        self.workers = {}
        self.node_labels = {}
        self.processed_message_keys = {}
        self.processed_message_contents = {}
        self.active_sessions = {}
        self._lock = threading.RLock()

    def bind_session(self, node_id: str, conn) -> object | None:
        with self._lock:
            previous = self.active_sessions.get(node_id)
            self.active_sessions[node_id] = conn
            return previous if previous is not conn else None

    def is_active_session(self, node_id: str, conn) -> bool:
        with self._lock:
            return self.active_sessions.get(node_id) is conn

    def release_session(self, node_id: str, conn) -> bool:
        with self._lock:
            if self.active_sessions.get(node_id) is not conn:
                return False
            del self.active_sessions[node_id]
            return True

    def remember_processed_message(self, message: dict, response: dict) -> dict:
        with self._lock:
            if not isinstance(message, dict):
                return response
            source = message.get("source")
            if not isinstance(source, dict):
                return response
            key = (str(source.get("node_id", "")), str(message.get("request_id", "")), str(message.get("type", "")))
            self.processed_message_keys[key] = response
            self.processed_message_contents[key] = json.dumps(
                {
                    "source": message.get("source"),
                    "destination": message.get("destination"),
                    "payload": message.get("payload"),
                },
                sort_keys=True,
                ensure_ascii=False,
            )
            return response

    def get_processed_response(self, message: dict) -> dict | None:
        with self._lock:
            if not isinstance(message, dict):
                return None
            source = message.get("source")
            if not isinstance(source, dict):
                return None
            key = (str(source.get("node_id", "")), str(message.get("request_id", "")), str(message.get("type", "")))
            return self.processed_message_keys.get(key)

    def has_request_conflict(self, message: dict) -> bool:
        with self._lock:
            if not isinstance(message, dict):
                return False
            source = message.get("source")
            if not isinstance(source, dict):
                return False

            key = (
                str(source.get("node_id", "")),
                str(message.get("request_id", "")),
                str(message.get("type", "")),
            )
            original = self.processed_message_contents.get(key)
            if original is None:
                return False

            current = json.dumps(
                {
                    "source": message.get("source"),
                    "destination": message.get("destination"),
                    "payload": message.get("payload"),
                },
                sort_keys=True,
                ensure_ascii=False,
            )
            return current != original

    def upsert_worker(self, worker: dict) -> dict:
        with self._lock:
            uuid_value = worker.get("node_id") or worker.get("uuid")
            if not uuid_value:
                raise ValueError("worker UUID is required")

            candidate = dict(worker)
            candidate["node_id"] = str(uuid_value)
            candidate["node_label"] = candidate.get("node_label") or candidate.get("label")
            candidate["group_label"] = candidate.get("group_label")
            candidate["node_role"] = candidate.get("node_role")
            candidate["status"] = candidate.get("status", "available")
            candidate["heartbeat_misses"] = int(candidate.get("heartbeat_misses", 0) or 0)
            candidate["expected_heartbeat_request_id"] = None
            candidate["expected_heartbeat_sequence"] = None

            existing = self.workers.get(candidate["node_id"])
            if existing:
                incompatible = (
                    existing.get("node_label") != candidate.get("node_label")
                    or existing.get("group_label") != candidate.get("group_label")
                    or existing.get("node_role") != candidate.get("node_role")
                )
                if incompatible:
                    raise ValueError("same node_id already exists with incompatible identity")
                self.workers[candidate["node_id"]] = {**existing, **candidate}
                return self.workers[candidate["node_id"]]

            existing_label_owner = self.node_labels.get(candidate.get("node_label"))
            if candidate.get("node_label") and existing_label_owner and existing_label_owner != candidate["node_id"]:
                raise ValueError("node_label already registered for another worker")

            self.workers[candidate["node_id"]] = candidate
            if candidate.get("node_label"):
                self.node_labels[candidate["node_label"]] = candidate["node_id"]
            return self.workers[candidate["node_id"]]

    def mark_session_unavailable(
        self, node_id: str, conn, reason: str = "socket closed"
    ) -> bool:
        with self._lock:
            if self.active_sessions.get(node_id) is not conn:
                return False
            self.mark_worker_unavailable(node_id, reason=reason)
            return True

    def mark_worker_unavailable(self, node_id: str, reason: str = "heartbeat timeout") -> dict | None:
        with self._lock:
            worker = self.workers.get(node_id)
            if worker is None:
                return None
            worker["status"] = "unavailable"
            worker["heartbeat_misses"] = max(int(worker.get("heartbeat_misses", 0) or 0), 3)
            worker["expected_heartbeat_request_id"] = None
            worker["expected_heartbeat_sequence"] = None
            log_event("ASD", worker.get("node_label", "unknown"), "master", "heartbeat", reason, "unavailable")
            return worker

    def record_heartbeat_miss(self, node_id: str) -> bool:
        with self._lock:
            worker = self.workers.get(node_id)
            if worker is None:
                return False
            if worker.get("status") == "unavailable":
                return False

            worker["heartbeat_misses"] = int(worker.get("heartbeat_misses", 0) or 0) + 1
            if worker["heartbeat_misses"] >= 3:
                worker["status"] = "unavailable"
                worker["expected_heartbeat_request_id"] = None
                worker["expected_heartbeat_sequence"] = None
                log_event("ASD", worker.get("node_label", "unknown"), "master", "heartbeat", "timeout", "unavailable")
                return True
            worker["status"] = "suspect"
            return False

    def begin_heartbeat_expectation(self, node_id: str, request_id: str, sequence: int) -> dict:
        with self._lock:
            worker = self.workers.get(node_id)
            if worker is None:
                raise ValueError(f"unknown worker: {node_id}")

            loan_state = str(worker.get("loan_state", ""))
            protected_status = str(worker.get("status", "")) in {"reserved", "committed", "offered"} or loan_state in {"offered", "committed"}

            if worker.get("expected_heartbeat_request_id") is not None and worker.get("expected_heartbeat_sequence") is not None:
                worker["heartbeat_misses"] = int(worker.get("heartbeat_misses", 0) or 0) + 1
                if worker["heartbeat_misses"] >= 3:
                    if not protected_status:
                        worker["status"] = "unavailable"
                    worker["expected_heartbeat_request_id"] = None
                    worker["expected_heartbeat_sequence"] = None
                    worker["expected_heartbeat_request_id"] = str(request_id)
                    worker["expected_heartbeat_sequence"] = int(sequence)
                    return worker
                if not protected_status:
                    worker["status"] = "suspect"
                worker["expected_heartbeat_request_id"] = None
                worker["expected_heartbeat_sequence"] = None

            if not protected_status:
                if worker.get("heartbeat_misses", 0) >= 3:
                    worker["status"] = "unavailable"
                elif int(worker.get("heartbeat_misses", 0) or 0) > 0:
                    worker["status"] = "suspect"
                else:
                    worker["status"] = "available"
            worker["expected_heartbeat_request_id"] = str(request_id)
            worker["expected_heartbeat_sequence"] = int(sequence)
            return worker

    def process_heartbeat_ack(self, node_id: str, request_id: str, sequence: int, status: str) -> bool:
        with self._lock:
            worker = self.workers.get(node_id)
            if worker is None:
                return False

            expected_id = worker.get("expected_heartbeat_request_id")
            expected_sequence = worker.get("expected_heartbeat_sequence")
            valid = (
                expected_id is not None
                and expected_sequence is not None
                and request_id == expected_id
                and int(sequence) == int(expected_sequence)
                and str(status) == "alive"
            )

            if valid:
                worker["heartbeat_misses"] = 0
                loan_state = str(worker.get("loan_state", ""))
                protected_status = str(worker.get("status", "")) in {"reserved", "committed", "offered"} or loan_state in {"offered", "committed"}
                if not protected_status:
                    worker["status"] = "available"
                worker["expected_heartbeat_request_id"] = None
                worker["expected_heartbeat_sequence"] = None
                return True

            if expected_id is not None and expected_sequence is not None:
                worker["heartbeat_misses"] = int(worker.get("heartbeat_misses", 0) or 0) + 1
                worker["expected_heartbeat_request_id"] = None

                worker["expected_heartbeat_sequence"] = None
                if worker["heartbeat_misses"] >= 3:
                    if str(worker.get("status", "")) not in {"reserved", "committed", "offered"}:
                        worker["status"] = "unavailable"
                else:
                    if str(worker.get("status", "")) not in {"reserved", "committed", "offered"}:
                        worker["status"] = "suspect"
                return False



            return False

    def get_worker(self, uuid_value: str):
        with self._lock:
            return self.workers.get(uuid_value)

    def remove_worker(self, uuid_value: str) -> None:
        with self._lock:
            worker = self.workers.pop(uuid_value, None)
            if worker and worker.get("node_label") in self.node_labels:
                if self.node_labels.get(worker["node_label"]) == uuid_value:
                    del self.node_labels[worker["node_label"]]

