import json
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

    if node["node_role"] not in {"master", "worker"}:
        return False

    return True


def validate_register_worker_payload(payload: Any) -> tuple[bool, str | None]:
    if not isinstance(payload, dict):
        return False, "invalid payload"

    required_fields = ("home_master", "assignment_epoch", "loan_id", "host", "port")
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
    if not isinstance(host, str) or not host.strip():
        return False, "host must be a non-empty string"

    port = payload.get("port")
    if not isinstance(port, int) or isinstance(port, bool) or not (1 <= port <= 65535):
        return False, "port must be an integer between 1 and 65535"

    return True, None


def validate_message(message: Any) -> bool:
    if not isinstance(message, dict):
        return False

    for field in REQUIRED_MESSAGE_FIELDS:
        if field not in message:
            return False

    if message["version"] != "sd_2026_2_v1":
        return False

    if message.get("type") not in {"register_worker", "registration_ack", "heartbeat", "heartbeat_ack", "protocol_error"}:
        return False

    for field in ("type", "message_id", "request_id"):
        if not isinstance(message[field], str) or not message[field].strip():
            return False

    if not isinstance(message["timestamp_ms"], int) or isinstance(message["timestamp_ms"], bool):
        return False
    if message["timestamp_ms"] <= 0:
        return False

    if not _validate_node_identity(message["source"]):
        return False
    if not _validate_node_identity(message["destination"]):
        return False

    payload = message["payload"]
    if payload is None:
        return False

    if message["type"] == "register_worker":
        ok, _ = validate_register_worker_payload(payload)
        if not ok:
            return False

    if message["type"] in {"heartbeat", "heartbeat_ack"}:
        if not isinstance(payload, dict):
            return False
        sequence = payload.get("sequence")
        if not isinstance(sequence, int) or isinstance(sequence, bool):
            return False
        if message["type"] == "heartbeat_ack" and payload.get("status") not in {"alive", "rejected"}:
            return False

    return True


def build_heartbeat_message(source: dict, destination: dict, sequence: int, request_id: str | None = None) -> dict:
    if not isinstance(sequence, int) or isinstance(sequence, bool):
        raise ValueError("heartbeat sequence must be an integer")

    message_id = f"heartbeat-{uuid.uuid4()}"
    return {
        "version": "sd_2026_2_v1",
        "type": "heartbeat",
        "message_id": message_id,
        "request_id": request_id or f"req-{uuid.uuid4()}",
        "timestamp_ms": int(time.time() * 1000),
        "source": dict(source),
        "destination": dict(destination),
        "payload": {"sequence": int(sequence)},
    }


def build_heartbeat_ack_message(request_message: dict, source: dict, destination: dict) -> dict:
    payload = request_message.get("payload", {}) if isinstance(request_message.get("payload", {}), dict) else {}
    sequence = payload.get("sequence", 0)
    return {
        "version": "sd_2026_2_v1",
        "type": "heartbeat_ack",
        "message_id": f"heartbeat-ack-{uuid.uuid4()}",
        "request_id": request_message["request_id"],
        "timestamp_ms": int(time.time() * 1000),
        "source": dict(source),
        "destination": dict(destination),
        "payload": {"sequence": int(sequence), "status": "alive"},
    }


def build_protocol_error_message(message: dict, master_identity: dict, reason: str) -> dict:
    if not isinstance(message, dict):
        raise ValueError("protocol_error requires a message dict")

    request_id = str(message.get("request_id", "unknown"))
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
        "message_id": f"protocol-error-{uuid.uuid4()}",
        "request_id": request_id,
        "timestamp_ms": int(time.time() * 1000),
        "source": {
            "group_label": str(master_identity.get("group_label", "p2p-cluster")),
            "node_id": str(master_identity.get("node_id", "00000000-0000-4000-8000-000000000000")),
            "node_label": str(master_identity.get("node_label", "master")),
            "node_role": str(master_identity.get("node_role", "master")),
        },
        "destination": unsafe_source or unsafe_destination,
        "payload": {"status": "error", "reason": str(reason)},
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
        if message["type"] not in {"register_worker", "registration_ack", "heartbeat", "heartbeat_ack", "protocol_error"}:
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
    return extract_complete_messages(buffer)


class MasterRegistry:
    def __init__(self):
        self.workers = {}
        self.node_labels = {}
        self._lock = threading.RLock()

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
            return False

    def begin_heartbeat_expectation(self, node_id: str, request_id: str, sequence: int) -> dict:
        with self._lock:
            worker = self.workers.get(node_id)
            if worker is None:
                raise ValueError(f"unknown worker: {node_id}")

            if worker.get("expected_heartbeat_request_id") is not None and worker.get("expected_heartbeat_sequence") is not None:
                worker["heartbeat_misses"] = int(worker.get("heartbeat_misses", 0) or 0) + 1
                if worker["heartbeat_misses"] >= 3:
                    worker["status"] = "unavailable"
                    worker["expected_heartbeat_request_id"] = None
                    worker["expected_heartbeat_sequence"] = None
                    log_event("ASD", worker.get("node_label", "unknown"), "master", "heartbeat", request_id, "unavailable")
                else:
                    worker["expected_heartbeat_request_id"] = None
                    worker["expected_heartbeat_sequence"] = None

            worker["status"] = "available" if worker.get("status") != "unavailable" else "unavailable"
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
                worker["status"] = "available"
                worker["expected_heartbeat_request_id"] = None
                worker["expected_heartbeat_sequence"] = None
                return True

            if expected_id is not None and expected_sequence is not None:
                worker["heartbeat_misses"] = int(worker.get("heartbeat_misses", 0) or 0) + 1
                worker["expected_heartbeat_request_id"] = None
                worker["expected_heartbeat_sequence"] = None
                if worker["heartbeat_misses"] >= 3:
                    worker["status"] = "unavailable"
                    log_event("ASD", worker.get("node_label", "unknown"), "master", "heartbeat_ack", request_id, "unavailable")
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
