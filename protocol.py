import json
import socket
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


def validate_message(message: Any) -> bool:
    if not isinstance(message, dict):
        return False

    for field in REQUIRED_MESSAGE_FIELDS:
        if field not in message:
            return False

    if message["version"] != "sd_2026_2_v1":
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

        if not isinstance(payload, dict) or not validate_message(payload):
            source = payload.get("source", {}) if isinstance(payload, dict) else {}
            source_label = source.get("node_label", "unknown") if isinstance(source, dict) else "unknown"
            msg_type = payload.get("type", "invalid_message") if isinstance(payload, dict) else "invalid_message"
            request_id = payload.get("request_id", "unknown") if isinstance(payload, dict) else "unknown"
            log_event("ASD", source_label, "unknown", msg_type, request_id, "discarded")
            continue

        messages.append(payload)

    return messages


def send_msg(sock: socket.socket, message: dict) -> None:
    encoded = (json.dumps(message, separators=(",", ":"), ensure_ascii=False) + "\n").encode("utf-8")
    sock.sendall(encoded)


def recv_msgs(sock: socket.socket, buffer: bytearray) -> List[dict]:
    try:
        chunk = sock.recv(4096)
    except socket.timeout:
        return []

    if not chunk:
        return []

    buffer.extend(chunk)
    return extract_complete_messages(buffer)


class MasterRegistry:
    def __init__(self):
        self.workers = {}
        self.node_labels = {}

    def upsert_worker(self, worker: dict) -> dict:
        uuid_value = worker.get("node_id") or worker.get("uuid")
        if not uuid_value:
            raise ValueError("worker UUID is required")

        candidate = dict(worker)
        candidate["node_id"] = str(uuid_value)
        candidate["node_label"] = candidate.get("node_label") or candidate.get("label")
        candidate["group_label"] = candidate.get("group_label")
        candidate["node_role"] = candidate.get("node_role")

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

    def get_worker(self, uuid_value: str):
        return self.workers.get(uuid_value)

    def remove_worker(self, uuid_value: str) -> None:
        worker = self.workers.pop(uuid_value, None)
        if worker and worker.get("node_label") in self.node_labels:
            if self.node_labels.get(worker["node_label"]) == uuid_value:
                del self.node_labels[worker["node_label"]]
