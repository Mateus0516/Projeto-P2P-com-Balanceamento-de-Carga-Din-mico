import json
import socket
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

    if message["payload"] is None:
        return False

    return True


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

    def upsert_worker(self, worker: dict) -> dict:
        uuid_value = worker.get("uuid")
        if not uuid_value:
            raise ValueError("worker UUID is required")
        self.workers[uuid_value] = worker
        return self.workers[uuid_value]

    def get_worker(self, uuid_value: str):
        return self.workers.get(uuid_value)

    def remove_worker(self, uuid_value: str) -> None:
        self.workers.pop(uuid_value, None)
