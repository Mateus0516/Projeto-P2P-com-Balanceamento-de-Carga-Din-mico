import json
import socket
from typing import Any, List

REQUIRED_MESSAGE_FIELDS = (
    "type",
    "msg_id",
    "request_id",
    "origin",
    "timestamp",
    "payload",
)


def log_event(group: str, origin: str, destination: str, msg_type: str, request_id: str, result: str) -> None:
    print(f"{group} | {origin} | {destination} | {msg_type} | {request_id} | {result}")


def validate_message(message: Any) -> bool:
    if not isinstance(message, dict):
        return False

    for field in REQUIRED_MESSAGE_FIELDS:
        if field not in message:
            return False

    for field in ("type", "msg_id", "request_id", "timestamp"):
        if not isinstance(message[field], str) or not message[field].strip():
            return False

    origin = message["origin"]
    if not isinstance(origin, dict):
        return False
    if not isinstance(origin.get("uuid"), str) or not origin["uuid"].strip():
        return False
    if not isinstance(origin.get("label"), str) or not origin["label"].strip():
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

        try:
            decoded = raw_line.decode("utf-8")
        except UnicodeDecodeError:
            log_event("ASD", "unknown", "unknown", "invalid_json", "unknown", "discarded")
            continue

        try:
            payload = json.loads(decoded)
        except json.JSONDecodeError:
            log_event("ASD", "unknown", "unknown", "invalid_json", "unknown", "discarded")
            continue

        if not validate_message(payload):
            origin = payload.get("origin", {}) if isinstance(payload, dict) else {}
            origin_label = origin.get("label", "unknown") if isinstance(origin, dict) else "unknown"
            msg_type = payload.get("type", "invalid_message") if isinstance(payload, dict) else "invalid_message"
            request_id = payload.get("request_id", "unknown") if isinstance(payload, dict) else "unknown"
            log_event("ASD", origin_label, "unknown", msg_type, request_id, "discarded")
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
