#!/usr/bin/env python3
import argparse
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
    build_protocol_error_message,
    classify_protocol_error,
    log_event,
    recv_msgs,
    send_msg,
    validate_message,
    validate_register_worker_payload,
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
    )
    with write_lock:
        send_msg(conn, message)
    return message


def start_master_heartbeat_loop(conn: socket.socket, registry: MasterRegistry, master_identity: Dict[str, object], worker_identity: Dict[str, object], write_lock: threading.Lock, stop_event: threading.Event | None = None, interval_seconds: float = 10.0):
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


def handle_client(conn: socket.socket, addr, registry: MasterRegistry, master_identity: Dict[str, object]) -> None:
    buffer = bytearray()
    write_lock = threading.Lock()
    session_stop = threading.Event()
    current_worker_identity = None
    current_worker_thread = None

    try:
        conn.settimeout(1.0)
        idle_reads = 0
        while not session_stop.is_set():
            try:
                messages = recv_msgs(conn, buffer)
            except OSError:
                if current_worker_identity is not None:
                    registry.mark_worker_unavailable(current_worker_identity["node_id"], reason="socket error")
                break

            if messages is None:
                if current_worker_identity is not None:
                    registry.mark_worker_unavailable(current_worker_identity["node_id"], reason="socket closed")
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
                    with write_lock:
                        send_msg(conn, ack)
                    log_event("ASD", "master", source["node_label"], ack["type"], request_id, "sent")

                    if current_worker_thread is None:
                        current_worker_thread, session_stop = start_master_heartbeat_loop(conn, registry, master_identity, current_worker_identity, write_lock)

                elif msg_type == "heartbeat":
                    ack = build_heartbeat_ack_message(message, source=master_identity, destination=source)
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

                else:
                    log_event("ASD", source["node_label"], "master", msg_type, request_id, "ignored")
    except ConnectionResetError:
        if current_worker_identity is not None:
            registry.mark_worker_unavailable(current_worker_identity["node_id"], reason="connection reset")
    except BrokenPipeError:
        if current_worker_identity is not None:
            registry.mark_worker_unavailable(current_worker_identity["node_id"], reason="broken pipe")
    except OSError:
        if current_worker_identity is not None:
            registry.mark_worker_unavailable(current_worker_identity["node_id"], reason="socket error")
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


def run_server(host: str, port: int, master_identity: Dict[str, object]) -> None:
    registry = MasterRegistry()
    server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    server.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    server.bind((host, port))
    server.listen(5)
    log_event("ASD", "master", "network", "server_start", "-", f"listening on {host}:{port}")

    try:
        while True:
            conn, addr = server.accept()
            threading.Thread(target=handle_client, args=(conn, addr, registry, master_identity), daemon=True).start()
    except KeyboardInterrupt:
        pass
    finally:
        server.close()


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
