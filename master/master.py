#!/usr/bin/env python3
import argparse
import json
import socket
import sys
import threading
from pathlib import Path
from typing import Dict

ROOT = Path(__file__).resolve().parent.parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from protocol import MasterRegistry, log_event, recv_msgs, send_msg, validate_message

CONFIG_PATH = Path(__file__).with_name("config.json")


def load_or_create_config(path: Path) -> Dict[str, object]:
    if not path.exists():
        config = {
            "uuid": "master-uuid",
            "label": "master",
            "host": "127.0.0.1",
            "port": 9000,
        }
        path.write_text(json.dumps(config, indent=2), encoding="utf-8")
        return config

    with path.open("r", encoding="utf-8") as handle:
        data = json.load(handle)

    if "uuid" not in data:
        data["uuid"] = "master-uuid"
    if "label" not in data:
        data["label"] = "master"
    if "host" not in data:
        data["host"] = "127.0.0.1"
    if "port" not in data:
        data["port"] = 9000

    path.write_text(json.dumps(data, indent=2), encoding="utf-8")
    return data


def build_ack_message(request_message: dict, status: str) -> dict:
    origin = {
        "uuid": request_message["origin"]["uuid"],
        "label": request_message["origin"]["label"],
    }
    return {
        "type": "registration_ack",
        "msg_id": "ack-" + request_message["msg_id"],
        "request_id": request_message["request_id"],
        "origin": {"uuid": "master-uuid", "label": "master"},
        "timestamp": request_message["timestamp"],
        "payload": {
            "status": status,
            "worker": {
                "uuid": origin["uuid"],
                "label": origin["label"],
            },
        },
    }


def handle_client(conn: socket.socket, addr, registry: MasterRegistry) -> None:
    buffer = bytearray()
    try:
        while True:
            messages = recv_msgs(conn, buffer)
            if not messages:
                continue

            for message in messages:
                if not validate_message(message):
                    log_event("ASD", "unknown", "master", "invalid_message", "unknown", "discarded")
                    continue

                msg_type = message["type"]
                request_id = message["request_id"]
                origin = message["origin"]

                if msg_type == "register_worker":
                    payload = message.get("payload", {})
                    if not isinstance(payload, dict):
                        log_event("ASD", origin["label"], "master", msg_type, request_id, "discarded")
                        continue

                    worker_record = {
                        "uuid": origin["uuid"],
                        "label": origin["label"],
                        "host": payload.get("host", "127.0.0.1"),
                        "port": payload.get("port", 0),
                    }
                    registry.upsert_worker(worker_record)
                    log_event("ASD", origin["label"], "master", msg_type, request_id, "registered")

                    ack = build_ack_message(message, "accepted")
                    send_msg(conn, ack)
                    log_event("ASD", "master", origin["label"], ack["type"], request_id, "sent")

                else:
                    log_event("ASD", origin["label"], "master", msg_type, request_id, "ignored")
    except ConnectionResetError:
        pass
    finally:
        conn.close()


def run_server(host: str, port: int) -> None:
    registry = MasterRegistry()
    server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    server.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    server.bind((host, port))
    server.listen(5)
    log_event("ASD", "master", "network", "server_start", "-", f"listening on {host}:{port}")

    try:
        while True:
            conn, addr = server.accept()
            threading.Thread(target=handle_client, args=(conn, addr, registry), daemon=True).start()
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
    run_server(host, port)


if __name__ == "__main__":
    main()
