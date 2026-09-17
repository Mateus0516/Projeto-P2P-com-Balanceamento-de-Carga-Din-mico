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

from protocol import log_event, recv_msgs, send_msg, validate_message

CONFIG_PATH = Path(__file__).with_name("config.json")


def load_or_create_config(path: Path) -> Dict[str, object]:
    if not path.exists():
        config = {
            "uuid": str(uuid.uuid4()),
            "label": "worker-a",
            "host": "127.0.0.1",
            "port": 9101,
        }
        path.write_text(json.dumps(config, indent=2), encoding="utf-8")
        return config

    with path.open("r", encoding="utf-8") as handle:
        data = json.load(handle)

    if "uuid" not in data:
        data["uuid"] = str(uuid.uuid4())
    if "label" not in data:
        data["label"] = "worker-a"
    if "host" not in data:
        data["host"] = "127.0.0.1"
    if "port" not in data:
        data["port"] = 9101

    path.write_text(json.dumps(data, indent=2), encoding="utf-8")
    return data


def register_worker(master_host: str, master_port: int, config: Dict[str, object]) -> None:
    message = {
        "type": "register_worker",
        "msg_id": f"register-{uuid.uuid4()}",
        "request_id": f"req-{uuid.uuid4()}",
        "origin": {
            "uuid": config["uuid"],
            "label": config["label"],
        },
        "timestamp": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
        "payload": {
            "host": config["host"],
            "port": config["port"],
        },
    }

    with socket.create_connection((master_host, master_port), timeout=5) as sock:
        send_msg(sock, message)
        log_event("ASD", config["label"], "master", message["type"], message["request_id"], "sent")

        buffer = bytearray()
        deadline = time.time() + 5
        while time.time() < deadline:
            messages = recv_msgs(sock, buffer)
            if not messages:
                time.sleep(0.1)
                continue

            for reply in messages:
                if not validate_message(reply):
                    log_event("ASD", config["label"], "master", "invalid_reply", message["request_id"], "discarded")
                    continue

                if reply["type"] == "registration_ack":
                    log_event("ASD", config["label"], "master", reply["type"], reply["request_id"], "accepted")
                    return

        log_event("ASD", config["label"], "master", "registration_ack", message["request_id"], "timeout")


def run_listener(host: str, port: int, worker_label: str) -> None:
    server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    server.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    server.bind((host, port))
    server.listen(5)

    def handle_connection(conn: socket.socket, addr) -> None:
        buffer = bytearray()
        try:
            while True:
                messages = recv_msgs(conn, buffer)
                if not messages:
                    continue
                for msg in messages:
                    if validate_message(msg):
                        log_event("ASD", msg["origin"]["label"], worker_label, msg["type"], msg["request_id"], "received")
        except ConnectionResetError:
            pass
        finally:
            conn.close()

    while True:
        conn, addr = server.accept()
        threading.Thread(target=handle_connection, args=(conn, addr), daemon=True).start()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Worker node for NDJSON registration protocol")
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--port", type=int, default=9101)
    parser.add_argument("--master-host", default="127.0.0.1")
    parser.add_argument("--master-port", type=int, default=9000)
    args = parser.parse_args()

    config = load_or_create_config(CONFIG_PATH)
    config["host"] = args.host
    config["port"] = args.port

    listener_thread = threading.Thread(target=run_listener, args=(config["host"], config["port"], config["label"]), daemon=True)
    listener_thread.start()

    register_worker(args.master_host, args.master_port, config)
    while True:
        time.sleep(1)
