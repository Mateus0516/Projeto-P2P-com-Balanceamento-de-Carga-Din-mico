#!/usr/bin/env python3
import argparse
import csv
import json
import socket
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, Tuple

from protocol import (
    build_metrics_request_message,
    recv_msgs,
    send_msg,
    validate_message,
)


def default_collector_identity(label: str = "collector") -> Dict[str, str]:
    return {
        "group_label": "p2p-cluster",
        "node_id": "33333333-3333-4333-8333-333333333333",
        "node_label": str(label),
        "node_role": "collector",
    }


def default_master_identity() -> Dict[str, str]:
    return {
        "group_label": "p2p-cluster",
        "node_id": "22222222-2222-4222-8222-222222222222",
        "node_label": "master",
        "node_role": "master",
    }


def request_metrics(host: str, port: int, *, timeout: float = 5.0, request_id: str | None = None, collector_identity: Dict[str, str] | None = None) -> Dict[str, Any]:
    collector = default_collector_identity() if collector_identity is None else collector_identity
    master = default_master_identity()
    message = build_metrics_request_message(
        source=dict(collector),
        destination=dict(master),
        request_id=request_id,
    )

    with socket.create_connection((host, int(port)), timeout=timeout) as sock:
        sock.settimeout(timeout)
        send_msg(sock, message)
        buffer = bytearray()
        deadline = time.monotonic() + timeout

        while time.monotonic() < deadline:
            try:
                packets = recv_msgs(sock, buffer)
            except socket.timeout:
                continue

            if packets is None:
                break

            for packet in packets:
                if not isinstance(packet, dict) or not validate_message(packet):
                    continue
                if packet.get("type") == "metrics_response" and packet.get("request_id") == message["request_id"]:
                    payload = packet.get("payload", {}) if isinstance(packet.get("payload", {}), dict) else {}
                    return dict(payload)

        raise TimeoutError(f"No metrics_response received for request_id={message['request_id']}")


def export_reports(metrics: Dict[str, Any], output_dir: str | Path, *, timestamp: str | None = None) -> Tuple[Path, Path]:
    if not isinstance(metrics, dict):
        raise ValueError("metrics must be a mapping")

    out_dir = Path(output_dir)
    out_dir.mkdir(parents=True, exist_ok=True)
    stamp = str(timestamp) if timestamp is not None else datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")

    json_path = out_dir / f"metrics_{stamp}.json"
    json_path.write_text(json.dumps(metrics, indent=2, sort_keys=True) + "\n", encoding="utf-8")

    csv_columns = [
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
    ]
    csv_path = out_dir / f"metrics_{stamp}.csv"
    with csv_path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=csv_columns)
        writer.writeheader()
        row = {}
        for field in csv_columns:
            value = metrics.get(field, [])
            if isinstance(value, list):
                row[field] = ";".join(str(item) for item in value)
            elif value is None:
                row[field] = ""
            else:
                row[field] = str(value)
        writer.writerow(row)

    return json_path, csv_path


def run_cli(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Collect master metrics and export JSON/CSV reports.")
    parser.add_argument("--host", default="127.0.0.1", help="Master host to connect to")
    parser.add_argument("--port", type=int, default=9000, help="Master port to connect to")
    parser.add_argument("--output-dir", default="./reports", help="Directory for exported reports")
    parser.add_argument("--request-id", default=None, help="Optional request ID for the metrics request")
    parser.add_argument("--collector-label", default="collector", help="Collector label used in the request identity")
    parser.add_argument("--timeout", type=float, default=5.0, help="Timeout for the metrics request")
    args = parser.parse_args(argv)

    identity = default_collector_identity(args.collector_label)
    metrics = request_metrics(args.host, args.port, timeout=args.timeout, request_id=args.request_id, collector_identity=identity)
    json_path, csv_path = export_reports(metrics, args.output_dir)
    print(json_path)
    print(csv_path)
    return 0


if __name__ == "__main__":
    raise SystemExit(run_cli())
