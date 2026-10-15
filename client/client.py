#!/usr/bin/env python3
import argparse
import socket
import sys
import time
import uuid
from pathlib import Path
from typing import Any

ROOT = Path(__file__).resolve().parent.parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from protocol import (
    build_task_submit_message,
    recv_msgs,
    send_msg,
    validate_message,
    validate_task_submit_payload,
)


class TaskClientError(RuntimeError):
    pass


class TaskClient:
    def __init__(self, host: str, port: int, *, group_label: str = "p2p-cluster", node_label: str = "client-a", node_role: str = "worker", timeout: float = 5.0):
        if not host or not host.strip():
            raise ValueError("host is required")
        if not (1 <= int(port) <= 65535):
            raise ValueError("port must be between 1 and 65535")
        self.host = str(host)
        self.port = int(port)
        self.group_label = str(group_label)
        self.node_label = str(node_label)
        self.node_role = str(node_role)
        self.timeout = float(timeout)
        self.node_id = str(uuid.uuid4())

    @property
    def source_identity(self) -> dict[str, str]:
        return {
            "group_label": self.group_label,
            "node_id": self.node_id,
            "node_label": self.node_label,
            "node_role": self.node_role,
        }

    def build_task_message(self, task_id: str, *, start: int, end: int, request_id: str | None = None) -> dict[str, Any]:
        task = {
            "task_id": str(task_id),
            "task_kind": "sum_range",
            "parameters": {"start": int(start), "end": int(end)},
        }
        if not validate_task_submit_payload(task):
            raise ValueError(f"invalid sum_range task for task_id={task_id!r}")

        destination = {
            "group_label": self.group_label,
            "node_id": "22222222-2222-4222-8222-222222222222",
            "node_label": "master",
            "node_role": "master",
        }
        return build_task_submit_message(
            source=self.source_identity,
            destination=destination,
            task=task,
            request_id=request_id or f"req-{uuid.uuid4()}",
        )

    def submit_task(self, task_id: str, *, start: int, end: int, timeout: float | None = None, request_id: str | None = None) -> dict[str, Any]:
        message = self.build_task_message(task_id, start=start, end=end, request_id=request_id)
        deadline = time.time() + float(timeout if timeout is not None else self.timeout)
        buffer = bytearray()

        try:
            with socket.create_connection((self.host, self.port), timeout=float(timeout if timeout is not None else self.timeout)) as sock:
                sock.settimeout(float(timeout if timeout is not None else self.timeout))
                send_msg(sock, message)

                while time.time() <= deadline:
                    try:
                        packets = recv_msgs(sock, buffer)
                    except socket.timeout:
                        packets = []

                    if packets is None:
                        break
                    if packets:
                        for packet in packets:
                            if packet.get("type") == "task_submit_ack":
                                if not validate_message(packet):
                                    raise TaskClientError(f"invalid task_submit_ack from master: {packet!r}")
                                return packet
                            if packet.get("type") == "protocol_error":
                                reason = packet.get("payload", {}).get("reason", "unknown protocol error")
                                raise TaskClientError(f"master rejected the task: {reason}")
                        continue

                    time.sleep(0.05)

                raise TimeoutError(f"timeout waiting for task_submit_ack for task_id={task_id!r}")
        except OSError as exc:
            raise TaskClientError(f"connection failed for {self.host}:{self.port}: {exc}") from exc


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Enviar uma tarefa sum_range ao master via TCP NDJSON")
    parser.add_argument("--host", default="127.0.0.1", help="Host do Master")
    parser.add_argument("--port", type=int, default=9000, help="Porta do Master")
    parser.add_argument("--task-id", required=True, help="Identificador da tarefa")
    parser.add_argument("--start", type=int, required=True, help="Valor inicial do intervalo, inclusivo")
    parser.add_argument("--end", type=int, required=True, help="Valor final do intervalo, inclusivo")
    parser.add_argument("--node-label", default="client-a", help="Label do cliente para a identidade do remetente")
    parser.add_argument("--timeout", type=float, default=5.0, help="Tempo limite em segundos")
    return parser


def main() -> None:
    parser = _build_parser()
    args = parser.parse_args()

    client = TaskClient(host=args.host, port=args.port, node_label=args.node_label, timeout=args.timeout)
    try:
        ack = client.submit_task(args.task_id, start=args.start, end=args.end, timeout=args.timeout)
    except (TaskClientError, TimeoutError, ValueError) as exc:
        print(f"ERROR: {exc}", file=sys.stderr)
        raise SystemExit(1) from exc

    payload = ack.get("payload", {})
    print(f"task_id={payload.get('task_id')}")
    print(f"task_label={payload.get('task_label')}")
    print(f"accepted={payload.get('accepted')}")
    print(f"reason={payload.get('reason')}")


if __name__ == "__main__":
    main()
