#!/usr/bin/env python3
import argparse
import socket
import time
from typing import Any

from client.client import TaskClient
from protocol import validate_task_submit_payload


def build_load_plan(*, task_count: int = 200, start: int = 1, end: int = 1000, interval_ms: int = 20, seed: int = 0) -> list[dict[str, Any]]:
    if task_count < 0:
        raise ValueError("task_count must be non-negative")
    if interval_ms < 0:
        raise ValueError("interval_ms must be non-negative")

    plan: list[dict[str, Any]] = []
    for index in range(int(task_count)):
        task = {
            "task_id": f"load-{int(seed):04d}-{index:04d}",
            "task_kind": "sum_range",
            "parameters": {"start": int(start), "end": int(end)},
            "scheduled_after_ms": int(interval_ms * index),
        }
        if not validate_task_submit_payload(task):
            raise ValueError(f"invalid generated task payload: {task!r}")
        plan.append(task)
    return plan


def submit_load_plan(*, host: str, port: int, task_count: int = 200, start: int = 1, end: int = 1000, interval_ms: int = 20, seed: int = 0, node_label: str = "load-generator") -> list[dict[str, Any]]:
    client = TaskClient(host=host, port=port, node_label=node_label, timeout=10.0)
    replies: list[dict[str, Any]] = []
    for index, task in enumerate(build_load_plan(task_count=task_count, start=start, end=end, interval_ms=interval_ms, seed=seed)):
        ack = client.submit_task(task["task_id"], start=int(task["parameters"]["start"]), end=int(task["parameters"]["end"]), timeout=10.0, request_id=f"load-{seed}-{index:04d}")
        replies.append(ack)
        if index + 1 < task_count and interval_ms > 0:
            time.sleep(max(0.0, interval_ms / 1000.0))
    return replies


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Generates and submits a deterministic load of sum_range tasks to the Master.")
    parser.add_argument("--host", default="127.0.0.1", help="Host do Master")
    parser.add_argument("--port", type=int, default=9000, help="Porta do Master")
    parser.add_argument("--task-count", type=int, default=200, help="Quantidade de tarefas a enviar")
    parser.add_argument("--start", type=int, default=1, help="Valor inicial do intervalo")
    parser.add_argument("--end", type=int, default=1000, help="Valor final do intervalo")
    parser.add_argument("--interval-ms", type=int, default=20, help="Intervalo entre as tarefas em milissegundos")
    parser.add_argument("--seed", type=int, default=0, help="Semente do plano de carga para manter determinismo")
    parser.add_argument("--node-label", default="load-generator", help="Label do cliente emissor")
    return parser


def main() -> None:
    args = _build_parser().parse_args()
    replies = submit_load_plan(
        host=args.host,
        port=args.port,
        task_count=args.task_count,
        start=args.start,
        end=args.end,
        interval_ms=args.interval_ms,
        seed=args.seed,
        node_label=args.node_label,
    )
    accepted = sum(1 for ack in replies if ack.get("payload", {}).get("accepted") is True)
    print(f"sended={len(replies)} accepted={accepted} seed={args.seed}")


if __name__ == "__main__":
    main()
