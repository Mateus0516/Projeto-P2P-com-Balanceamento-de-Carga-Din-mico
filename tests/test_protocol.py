import json
import socket
import sys
import threading
import time
import uuid
from pathlib import Path

import pytest

from master.master import (
    MasterTaskQueue,
    build_ack_message,
    handle_client,
    load_or_create_config as load_master_config,
    send_heartbeat_to_worker,
    start_master_heartbeat_loop,
)
from protocol import (
    MasterRegistry,
    build_heartbeat_ack_message,
    build_heartbeat_message,
    extract_complete_messages,
    send_msg,
    validate_message,
)
import worker.worker as worker_module
from worker.worker import (
    WorkerControlSession,
    WorkerSessionManager,
    compute_retry_delay,
    load_or_create_config as load_worker_config,
    open_control_session,
    register_worker,
    run_worker,
    send_heartbeat,
    send_with_retry,
    start_heartbeat_loop,
)


def build_register_payload(master_identity: dict, host: str, port: int, assignment_epoch: int = 0, loan_id: str | None = None) -> dict:
    return {
        "home_master": {
            "group_label": str(master_identity.get("group_label", "p2p-cluster")),
            "node_id": str(master_identity.get("node_id", "00000000-0000-4000-8000-000000000000")),
            "node_label": str(master_identity.get("node_label", "master")),
            "node_role": "master",
            "host": str(master_identity.get("host", "127.0.0.1")),
            "port": int(master_identity.get("port", 9000)),
        },
        "assignment_epoch": int(assignment_epoch),
        "loan_id": None if loan_id is None else str(loan_id),
        "host": str(host),
        "port": int(port),
    }


def test_worker_load_or_create_config_keeps_distinct_persistent_identities(tmp_path):
    a_path = tmp_path / "worker-a.json"
    b_path = tmp_path / "worker-b.json"

    worker_a = load_worker_config(a_path)
    worker_b = load_worker_config(b_path)

    assert worker_a["node_label"] == "worker-a"
    assert worker_b["node_label"] == "worker-b"
    assert worker_a["node_id"] != worker_b["node_id"]
    assert uuid.UUID(worker_a["node_id"]).version == 4
    assert uuid.UUID(worker_b["node_id"]).version == 4


def test_master_registry_tracks_two_workers_separately_and_keeps_same_identity_after_reconnect():
    registry = MasterRegistry()
    worker_a = {
        "node_id": "11111111-1111-4111-8111-111111111111",
        "node_label": "worker-a",
        "group_label": "p2p-cluster",
        "node_role": "worker",
        "host": "127.0.0.1",
        "port": 9101,
    }
    worker_b = {
        "node_id": "22222222-2222-4222-8222-222222222222",
        "node_label": "worker-b",
        "group_label": "p2p-cluster",
        "node_role": "worker",
        "host": "127.0.0.1",
        "port": 9102,
    }

    registry.upsert_worker(worker_a)
    registry.upsert_worker(worker_b)

    assert len(registry.workers) == 2
    assert set(registry.workers) == {worker_a["node_id"], worker_b["node_id"]}
    assert set(registry.node_labels) == {"worker-a", "worker-b"}

    registry.mark_worker_unavailable(worker_a["node_id"], reason="socket closed")
    assert registry.get_worker(worker_a["node_id"])["status"] == "unavailable"
    assert registry.get_worker(worker_b["node_id"])["status"] == "available"

    registry.upsert_worker(worker_a)
    assert len(registry.workers) == 2
    assert set(registry.workers) == {worker_a["node_id"], worker_b["node_id"]}


def test_valid_task_submit_message_is_accepted_by_protocol():
    message = {
        "version": "sd_2026_2_v1",
        "type": "task_submit",
        "message_id": "msg-task-submit-1",
        "request_id": "req-task-submit-1",
        "timestamp_ms": 1727170000123,
        "source": {
            "group_label": "p2p-cluster",
            "node_id": "11111111-1111-4111-8111-111111111111",
            "node_label": "worker-a",
            "node_role": "worker",
        },
        "destination": {
            "group_label": "p2p-cluster",
            "node_id": "22222222-2222-4222-8222-222222222222",
            "node_label": "master",
            "node_role": "master",
        },
        "payload": {
            "task_id": "task-001",
            "task_kind": "sum_range",
            "parameters": {"start": 0, "end": 10},
        },
    }
    assert validate_message(message) is True


def test_invalid_task_submit_message_is_rejected_by_protocol():
    message = {
        "version": "sd_2026_2_v1",
        "type": "task_submit",
        "message_id": "msg-task-submit-2",
        "request_id": "req-task-submit-2",
        "timestamp_ms": 1727170000124,
        "source": {
            "group_label": "p2p-cluster",
            "node_id": "11111111-1111-4111-8111-111111111111",
            "node_label": "worker-a",
            "node_role": "worker",
        },
        "destination": {
            "group_label": "p2p-cluster",
            "node_id": "22222222-2222-4222-8222-222222222222",
            "node_label": "master",
            "node_role": "master",
        },
        "payload": {
            "task_id": "task-002",
            "task_kind": "sum_range",
            "parameters": {"start": 3, "end": 1},
        },
    }
    assert validate_message(message) is False


def test_task_submit_ack_message_matches_protocol_contract():
    message = {
        "version": "sd_2026_2_v1",
        "type": "task_submit_ack",
        "message_id": "msg-task-submit-ack-1",
        "request_id": "req-task-submit-ack-1",
        "timestamp_ms": 1727170000125,
        "source": {
            "group_label": "p2p-cluster",
            "node_id": "22222222-2222-4222-8222-222222222222",
            "node_label": "master",
            "node_role": "master",
        },
        "destination": {
            "group_label": "p2p-cluster",
            "node_id": "11111111-1111-4111-8111-111111111111",
            "node_label": "worker-a",
            "node_role": "worker",
        },
        "payload": {
            "task_id": "task-001",
            "task_label": "sum_range-task-001",
            "accepted": True,
            "reason": "queued",
        },
    }
    assert validate_message(message) is True


def test_task_request_assignment_and_no_task_messages_match_protocol_contract():
    task_request = {
        "version": "sd_2026_2_v1",
        "type": "task_request",
        "message_id": "msg-task-request-1",
        "request_id": "req-task-request-1",
        "timestamp_ms": 1727170000127,
        "source": {
            "group_label": "p2p-cluster",
            "node_id": "11111111-1111-4111-8111-111111111111",
            "node_label": "worker-a",
            "node_role": "worker",
        },
        "destination": {
            "group_label": "p2p-cluster",
            "node_id": "22222222-2222-4222-8222-222222222222",
            "node_label": "master",
            "node_role": "master",
        },
        "payload": {"assignment_epoch": 7, "loan_id": None},
    }
    task_assignment = {
        "version": "sd_2026_2_v1",
        "type": "task_assignment",
        "message_id": "msg-task-assignment-1",
        "request_id": "req-task-assignment-1",
        "timestamp_ms": 1727170000128,
        "source": {
            "group_label": "p2p-cluster",
            "node_id": "22222222-2222-4222-8222-222222222222",
            "node_label": "master",
            "node_role": "master",
        },
        "destination": {
            "group_label": "p2p-cluster",
            "node_id": "11111111-1111-4111-8111-111111111111",
            "node_label": "worker-a",
            "node_role": "worker",
        },
        "payload": {
            "task_id": "task-021",
            "task_label": "task-0001",
            "attempt_id": "attempt-abc",
            "task_kind": "sum_range",
            "parameters": {"start": 0, "end": 10},
            "assignment_epoch": 7,
            "loan_id": None,
        },
    }
    no_task = {
        "version": "sd_2026_2_v1",
        "type": "no_task",
        "message_id": "msg-no-task-1",
        "request_id": "req-no-task-1",
        "timestamp_ms": 1727170000129,
        "source": {
            "group_label": "p2p-cluster",
            "node_id": "22222222-2222-4222-8222-222222222222",
            "node_label": "master",
            "node_role": "master",
        },
        "destination": {
            "group_label": "p2p-cluster",
            "node_id": "11111111-1111-4111-8111-111111111111",
            "node_label": "worker-a",
            "node_role": "worker",
        },
        "payload": {"retry_after_ms": 1000},
    }

    assert validate_message(task_request) is True
    assert validate_message(task_assignment) is True
    assert validate_message(no_task) is True


def test_task_submit_range_limits_and_sprint1_compatibility():
    valid = {
        "version": "sd_2026_2_v1",
        "type": "task_submit",
        "message_id": "msg-task-submit-3",
        "request_id": "req-task-submit-3",
        "timestamp_ms": 1727170000126,
        "source": {
            "group_label": "p2p-cluster",
            "node_id": "11111111-1111-4111-8111-111111111111",
            "node_label": "worker-a",
            "node_role": "worker",
        },
        "destination": {
            "group_label": "p2p-cluster",
            "node_id": "22222222-2222-4222-8222-222222222222",
            "node_label": "master",
            "node_role": "master",
        },
        "payload": {
            "task_id": "task-003",
            "task_kind": "sum_range",
            "parameters": {"start": 0, "end": 10000000},
        },
    }
    invalid = {
        **valid,
        "payload": {
            "task_id": "task-004",
            "task_kind": "sum_range",
            "parameters": {"start": -1, "end": 10},
        },
    }
    heartbeat = build_heartbeat_message(
        source={
            "group_label": "p2p-cluster",
            "node_id": "11111111-1111-4111-8111-111111111111",
            "node_label": "worker-a",
            "node_role": "worker",
        },
        destination={
            "group_label": "p2p-cluster",
            "node_id": "22222222-2222-4222-8222-222222222222",
            "node_label": "master",
            "node_role": "master",
        },
        sequence=5,
        request_id="req-heartbeat-compat",
    )

    assert validate_message(valid) is True
    assert validate_message(invalid) is False
    assert validate_message(heartbeat) is True


def test_master_task_queue_fifo_and_stable_labels():
    from master.master import MasterTaskQueue

    queue = MasterTaskQueue()
    task_a = {"task_id": "task-a", "task_kind": "sum_range", "parameters": {"start": 0, "end": 10}}
    task_b = {"task_id": "task-b", "task_kind": "sum_range", "parameters": {"start": 11, "end": 20}}

    decision_a = queue.submit(task_a)
    decision_b = queue.submit(task_b)

    assert decision_a["accepted"] is True
    assert decision_b["accepted"] is True
    assert decision_a["task_label"] == "task-0001"
    assert decision_b["task_label"] == "task-0002"
    assert [record["task_id"] for record in queue.pending()] == ["task-a", "task-b"]


def test_master_task_queue_rejects_duplicate_content_and_conflicting_same_id():
    from master.master import MasterTaskQueue

    queue = MasterTaskQueue()
    task_a = {"task_id": "task-repeat", "task_kind": "sum_range", "parameters": {"start": 1, "end": 2}}
    task_a_same = {"task_id": "task-repeat", "task_kind": "sum_range", "parameters": {"start": 1, "end": 2}}
    task_a_conflict = {"task_id": "task-repeat", "task_kind": "sum_range", "parameters": {"start": 1, "end": 3}}

    first = queue.submit(task_a)
    second = queue.submit(task_a_same)
    conflict = queue.submit(task_a_conflict)

    assert first["accepted"] is True
    assert second["accepted"] is True
    assert second["task_label"] == first["task_label"]
    assert conflict["accepted"] is False
    assert conflict["reason"] == "task_id conflict"
    assert queue.get_task("task-repeat")["status"] == "pending"


def test_master_task_queue_returns_no_task_when_empty_and_allows_assignment_for_registered_worker():
    from master.master import MasterTaskQueue

    queue = MasterTaskQueue()
    first = queue.claim_next_task("worker-a", assignment_epoch=1, loan_id=None)
    assert first is None

    queue.submit({"task_id": "task-assign-1", "task_kind": "sum_range", "parameters": {"start": 1, "end": 2}})
    assigned = queue.claim_next_task("worker-a", assignment_epoch=1, loan_id=None)

    assert assigned is not None
    assert assigned["task_id"] == "task-assign-1"
    assert assigned["task_label"] == "task-0001"
    assert assigned["assignment_epoch"] == 1
    assert assigned["loan_id"] is None
    assert "attempt_id" in assigned
    assert queue.get_task("task-assign-1")["status"] == "running"

    second = queue.claim_next_task("worker-a", assignment_epoch=1, loan_id=None)
    assert second is None


def test_master_task_queue_prevents_duplicate_assignments_under_concurrency():
    from master.master import MasterTaskQueue

    queue = MasterTaskQueue()
    for index in range(5):
        queue.submit({"task_id": f"task-concurrent-{index}", "task_kind": "sum_range", "parameters": {"start": index, "end": index + 1}})

    results = []
    barrier = threading.Barrier(5)

    def worker(worker_id):
        barrier.wait()
        assignment = queue.claim_next_task(worker_id, assignment_epoch=1, loan_id=None)
        results.append(assignment)

    threads = [threading.Thread(target=worker, args=(f"worker-{idx}",)) for idx in range(5)]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join()

    assigned_tasks = [item["task_id"] for item in results if item is not None]
    assert len(assigned_tasks) == 5
    assert len(set(assigned_tasks)) == 5
    assert len({item["attempt_id"] for item in results if item is not None}) == 5


def test_master_task_request_returns_no_task_for_unregistered_worker_and_task_assignment_for_registered_worker():
    registry = MasterRegistry()
    master_identity = {
        "group_label": "p2p-cluster",
        "node_id": "22222222-2222-4222-8222-222222222222",
        "node_label": "master",
        "node_role": "master",
    }
    task_queue = MasterTaskQueue()
    task_queue.submit({"task_id": "task-cycle-1", "task_kind": "sum_range", "parameters": {"start": 2, "end": 4}})

    def exchange(message: dict):
        server_sock, client_sock = socket.socketpair()
        thread = threading.Thread(target=handle_client, args=(server_sock, None, registry, master_identity, task_queue), daemon=True)
        thread.start()
        client_sock.settimeout(2)
        try:
            send_msg(client_sock, message)
            raw = client_sock.recv(4096)
        finally:
            client_sock.close()
        if not raw:
            return None
        return json.loads(raw.decode("utf-8").strip())

    unregistered = {
        "version": "sd_2026_2_v1",
        "type": "task_request",
        "message_id": "msg-task-request-unknown",
        "request_id": "req-task-request-unknown",
        "timestamp_ms": 1727170000130,
        "source": {
            "group_label": "p2p-cluster",
            "node_id": "99999999-9999-4999-8999-999999999999",
            "node_label": "worker-z",
            "node_role": "worker",
        },
        "destination": {
            "group_label": "p2p-cluster",
            "node_id": "22222222-2222-4222-8222-222222222222",
            "node_label": "master",
            "node_role": "master",
        },
        "payload": {"assignment_epoch": 2, "loan_id": None},
    }
    unregistered_reply = exchange(unregistered)
    assert unregistered_reply is not None
    assert unregistered_reply["type"] == "no_task"
    assert unregistered_reply["payload"]["retry_after_ms"] == 1000

    registry.upsert_worker({
        "node_id": "11111111-1111-4111-8111-111111111111",
        "node_label": "worker-a",
        "group_label": "p2p-cluster",
        "node_role": "worker",
        "host": "127.0.0.1",
        "port": 9101,
        "status": "available",
    })

    registered = {
        **unregistered,
        "source": {
            "group_label": "p2p-cluster",
            "node_id": "11111111-1111-4111-8111-111111111111",
            "node_label": "worker-a",
            "node_role": "worker",
        },
        "message_id": "msg-task-request-known",
        "request_id": "req-task-request-known",
    }
    registered_reply = exchange(registered)
    assert registered_reply is not None
    assert registered_reply["type"] == "task_assignment"
    assert registered_reply["payload"]["task_id"] == "task-cycle-1"
    assert registered_reply["payload"]["task_label"] == "task-0001"
    assert registered_reply["payload"]["assignment_epoch"] == 2
    assert registered_reply["payload"]["loan_id"] is None
    assert task_queue.get_task("task-cycle-1")["status"] == "running"


def test_master_task_queue_is_thread_safe_for_parallel_submissions():
    from master.master import MasterTaskQueue

    queue = MasterTaskQueue()
    submitted = []
    barrier = threading.Barrier(8)

    def worker(index):
        barrier.wait()
        payload = {"task_id": f"parallel-{index}", "task_kind": "sum_range", "parameters": {"start": index, "end": index + 1}}
        decision = queue.submit(payload)
        submitted.append(decision)

    threads = [threading.Thread(target=worker, args=(i,)) for i in range(8)]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join()

    assert len(queue.pending()) == 8
    assert len({item["task_label"] for item in submitted}) == 8
    assert len({item["task_id"] for item in submitted}) == 8


def test_end_to_end_real_tcp_flow_submit_assign_result_and_ack():
    from client.client import TaskClient
    from master.master import MasterRegistry, MasterTaskQueue, handle_client

    def reserve_port() -> int:
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
            sock.bind(("127.0.0.1", 0))
            return int(sock.getsockname()[1])

    master_port = reserve_port()
    worker_a_port = reserve_port()
    worker_b_port = reserve_port()
    master_identity = {
        "group_label": "p2p-cluster",
        "node_id": "22222222-2222-4222-8222-222222222222",
        "node_label": "master",
        "node_role": "master",
        "host": "127.0.0.1",
        "port": master_port,
    }
    registry = MasterRegistry()
    task_queue = MasterTaskQueue()
    server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    server.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    server.bind(("127.0.0.1", master_port))
    server.listen(16)
    server_stop = threading.Event()

    def accept_loop() -> None:
        server.settimeout(0.25)
        try:
            while not server_stop.is_set():
                try:
                    conn, addr = server.accept()
                except socket.timeout:
                    continue
                threading.Thread(target=handle_client, args=(conn, addr, registry, master_identity, task_queue), daemon=True).start()
        finally:
            try:
                server.close()
            except OSError:
                pass

    accept_thread = threading.Thread(target=accept_loop, daemon=True)
    accept_thread.start()

    worker_a_stop = threading.Event()
    worker_b_stop = threading.Event()
    worker_a_cfg = {
        "group_label": "p2p-cluster",
        "node_id": "11111111-1111-4111-8111-111111111111",
        "node_label": "worker-a",
        "node_role": "worker",
        "host": "127.0.0.1",
        "port": worker_a_port,
        "assignment_epoch": 0,
        "loan_id": None,
        "master": {
            "group_label": "p2p-cluster",
            "node_id": "22222222-2222-4222-8222-222222222222",
            "node_label": "master",
            "node_role": "master",
            "host": "127.0.0.1",
            "port": master_port,
        },
    }
    worker_b_cfg = {
        "group_label": "p2p-cluster",
        "node_id": "33333333-3333-4333-8333-333333333333",
        "node_label": "worker-b",
        "node_role": "worker",
        "host": "127.0.0.1",
        "port": worker_b_port,
        "assignment_epoch": 0,
        "loan_id": None,
        "master": {
            "group_label": "p2p-cluster",
            "node_id": "22222222-2222-4222-8222-222222222222",
            "node_label": "master",
            "node_role": "master",
            "host": "127.0.0.1",
            "port": master_port,
        },
    }

    worker_a_thread = threading.Thread(target=run_worker, args=("127.0.0.1", master_port, worker_a_cfg), kwargs={"stop_event": worker_a_stop}, daemon=True)
    worker_b_thread = threading.Thread(target=run_worker, args=("127.0.0.1", master_port, worker_b_cfg), kwargs={"stop_event": worker_b_stop}, daemon=True)
    worker_a_thread.start()
    worker_b_thread.start()

    try:
        deadline = time.time() + 10.0
        while time.time() < deadline and len(registry.workers) < 2:
            time.sleep(0.1)
        assert len(registry.workers) >= 2

        client = TaskClient(host="127.0.0.1", port=master_port, node_label="client-e2e", timeout=5.0)
        ack = client.submit_task(task_id="e2e-task-001", start=1, end=100, timeout=3.0)
        assert ack["payload"]["task_id"] == "e2e-task-001"
        assert ack["payload"]["accepted"] is True
        assert ack["payload"]["reason"] == "queued"

        deadline = time.time() + 15.0
        task_record = None
        while time.time() < deadline:
            task_record = task_queue.get_task("e2e-task-001")
            if task_record is not None and task_record["status"] == "completed":
                break
            time.sleep(0.05)

        assert task_record is not None
        assert task_record["task_id"] == "e2e-task-001"
        assert task_record["status"] == "completed"
        assert task_record["result_status"] == "ok"
        assert task_record["attempt_count"] == 1
        assert task_record["last_attempt_id"] is not None
        assert task_record["result_payload"]["status"] == "ok"
        assert task_record["result_payload"]["result"] == {"sum": 5050}

        assignment_records = [
            item for item in task_queue.pending() if item["task_id"] == "e2e-task-001"
        ]
        assert len(assignment_records) == 1
        assert assignment_records[0]["task_id"] == "e2e-task-001"
    finally:
        worker_a_stop.set()
        worker_b_stop.set()
        server_stop.set()
        try:
            worker_a_thread.join(timeout=3)
        except RuntimeError:
            pass
        try:
            worker_b_thread.join(timeout=3)
        except RuntimeError:
            pass
        try:
            accept_thread.join(timeout=3)
        except RuntimeError:
            pass
        try:
            server.shutdown(socket.SHUT_RDWR)
        except OSError:
            pass
        try:
            server.close()
        except OSError:
            pass


def test_master_task_queue_keeps_task_attempt_structure_ready_for_retries():
    from master.master import MasterTaskQueue

    queue = MasterTaskQueue()
    task = {"task_id": "task-retry", "task_kind": "sum_range", "parameters": {"start": 5, "end": 8}}
    decision = queue.submit(task)

    record = queue.get_task("task-retry")
    assert record["task_id"] == "task-retry"
    assert record["status"] == "pending"
    assert isinstance(record["attempts"], dict)
    assert record["attempts"] == {}
    assert decision["task_label"] == "task-0001"


def test_client_builds_valid_task_submit_message_using_protocol_contract():
    from protocol import build_task_submit_message, validate_message

    message = build_task_submit_message(
        source={
            "group_label": "p2p-cluster",
            "node_id": "11111111-1111-4111-8111-111111111111",
            "node_label": "client-a",
            "node_role": "worker",
        },
        destination={
            "group_label": "p2p-cluster",
            "node_id": "22222222-2222-4222-8222-222222222222",
            "node_label": "master",
            "node_role": "master",
        },
        task={
            "task_id": "client-task-001",
            "task_kind": "sum_range",
            "parameters": {"start": 3, "end": 9},
        },
        request_id="req-client-task-001",
    )

    assert validate_message(message) is True
    assert message["payload"]["task_id"] == "client-task-001"
    assert message["type"] == "task_submit"


def test_client_submit_task_round_trip_and_ack_is_printable():
    from client.client import TaskClient

    server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    server.bind(("127.0.0.1", 0))
    server.listen(1)
    port = server.getsockname()[1]

    def handler():
        conn, _ = server.accept()
        try:
            buffer = bytearray()
            while True:
                chunk = conn.recv(4096)
                if not chunk:
                    break
                buffer.extend(chunk)
                if b"\n" in buffer:
                    break
            payload = json.loads(buffer.decode("utf-8").strip())
            assert payload["type"] == "task_submit"
            ack = {
                "version": "sd_2026_2_v1",
                "type": "task_submit_ack",
                "message_id": "msg-client-ack-1",
                "request_id": payload["request_id"],
                "timestamp_ms": 1727170000123,
                "source": {
                    "group_label": "p2p-cluster",
                    "node_id": "22222222-2222-4222-8222-222222222222",
                    "node_label": "master",
                    "node_role": "master",
                },
                "destination": {
                    "group_label": "p2p-cluster",
                    "node_id": "11111111-1111-4111-8111-111111111111",
                    "node_label": "client-a",
                    "node_role": "worker",
                },
                "payload": {
                    "task_id": payload["payload"]["task_id"],
                    "task_label": "task-0001",
                    "accepted": True,
                    "reason": "queued",
                },
            }
            conn.sendall((json.dumps(ack, separators=(",", ":"), ensure_ascii=False) + "\n").encode("utf-8"))
        finally:
            conn.close()
            server.close()

    thread = threading.Thread(target=handler, daemon=True)
    thread.start()

    client = TaskClient(host="127.0.0.1", port=port, node_label="client-a")
    ack = client.submit_task(task_id="client-task-002", start=1, end=7, timeout=3.0)

    assert ack["payload"]["task_id"] == "client-task-002"
    assert ack["payload"]["accepted"] is True
    assert ack["payload"]["task_label"] == "task-0001"
    assert ack["payload"]["reason"] == "queued"


def test_worker_session_manager_requests_task_after_registration():
    config = {
        "group_label": "p2p-cluster",
        "node_id": "11111111-1111-4111-8111-111111111111",
        "node_label": "worker-a",
        "node_role": "worker",
        "host": "127.0.0.1",
        "port": 9101,
        "assignment_epoch": 0,
        "loan_id": None,
    }
    manager = WorkerSessionManager("127.0.0.1", 9000, config)
    server_sock, client_sock = socket.socketpair()
    try:
        session = WorkerControlSession(client_sock)
        received = {}

        def read_request():
            chunk = server_sock.recv(4096)
            received["message"] = json.loads(chunk.decode("utf-8").strip())

        thread = threading.Thread(target=read_request, daemon=True)
        thread.start()

        assert manager.request_task(session) is True
        thread.join(timeout=2)

        message = received["message"]
        assert message["type"] == "task_request"
        assert message["destination"]["node_label"] == "master"
        assert message["payload"]["assignment_epoch"] == 0
        assert message["payload"]["loan_id"] is None
    finally:
        server_sock.close()
        client_sock.close()


def test_worker_session_manager_respects_retry_after_no_task():
    config = {
        "group_label": "p2p-cluster",
        "node_id": "11111111-1111-4111-8111-111111111111",
        "node_label": "worker-a",
        "node_role": "worker",
        "host": "127.0.0.1",
        "port": 9101,
        "assignment_epoch": 0,
        "loan_id": None,
    }
    manager = WorkerSessionManager("127.0.0.1", 9000, config)
    no_task = {
        "version": "sd_2026_2_v1",
        "type": "no_task",
        "message_id": "msg-no-task-1",
        "request_id": "req-no-task-1",
        "timestamp_ms": 1727170000129,
        "source": {
            "group_label": "p2p-cluster",
            "node_id": "22222222-2222-4222-8222-222222222222",
            "node_label": "master",
            "node_role": "master",
        },
        "destination": {
            "group_label": "p2p-cluster",
            "node_id": "11111111-1111-4111-8111-111111111111",
            "node_label": "worker-a",
            "node_role": "worker",
        },
        "payload": {"retry_after_ms": 1000},
    }

    manager.process_inbound_message(no_task)
    assert manager.busy is False
    assert manager.next_task_ready_at > time.monotonic()
    assert manager.should_poll_for_task() is False


def test_worker_session_manager_marks_busy_on_assignment_and_blocks_new_requests():
    config = {
        "group_label": "p2p-cluster",
        "node_id": "11111111-1111-4111-8111-111111111111",
        "node_label": "worker-a",
        "node_role": "worker",
        "host": "127.0.0.1",
        "port": 9101,
        "assignment_epoch": 0,
        "loan_id": None,
    }
    manager = WorkerSessionManager("127.0.0.1", 9000, config)
    assignment = {
        "version": "sd_2026_2_v1",
        "type": "task_assignment",
        "message_id": "msg-task-assignment-1",
        "request_id": "req-task-assignment-1",
        "timestamp_ms": 1727170000128,
        "source": {
            "group_label": "p2p-cluster",
            "node_id": "22222222-2222-4222-8222-222222222222",
            "node_label": "master",
            "node_role": "master",
        },
        "destination": {
            "group_label": "p2p-cluster",
            "node_id": "11111111-1111-4111-8111-111111111111",
            "node_label": "worker-a",
            "node_role": "worker",
        },
        "payload": {
            "task_id": "task-021",
            "task_label": "task-0001",
            "attempt_id": "attempt-abc",
            "task_kind": "sum_range",
            "parameters": {"start": 0, "end": 10},
            "assignment_epoch": 7,
            "loan_id": None,
        },
    }

    manager.process_inbound_message(assignment)
    assert manager.busy is True
    assert manager.current_task["task_id"] == "task-021"
    assert manager.current_task["task_label"] == "task-0001"
    assert manager.should_poll_for_task() is False

    server_sock, client_sock = socket.socketpair()
    try:
        session = WorkerControlSession(client_sock)
        assert manager.request_task(session) is False
    finally:
        server_sock.close()
        client_sock.close()


def test_worker_session_manager_keeps_heartbeat_running_while_polling():
    config = {
        "group_label": "p2p-cluster",
        "node_id": "11111111-1111-4111-8111-111111111111",
        "node_label": "worker-a",
        "node_role": "worker",
        "host": "127.0.0.1",
        "port": 9101,
        "assignment_epoch": 0,
        "loan_id": None,
    }
    manager = WorkerSessionManager("127.0.0.1", 9000, config)
    server_sock, client_sock = socket.socketpair()
    try:
        session = WorkerControlSession(client_sock)
        manager._set_session(session)
        manager._start_heartbeat_for_session(session)
        assert manager._current_heartbeat_thread is not None
        assert manager._current_heartbeat_thread.is_alive() is True
        manager._set_task_polling_thread(threading.Thread(target=lambda: None, daemon=True))
        assert manager._task_polling_thread is not None
    finally:
        server_sock.close()
        client_sock.close()


def test_master_handles_task_submit_round_trip_and_conflict_over_tcp():
    from master.master import MasterTaskQueue

    registry = MasterRegistry()
    master_identity = {
        "group_label": "p2p-cluster",
        "node_id": "22222222-2222-4222-8222-222222222222",
        "node_label": "master",
        "node_role": "master",
    }
    task_queue = MasterTaskQueue()

    def exchange(message: dict):
        server_sock, client_sock = socket.socketpair()
        thread = threading.Thread(target=handle_client, args=(server_sock, None, registry, master_identity, task_queue), daemon=True)
        thread.start()
        client_sock.settimeout(2)
        try:
            send_msg(client_sock, message)
            raw = client_sock.recv(4096)
        finally:
            client_sock.close()

        if not raw:
            return None
        return json.loads(raw.decode("utf-8").strip())

    task_payload = {
        "task_id": "tcp-task-001",
        "task_kind": "sum_range",
        "parameters": {"start": 0, "end": 10},
    }
    message = {
        "version": "sd_2026_2_v1",
        "type": "task_submit",
        "message_id": "msg-task-submit-1",
        "request_id": "req-task-submit-1",
        "timestamp_ms": 1727170000123,
        "source": {
            "group_label": "p2p-cluster",
            "node_id": "11111111-1111-4111-8111-111111111111",
            "node_label": "worker-a",
            "node_role": "worker",
        },
        "destination": {
            "group_label": "p2p-cluster",
            "node_id": "22222222-2222-4222-8222-222222222222",
            "node_label": "master",
            "node_role": "master",
        },
        "payload": task_payload,
    }
    ack = exchange(message)
    assert ack is not None
    assert ack["type"] == "task_submit_ack"
    assert ack["payload"]["task_id"] == "tcp-task-001"
    assert ack["payload"]["accepted"] is True
    assert ack["payload"]["task_label"] == "task-0001"
    assert task_queue.pending()[0]["task_id"] == "tcp-task-001"

    duplicate_ack = exchange(message)
    assert duplicate_ack["payload"]["accepted"] is True
    assert duplicate_ack["payload"]["task_label"] == ack["payload"]["task_label"]
    assert len(task_queue.pending()) == 1

    conflict_message = {
        **message,
        "message_id": "msg-task-submit-conflict",
        "request_id": "req-task-submit-conflict",
        "payload": {
            "task_id": "tcp-task-001",
            "task_kind": "sum_range",
            "parameters": {"start": 0, "end": 20},
        },
    }
    conflict_ack = exchange(conflict_message)
    assert conflict_ack["type"] == "task_submit_ack"
    assert conflict_ack["payload"]["accepted"] is False
    assert conflict_ack["payload"]["reason"] == "task_id conflict"
    assert task_queue.get_task("tcp-task-001")["task_label"] == "task-0001"


def test_master_returns_protocol_error_for_invalid_task_submit_over_tcp():
    from master.master import MasterTaskQueue

    registry = MasterRegistry()
    master_identity = {
        "group_label": "p2p-cluster",
        "node_id": "22222222-2222-4222-8222-222222222222",
        "node_label": "master",
        "node_role": "master",
    }
    task_queue = MasterTaskQueue()

    def exchange(message: dict):
        server_sock, client_sock = socket.socketpair()
        thread = threading.Thread(target=handle_client, args=(server_sock, None, registry, master_identity, task_queue), daemon=True)
        thread.start()
        client_sock.settimeout(2)
        try:
            send_msg(client_sock, message)
            raw = client_sock.recv(4096)
        finally:
            client_sock.close()

        if not raw:
            return None
        return json.loads(raw.decode("utf-8").strip())

    invalid = {
        "version": "sd_2026_2_v1",
        "type": "task_submit",
        "message_id": "msg-task-submit-invalid",
        "request_id": "req-task-submit-invalid",
        "timestamp_ms": 1727170000123,
        "source": {
            "group_label": "p2p-cluster",
            "node_id": "11111111-1111-4111-8111-111111111111",
            "node_label": "worker-a",
            "node_role": "worker",
        },
        "destination": {
            "group_label": "p2p-cluster",
            "node_id": "22222222-2222-4222-8222-222222222222",
            "node_label": "master",
            "node_role": "master",
        },
        "payload": {
            "task_id": "tcp-task-999",
            "task_kind": "sum_range",
            "parameters": {"start": 12, "end": 3},
        },
    }
    reply = exchange(invalid)
    assert reply is not None
    assert reply["type"] == "protocol_error"
    assert reply["payload"]["status"] == "error"
    assert "reason" in reply["payload"]
    assert len(task_queue.pending()) == 0


def test_ct_02_two_workers_reconnect_and_identity_conflict_acceptance():
    registry = MasterRegistry()
    worker_a = {
        "node_id": "11111111-1111-4111-8111-111111111111",
        "node_label": "worker-a",
        "group_label": "p2p-cluster",
        "node_role": "worker",
        "host": "127.0.0.1",
        "port": 9101,
    }
    worker_b = {
        "node_id": "22222222-2222-4222-8222-222222222222",
        "node_label": "worker-b",
        "group_label": "p2p-cluster",
        "node_role": "worker",
        "host": "127.0.0.1",
        "port": 9102,
    }

    registry.upsert_worker(worker_a)
    registry.upsert_worker(worker_b)

    assert len(registry.workers) == 2
    assert set(registry.workers) == {worker_a["node_id"], worker_b["node_id"]}
    assert set(registry.node_labels) == {"worker-a", "worker-b"}

    registry.mark_worker_unavailable(worker_a["node_id"], reason="socket closed")
    assert registry.get_worker(worker_a["node_id"])["status"] == "unavailable"
    assert registry.get_worker(worker_b["node_id"])["status"] == "available"

    registry.upsert_worker(worker_a)
    assert len(registry.workers) == 2
    assert set(registry.workers) == {worker_a["node_id"], worker_b["node_id"]}
    assert registry.node_labels["worker-a"] == worker_a["node_id"]

    conflicting_label = {
        "node_id": "33333333-3333-4333-8333-333333333333",
        "node_label": "worker-a",
        "group_label": "p2p-cluster",
        "node_role": "worker",
        "host": "127.0.0.1",
        "port": 9103,
    }
    with pytest.raises(ValueError, match="node_label already registered for another worker"):
        registry.upsert_worker(conflicting_label)

    registry.upsert_worker(worker_b)
    assert len(registry.workers) == 2
    assert set(registry.node_labels) == {"worker-a", "worker-b"}


def test_sum_range_computes_expected_totals():
    from worker.worker import sum_range

    assert sum_range(0, 30) == 465
    assert sum_range(5, 5) == 5
    with pytest.raises(ValueError):
        sum_range(10, 3)


def test_worker_sends_task_result_and_master_acks_it():
    from worker.worker import WorkerControlSession, WorkerSessionManager

    registry = MasterRegistry()
    master_identity = {
        "group_label": "p2p-cluster",
        "node_id": "22222222-2222-4222-8222-222222222222",
        "node_label": "master",
        "node_role": "master",
    }
    config = {
        "group_label": "p2p-cluster",
        "node_id": "11111111-1111-4111-8111-111111111111",
        "node_label": "worker-a",
        "node_role": "worker",
        "host": "127.0.0.1",
        "port": 9101,
        "assignment_epoch": 7,
        "loan_id": None,
    }
    manager = WorkerSessionManager("127.0.0.1", 9000, config)
    server_sock, client_sock = socket.socketpair()
    try:
        session = WorkerControlSession(client_sock)
        manager._set_session(session)

        assignment = {
            "version": "sd_2026_2_v1",
            "type": "task_assignment",
            "message_id": "msg-task-assignment-1",
            "request_id": "req-task-assignment-1",
            "timestamp_ms": 1727170000128,
            "source": {
                "group_label": "p2p-cluster",
                "node_id": "22222222-2222-4222-8222-222222222222",
                "node_label": "master",
                "node_role": "master",
            },
            "destination": {
                "group_label": "p2p-cluster",
                "node_id": "11111111-1111-4111-8111-111111111111",
                "node_label": "worker-a",
                "node_role": "worker",
            },
            "payload": {
                "task_id": "task-021",
                "task_label": "task-0001",
                "attempt_id": "attempt-abc",
                "task_kind": "sum_range",
                "parameters": {"start": 0, "end": 30},
                "assignment_epoch": 7,
                "loan_id": None,
            },
        }

        result_message = manager.process_inbound_message(assignment)
        assert result_message is not None
        assert result_message["type"] == "task_result"
        assert result_message["payload"]["task_id"] == "task-021"
        assert result_message["payload"]["status"] == "ok"
        assert result_message["payload"]["result"] == {"sum": 465}
        assert result_message["payload"]["error_code"] is None

        master_ack = {
            "version": "sd_2026_2_v1",
            "type": "task_ack",
            "message_id": "msg-task-ack-1",
            "request_id": result_message["request_id"],
            "timestamp_ms": 1727170000130,
            "source": {
                "group_label": "p2p-cluster",
                "node_id": "22222222-2222-4222-8222-222222222222",
                "node_label": "master",
                "node_role": "master",
            },
            "destination": {
                "group_label": "p2p-cluster",
                "node_id": "11111111-1111-4111-8111-111111111111",
                "node_label": "worker-a",
                "node_role": "worker",
            },
            "payload": {
                "task_id": "task-021",
                "attempt_id": "attempt-abc",
                "accepted": True,
                "reason": "completed",
            },
        }
        manager.process_task_ack(master_ack)
        assert manager.busy is False
        assert manager.current_task is None

        raw = server_sock.recv(4096)
        task_result = json.loads(raw.decode("utf-8").strip())
        assert task_result["type"] == "task_result"
        assert task_result["payload"]["status"] == "ok"
    finally:
        server_sock.close()
        client_sock.close()


def test_worker_resumes_polling_after_accepted_task_ack():
    from worker.worker import WorkerControlSession, WorkerSessionManager

    config = {
        "group_label": "p2p-cluster",
        "node_id": "11111111-1111-4111-8111-111111111111",
        "node_label": "worker-b",
        "node_role": "worker",
        "host": "127.0.0.1",
        "port": 9102,
        "assignment_epoch": 3,
        "loan_id": None,
    }
    manager = WorkerSessionManager("127.0.0.1", 9000, config)
    server_sock, client_sock = socket.socketpair()
    try:
        session = WorkerControlSession(client_sock)
        manager._set_session(session)
        manager.stop_event.clear()

        assignment = {
            "version": "sd_2026_2_v1",
            "type": "task_assignment",
            "message_id": "msg-task-assignment-2",
            "request_id": "req-task-assignment-2",
            "timestamp_ms": 1727170000228,
            "source": {
                "group_label": "p2p-cluster",
                "node_id": "22222222-2222-4222-8222-222222222222",
                "node_label": "master",
                "node_role": "master",
            },
            "destination": {
                "group_label": "p2p-cluster",
                "node_id": "11111111-1111-4111-8111-111111111111",
                "node_label": "worker-b",
                "node_role": "worker",
            },
            "payload": {
                "task_id": "task-022",
                "task_label": "task-0002",
                "attempt_id": "attempt-xyz",
                "task_kind": "sum_range",
                "parameters": {"start": 0, "end": 10},
                "assignment_epoch": 3,
                "loan_id": None,
            },
        }
        task_ack = {
            "version": "sd_2026_2_v1",
            "type": "task_ack",
            "message_id": "msg-task-ack-2",
            "request_id": "req-task-assignment-2",
            "timestamp_ms": 1727170000230,
            "source": {
                "group_label": "p2p-cluster",
                "node_id": "22222222-2222-4222-8222-222222222222",
                "node_label": "master",
                "node_role": "master",
            },
            "destination": {
                "group_label": "p2p-cluster",
                "node_id": "11111111-1111-4111-8111-111111111111",
                "node_label": "worker-b",
                "node_role": "worker",
            },
            "payload": {
                "task_id": "task-022",
                "attempt_id": "attempt-xyz",
                "accepted": True,
                "reason": "completed",
            },
        }

        manager.start_task_polling_loop(session, interval_seconds=0.05)
        session._queue.put(assignment)
        time.sleep(0.25)
        assert manager.busy is True

        session._queue.put(task_ack)
        time.sleep(0.30)

        assert manager.busy is False
        assert manager.current_task is None
        manager.stop_event.set()
    finally:
        manager.stop_event.set()
        server_sock.close()
        client_sock.close()


def test_worker_handles_two_consecutive_tasks():
    from worker.worker import WorkerControlSession, WorkerSessionManager

    config = {
        "group_label": "p2p-cluster",
        "node_id": "11111111-1111-4111-8111-111111111111",
        "node_label": "worker-a",
        "node_role": "worker",
        "host": "127.0.0.1",
        "port": 9101,
        "assignment_epoch": 5,
        "loan_id": None,
    }
    manager = WorkerSessionManager("127.0.0.1", 9000, config)
    server_sock, client_sock = socket.socketpair()
    try:
        session = WorkerControlSession(client_sock)
        manager._set_session(session)

        assignment_one = {
            "version": "sd_2026_2_v1",
            "type": "task_assignment",
            "message_id": "msg-task-assignment-3",
            "request_id": "req-task-assignment-3",
            "timestamp_ms": 1727170000300,
            "source": {
                "group_label": "p2p-cluster",
                "node_id": "22222222-2222-4222-8222-222222222222",
                "node_label": "master",
                "node_role": "master",
            },
            "destination": {
                "group_label": "p2p-cluster",
                "node_id": "11111111-1111-4111-8111-111111111111",
                "node_label": "worker-a",
                "node_role": "worker",
            },
            "payload": {
                "task_id": "task-030",
                "task_label": "task-0003",
                "attempt_id": "attempt-01",
                "task_kind": "sum_range",
                "parameters": {"start": 0, "end": 5},
                "assignment_epoch": 5,
                "loan_id": None,
            },
        }
        assignment_two = {
            **assignment_one,
            "message_id": "msg-task-assignment-4",
            "request_id": "req-task-assignment-4",
            "payload": {
                **assignment_one["payload"],
                "task_id": "task-031",
                "task_label": "task-0004",
                "attempt_id": "attempt-02",
                "parameters": {"start": 6, "end": 10},
            },
        }

        result_one = manager.process_inbound_message(assignment_one)
        assert result_one["type"] == "task_result"
        ack_one = {
            "version": "sd_2026_2_v1",
            "type": "task_ack",
            "message_id": "msg-task-ack-3",
            "request_id": result_one["request_id"],
            "timestamp_ms": 1727170000305,
            "source": assignment_one["source"],
            "destination": assignment_one["destination"],
            "payload": {
                "task_id": "task-030",
                "attempt_id": "attempt-01",
                "accepted": True,
                "reason": "completed",
            },
        }
        assert manager.process_task_ack(ack_one) is True
        assert manager.busy is False

        result_two = manager.process_inbound_message(assignment_two)
        assert result_two["payload"]["result"] == {"sum": 40}
        ack_two = {
            **ack_one,
            "message_id": "msg-task-ack-4",
            "request_id": result_two["request_id"],
            "payload": {
                "task_id": "task-031",
                "attempt_id": "attempt-02",
                "accepted": True,
                "reason": "completed",
            },
        }
        assert manager.process_task_ack(ack_two) is True
        assert manager.busy is False
        assert manager.current_task is None
    finally:
        server_sock.close()
        client_sock.close()


def test_master_task_lifecycle_tracks_pending_and_running_states():
    from master.master import MasterTaskQueue

    queue = MasterTaskQueue()
    decision = queue.submit({"task_id": "task-lifecycle-1", "task_kind": "sum_range", "parameters": {"start": 0, "end": 10}})
    assert decision["accepted"] is True

    task = queue.get_task("task-lifecycle-1")
    assert task["status"] == "pending"
    assert task["attempt_count"] == 0

    assignment = queue.claim_next_task("worker-a", assignment_epoch=1, loan_id=None)
    assert assignment is not None
    assert assignment["task_id"] == "task-lifecycle-1"

    task_after_assignment = queue.get_task("task-lifecycle-1")
    assert task_after_assignment["status"] == "running"
    assert task_after_assignment["assigned_to"] == "worker-a"
    assert task_after_assignment["attempt_count"] == 1
    assert task_after_assignment["last_attempt_id"] == assignment["attempt_id"]


def test_master_runtime_reassigns_task_after_worker_disconnect():
    from master.master import MasterTaskQueue, handle_client

    registry = MasterRegistry()
    master_identity = {
        "group_label": "p2p-cluster",
        "node_id": "22222222-2222-4222-8222-222222222222",
        "node_label": "master",
        "node_role": "master",
        "host": "127.0.0.1",
        "port": 9000,
    }
    task_queue = MasterTaskQueue(task_timeout_ms=30000, max_task_attempts=3)
    task_queue.submit({"task_id": "task-runtime-disconnect", "task_kind": "sum_range", "parameters": {"start": 1, "end": 5}})

    worker_identity = {
        "group_label": "p2p-cluster",
        "node_id": "11111111-1111-4111-8111-111111111111",
        "node_label": "worker-a",
        "node_role": "worker",
    }
    registration_message = {
        "version": "sd_2026_2_v1",
        "type": "register_worker",
        "message_id": "msg-reg-runtime-disconnect",
        "request_id": "req-reg-runtime-disconnect",
        "timestamp_ms": int(time.time() * 1000),
        "source": {
            **worker_identity,
        },
        "destination": {
            "group_label": "p2p-cluster",
            "node_id": master_identity["node_id"],
            "node_label": master_identity["node_label"],
            "node_role": master_identity["node_role"],
        },
        "payload": build_register_payload(master_identity, "127.0.0.1", 9101, assignment_epoch=1),
    }

    server_sock, client_sock = socket.socketpair()
    thread = threading.Thread(target=handle_client, args=(server_sock, None, registry, master_identity, task_queue), daemon=True)
    thread.start()

    send_msg(client_sock, registration_message)
    time.sleep(0.05)

    assignment = task_queue.claim_next_task(worker_identity["node_id"], assignment_epoch=1, loan_id=None)
    assert assignment is not None
    assert task_queue.get_task("task-runtime-disconnect")["status"] == "running"

    client_sock.close()
    time.sleep(0.1)

    queued = task_queue.get_task("task-runtime-disconnect")
    assert queued["status"] == "pending"
    retry = task_queue.claim_next_task("22222222-2222-4222-8222-222222222223", assignment_epoch=2, loan_id=None)
    assert retry is not None
    assert retry["task_id"] == "task-runtime-disconnect"
    assert retry["task_label"] == queued["task_label"]
    assert retry["attempt_id"] != assignment["attempt_id"]


def test_master_runtime_reaps_timed_out_task_without_new_task_request():
    from master.master import MasterTaskQueue, start_task_reaper_loop

    task_queue = MasterTaskQueue(task_timeout_ms=20, max_task_attempts=3)
    task_queue.submit({"task_id": "task-runtime-timeout", "task_kind": "sum_range", "parameters": {"start": 1, "end": 5}})
    first_assignment = task_queue.claim_next_task("worker-a", assignment_epoch=1, loan_id=None)
    assert first_assignment is not None

    stop_event = threading.Event()
    thread = start_task_reaper_loop(task_queue, interval_seconds=0.01, stop_event=stop_event)
    time.sleep(0.12)
    stop_event.set()
    thread.join(timeout=1)

    queued = task_queue.get_task("task-runtime-timeout")
    assert queued["status"] == "pending"
    assert queued["attempt_count"] == 1
    retry = task_queue.claim_next_task("worker-b", assignment_epoch=2, loan_id=None)
    assert retry is not None
    assert retry["task_id"] == "task-runtime-timeout"
    assert retry["attempt_id"] != first_assignment["attempt_id"]


def test_master_third_attempt_can_succeed_and_only_fails_after_last_attempt_expires():
    from master.master import MasterTaskQueue

    queue = MasterTaskQueue(task_timeout_ms=50, max_task_attempts=3)
    queue.submit({"task_id": "task-third-attempt", "task_kind": "sum_range", "parameters": {"start": 1, "end": 10}})

    first = queue.claim_next_task("worker-a", assignment_epoch=1, loan_id=None)
    assert first is not None
    queue.handle_worker_unavailable("worker-a", reason="socket closed")
    second = queue.claim_next_task("worker-b", assignment_epoch=2, loan_id=None)
    assert second is not None
    assert second["attempt_id"] != first["attempt_id"]

    queue.release_task("task-third-attempt", reason="new_assignment")
    third = queue.claim_next_task("worker-c", assignment_epoch=3, loan_id=None)
    assert third is not None
    assert third["attempt_id"] != first["attempt_id"]
    assert queue.get_task("task-third-attempt")["status"] == "running"

    queue.complete_task("task-third-attempt", third["attempt_id"], accepted=True, reason="completed", payload={"result": 55})
    completed = queue.get_task("task-third-attempt")
    assert completed["status"] == "completed"
    assert completed["result_payload"]["result"] == 55

    queue.submit({"task_id": "task-third-attempt-final-fail", "task_kind": "sum_range", "parameters": {"start": 1, "end": 10}})
    first_fail = queue.claim_next_task("worker-a", assignment_epoch=1, loan_id=None)
    assert first_fail is not None
    queue.handle_worker_unavailable("worker-a", reason="socket closed")
    second_fail = queue.claim_next_task("worker-b", assignment_epoch=2, loan_id=None)
    assert second_fail is not None
    queue.handle_worker_unavailable("worker-b", reason="socket closed")
    third_fail = queue.claim_next_task("worker-c", assignment_epoch=3, loan_id=None)
    assert third_fail is not None
    queue.handle_worker_unavailable("worker-c", reason="socket closed")
    failed = queue.get_task("task-third-attempt-final-fail")
    assert failed["status"] == "failed"
    assert failed["failed_reason"] == "max_task_attempts_exceeded"


def test_master_reuses_attempt_id_for_same_retry_and_generates_new_one_for_new_assignment():
    from master.master import MasterTaskQueue

    queue = MasterTaskQueue()
    queue.submit({"task_id": "task-attempt-1", "task_kind": "sum_range", "parameters": {"start": 1, "end": 5}})

    first_assignment = queue.claim_next_task("worker-a", assignment_epoch=1, loan_id=None)
    assert first_assignment is not None
    first_attempt_id = first_assignment["attempt_id"]

    second_assignment = queue.claim_next_task("worker-a", assignment_epoch=1, loan_id=None)
    assert second_assignment is None

    queued = queue.get_task("task-attempt-1")
    assert queued["attempt_count"] == 1
    assert queued["last_attempt_id"] == first_attempt_id

    queue.release_task("task-attempt-1", reason="retransmit")
    retried = queue.claim_next_task("worker-a", assignment_epoch=2, loan_id=None)
    assert retried is not None
    assert retried["attempt_id"] == first_attempt_id

    queue.release_task("task-attempt-1", reason="new_assignment")
    next_assignment = queue.claim_next_task("worker-b", assignment_epoch=3, loan_id=None)
    assert next_assignment is not None
    assert next_assignment["attempt_id"] != first_attempt_id
    assert queue.get_task("task-attempt-1")["attempt_count"] == 2


def test_master_requeues_running_task_after_timeout_and_starts_new_attempt():
    from master.master import MasterTaskQueue

    queue = MasterTaskQueue(task_timeout_ms=5, max_task_attempts=3)
    queue.submit({"task_id": "task-timeout-1", "task_kind": "sum_range", "parameters": {"start": 1, "end": 5}})

    first_assignment = queue.claim_next_task("worker-a", assignment_epoch=1, loan_id=None)
    assert first_assignment is not None
    first_attempt_id = first_assignment["attempt_id"]

    queue.mark_task_timed_out("task-timeout-1", reason="timeout")
    queued = queue.get_task("task-timeout-1")
    assert queued["status"] == "pending"
    assert queued["attempt_count"] == 1
    assert queued["last_attempt_id"] == first_attempt_id

    retried = queue.claim_next_task("worker-b", assignment_epoch=2, loan_id=None)
    assert retried is not None
    assert retried["attempt_id"] != first_attempt_id
    assert retried["task_id"] == "task-timeout-1"
    assert retried["task_label"] == "task-0001"


def test_master_reassigns_task_when_worker_becomes_unavailable_and_starts_new_attempt():
    from master.master import MasterTaskQueue

    queue = MasterTaskQueue(task_timeout_ms=30000, max_task_attempts=3)
    queue.submit({"task_id": "task-worker-loss-1", "task_kind": "sum_range", "parameters": {"start": 10, "end": 20}})

    first_assignment = queue.claim_next_task("worker-a", assignment_epoch=1, loan_id=None)
    assert first_assignment is not None
    first_attempt_id = first_assignment["attempt_id"]

    queue.handle_worker_unavailable("worker-a", reason="socket closed")
    queued = queue.get_task("task-worker-loss-1")
    assert queued["status"] == "pending"
    assert queued["attempt_count"] == 1
    assert queued["last_attempt_id"] == first_attempt_id

    retried = queue.claim_next_task("worker-b", assignment_epoch=2, loan_id=None)
    assert retried is not None
    assert retried["attempt_id"] != first_attempt_id
    assert retried["task_id"] == "task-worker-loss-1"
    assert retried["task_label"] == "task-0001"


def test_master_marks_task_failed_after_max_attempts_are_exhausted():
    from master.master import MasterTaskQueue

    queue = MasterTaskQueue(task_timeout_ms=5, max_task_attempts=2)
    queue.submit({"task_id": "task-max-attempts-1", "task_kind": "sum_range", "parameters": {"start": 1, "end": 5}})

    first_assignment = queue.claim_next_task("worker-a", assignment_epoch=1, loan_id=None)
    assert first_assignment is not None
    queue.handle_worker_unavailable("worker-a", reason="socket closed")
    queue.release_task("task-max-attempts-1", reason="new_assignment")
    first_retry = queue.claim_next_task("worker-b", assignment_epoch=2, loan_id=None)
    assert first_retry is not None
    assert first_retry["attempt_id"] != first_assignment["attempt_id"]
    assert queue.get_task("task-max-attempts-1")["attempt_count"] == 2

    queue.handle_worker_unavailable("worker-b", reason="socket closed")
    queued = queue.get_task("task-max-attempts-1")
    assert queued["status"] == "failed"
    assert queued["attempt_count"] == 2
    assert queued["failed_reason"] == "max_task_attempts_exceeded"


def test_master_accepts_duplicate_task_result_for_same_attempt_and_does_not_count_twice():
    from master.master import MasterTaskQueue

    queue = MasterTaskQueue()
    queue.submit({"task_id": "task-dup-1", "task_kind": "sum_range", "parameters": {"start": 0, "end": 10}})
    assignment = queue.claim_next_task("worker-a", assignment_epoch=1, loan_id=None)
    assert assignment is not None

    accepted, reason = queue.complete_task("task-dup-1", assignment["attempt_id"], accepted=True, reason="completed", payload={"result": {"sum": 55}})
    assert accepted is True
    assert reason == "completed"
    assert queue.get_task("task-dup-1")["status"] == "completed"

    duplicate_accepted, duplicate_reason = queue.complete_task("task-dup-1", assignment["attempt_id"], accepted=True, reason="completed", payload={"result": {"sum": 55}})
    assert duplicate_accepted is True
    assert duplicate_reason == "duplicate result"
    current = queue.get_task("task-dup-1")
    assert current["status"] == "completed"
    assert current["result_payload"] == {"result": {"sum": 55}}


def test_master_rejects_stale_attempt_result_after_reassignment():
    from master.master import MasterTaskQueue

    queue = MasterTaskQueue(task_timeout_ms=30000, max_task_attempts=3)
    queue.submit({"task_id": "task-stale-1", "task_kind": "sum_range", "parameters": {"start": 1, "end": 5}})

    first_assignment = queue.claim_next_task("worker-a", assignment_epoch=1, loan_id=None)
    assert first_assignment is not None
    queue.handle_worker_unavailable("worker-a", reason="socket closed")

    second_assignment = queue.claim_next_task("worker-b", assignment_epoch=2, loan_id=None)
    assert second_assignment is not None
    assert second_assignment["task_id"] == "task-stale-1"
    assert second_assignment["attempt_id"] != first_assignment["attempt_id"]

    stale_accepted, stale_reason = queue.complete_task("task-stale-1", first_assignment["attempt_id"], accepted=True, reason="completed", payload={"result": {"sum": 15}})
    assert stale_accepted is False
    assert stale_reason == "stale_attempt"

    current = queue.get_task("task-stale-1")
    assert current["status"] == "running"
    assert current["assigned_to"] == "worker-b"
    assert current["active_attempt_id"] == second_assignment["attempt_id"]


def test_ct_03_invalid_messages_protocol_error_and_recovery_acceptance():
    registry = MasterRegistry()
    master_identity = {
        "group_label": "p2p-cluster",
        "node_id": "22222222-2222-4222-8222-222222222222",
        "node_label": "master",
        "node_role": "master",
    }

    def exchange(message: dict | str | bytes):
        server_sock, client_sock = socket.socketpair()
        thread = threading.Thread(target=handle_client, args=(server_sock, None, registry, master_identity), daemon=True)
        thread.start()
        client_sock.settimeout(2)
        try:
            if isinstance(message, (str, bytes)):
                payload = message if isinstance(message, bytes) else message.encode("utf-8")
                client_sock.sendall(payload)
            else:
                send_msg(client_sock, message)
            raw = client_sock.recv(4096)
        except socket.timeout:
            client_sock.close()
            return None
        finally:
            try:
                client_sock.close()
            except OSError:
                pass

        if not raw:
            return None
        return json.loads(raw.decode("utf-8").strip())

    missing_field = {
        "version": "sd_2026_2_v1",
        "type": "register_worker",
        "message_id": "msg-missing-field",
        "timestamp_ms": 1727170000123,
        "source": {
            "group_label": "p2p-cluster",
            "node_id": "11111111-1111-4111-8111-111111111111",
            "node_label": "worker-a",
            "node_role": "worker",
        },
        "destination": {
            "group_label": "p2p-cluster",
            "node_id": "22222222-2222-4222-8222-222222222222",
            "node_label": "master",
            "node_role": "master",
        },
        "payload": {"host": "127.0.0.1", "port": 9101},
    }
    wrong_capitalization = {
        "version": "sd_2026_2_v1",
        "Type": "register_worker",
        "message_id": "msg-capitalized",
        "request_id": "req-capitalized",
        "timestamp_ms": 1727170000123,
        "source": {
            "group_label": "p2p-cluster",
            "node_id": "11111111-1111-4111-8111-111111111111",
            "node_label": "worker-a",
            "node_role": "worker",
        },
        "destination": {
            "group_label": "p2p-cluster",
            "node_id": "22222222-2222-4222-8222-222222222222",
            "node_label": "master",
            "node_role": "master",
        },
        "payload": {"host": "127.0.0.1", "port": 9101},
    }
    unknown_type = {
        "version": "sd_2026_2_v1",
        "type": "unknown_event",
        "message_id": "msg-unknown-type",
        "request_id": "req-unknown-type",
        "timestamp_ms": 1727170000123,
        "source": {
            "group_label": "p2p-cluster",
            "node_id": "11111111-1111-4111-8111-111111111111",
            "node_label": "worker-a",
            "node_role": "worker",
        },
        "destination": {
            "group_label": "p2p-cluster",
            "node_id": "22222222-2222-4222-8222-222222222222",
            "node_label": "master",
            "node_role": "master",
        },
        "payload": {"detail": "unsupported"},
    }
    unknown_version = {
        "version": "sd_2026_2_v99",
        "type": "register_worker",
        "message_id": "msg-unknown-version",
        "request_id": "req-unknown-version",
        "timestamp_ms": 1727170000123,
        "source": {
            "group_label": "p2p-cluster",
            "node_id": "11111111-1111-4111-8111-111111111111",
            "node_label": "worker-a",
            "node_role": "worker",
        },
        "destination": {
            "group_label": "p2p-cluster",
            "node_id": "22222222-2222-4222-8222-222222222222",
            "node_label": "master",
            "node_role": "master",
        },
        "payload": {"host": "127.0.0.1", "port": 9101},
    }
    malformed_json = '{"version":"sd_2026_2_v1","type":'

    for message in (missing_field, wrong_capitalization, unknown_type, unknown_version):
        reply = exchange(message)
        assert reply is not None
        assert reply["type"] == "protocol_error"
        assert "reason" in reply["payload"]

    assert exchange(malformed_json) is None

    valid_message = {
        "version": "sd_2026_2_v1",
        "type": "register_worker",
        "message_id": "msg-valid-after-invalid",
        "request_id": "req-valid-after-invalid",
        "timestamp_ms": 1727170000456,
        "source": {
            "group_label": "p2p-cluster",
            "node_id": "44444444-4444-4444-8444-444444444444",
            "node_label": "worker-good",
            "node_role": "worker",
        },
        "destination": {
            "group_label": "p2p-cluster",
            "node_id": "22222222-2222-4222-8222-222222222222",
            "node_label": "master",
            "node_role": "master",
        },
        "payload": build_register_payload(
            {"group_label": "p2p-cluster", "node_id": "22222222-2222-4222-8222-222222222222", "node_label": "master", "node_role": "master", "host": "127.0.0.1", "port": 9000},
            "127.0.0.1",
            9106,
            assignment_epoch=1,
            loan_id="loan-valid-after-invalid",
        ),
    }

    valid_reply = exchange(valid_message)
    assert valid_reply is not None
    assert valid_reply["type"] == "registration_ack"
    assert valid_reply["payload"]["status"] == "accepted"
    assert len(registry.workers) == 1
    assert registry.workers["44444444-4444-4444-8444-444444444444"]["node_label"] == "worker-good"


def test_ct_01_ndjson_tcp_framing_acceptance():
    buffer = bytearray()
    messages = [
        {
            "version": "sd_2026_2_v1",
            "type": "register_worker",
            "message_id": "m1",
            "request_id": "r1",
            "timestamp_ms": 1727170000123,
            "source": {
                "group_label": "p2p-cluster",
                "node_id": str(uuid.uuid4()),
                "node_label": "worker-a",
                "node_role": "worker",
            },
            "destination": {
                "group_label": "p2p-cluster",
                "node_id": str(uuid.uuid4()),
                "node_label": "master",
                "node_role": "master",
            },
            "payload": build_register_payload({"group_label": "p2p-cluster", "node_id": str(uuid.uuid4()), "node_label": "master", "node_role": "master", "host": "127.0.0.1", "port": 9000}, "127.0.0.1", 9101, assignment_epoch=1, loan_id="loan-m1"),
        },
        {
            "version": "sd_2026_2_v1",
            "type": "register_worker",
            "message_id": "m2",
            "request_id": "r2",
            "timestamp_ms": 1727170002234,
            "source": {
                "group_label": "p2p-cluster",
                "node_id": str(uuid.uuid4()),
                "node_label": "worker-b",
                "node_role": "worker",
            },
            "destination": {
                "group_label": "p2p-cluster",
                "node_id": str(uuid.uuid4()),
                "node_label": "master",
                "node_role": "master",
            },
            "payload": build_register_payload({"group_label": "p2p-cluster", "node_id": str(uuid.uuid4()), "node_label": "master", "node_role": "master", "host": "127.0.0.1", "port": 9000}, "127.0.0.1", 9102, assignment_epoch=1, loan_id="loan-m2"),
        },
        {
            "version": "sd_2026_2_v1",
            "type": "register_worker",
            "message_id": "m3",
            "request_id": "r3",
            "timestamp_ms": 1727170003456,
            "source": {
                "group_label": "p2p-cluster",
                "node_id": str(uuid.uuid4()),
                "node_label": "worker-c",
                "node_role": "worker",
            },
            "destination": {
                "group_label": "p2p-cluster",
                "node_id": str(uuid.uuid4()),
                "node_label": "master",
                "node_role": "master",
            },
            "payload": build_register_payload({"group_label": "p2p-cluster", "node_id": str(uuid.uuid4()), "node_label": "master", "node_role": "master", "host": "127.0.0.1", "port": 9000}, "127.0.0.1", 9103, assignment_epoch=1, loan_id="loan-m3"),
        },
    ]

    serialized = [
        (json.dumps(message, separators=(",", ":")).encode("utf-8") + b"\n")
        for message in messages
    ]

    chunks = [
        serialized[0][:10],
        serialized[0][10:35],
        serialized[0][35:60],
        serialized[0][60:],
        serialized[1][:15],
        serialized[1][15:],
        serialized[2][:12],
        serialized[2][12:],
    ]

    received = []
    for chunk in chunks:
        buffer.extend(chunk)
        received.extend(extract_complete_messages(buffer))

    assert len(received) == 3
    assert all(validate_message(message) for message in received)
    assert [message["message_id"] for message in received] == ["m1", "m2", "m3"]
    assert received[0]["source"]["node_label"] == "worker-a"
    assert received[1]["source"]["node_label"] == "worker-b"
    assert received[2]["source"]["node_label"] == "worker-c"


def test_validate_message_rejects_missing_required_fields():
    invalid = {
        "version": "sd_2026_2_v1",
        "type": "register_worker",
        "message_id": "m1",
        "request_id": "r1",
        "timestamp_ms": 1727170000123,
    }
    assert validate_message(invalid) is False

    valid = {
        "version": "sd_2026_2_v1",
        "type": "register_worker",
        "message_id": "m1",
        "request_id": "r1",
        "timestamp_ms": 1727170000123,
        "source": {
            "group_label": "p2p-cluster",
            "node_id": "11111111-1111-4111-8111-111111111111",
            "node_label": "worker-a",
            "node_role": "worker",
        },
        "destination": {
            "group_label": "p2p-cluster",
            "node_id": "22222222-2222-4222-8222-222222222222",
            "node_label": "master",
            "node_role": "master",
        },
        "payload": build_register_payload({"group_label": "p2p-cluster", "node_id": "22222222-2222-4222-8222-222222222222", "node_label": "master", "node_role": "master", "host": "127.0.0.1", "port": 9000}, "127.0.0.1", 9101, assignment_epoch=1, loan_id="loan-valid"),
    }
    assert validate_message(valid) is True


def test_extract_complete_messages_discards_invalid_json_with_log(capfd):
    buffer = bytearray(b'{"broken":\n')
    assert extract_complete_messages(buffer) == []
    captured = capfd.readouterr().out
    assert "invalid_json" in captured.lower() or "discarded" in captured.lower()


def test_extract_complete_messages_rejects_duplicate_keys_and_non_finite_numbers():
    valid_message = {
        "version": "sd_2026_2_v1",
        "type": "register_worker",
        "message_id": "m-dup",
        "request_id": "r-dup",
        "timestamp_ms": 1727170000123,
        "source": {
            "group_label": "p2p-cluster",
            "node_id": "11111111-1111-4111-8111-111111111111",
            "node_label": "worker-a",
            "node_role": "worker",
        },
        "destination": {
            "group_label": "p2p-cluster",
            "node_id": "22222222-2222-4222-8222-222222222222",
            "node_label": "master",
            "node_role": "master",
        },
        "payload": build_register_payload({"group_label": "p2p-cluster", "node_id": "22222222-2222-4222-8222-222222222222", "node_label": "master", "node_role": "master", "host": "127.0.0.1", "port": 9000}, "127.0.0.1", 9101, assignment_epoch=1, loan_id="loan-dup"),
    }

    duplicate = (
        b'{"version":"sd_2026_2_v1","type":"register_worker","message_id":"m-dup","request_id":"r-dup",'
        b'"timestamp_ms":1727170000123,"source":{"group_label":"p2p-cluster","node_id":"11111111-1111-4111-8111-111111111111",'
        b'"node_label":"worker-a","node_role":"worker","node_role":"master"},'
        b'"destination":{"group_label":"p2p-cluster","node_id":"22222222-2222-4222-8222-222222222222",'
        b'"node_label":"master","node_role":"master"},"payload":{"host":"127.0.0.1","port":9101}}\n'
    )
    invalid_nan = (
        b'{"version":"sd_2026_2_v1","type":"register_worker","message_id":"m-nan","request_id":"r-nan",'
        b'"timestamp_ms":1727170000123,"source":{"group_label":"p2p-cluster","node_id":"11111111-1111-4111-8111-111111111111",'
        b'"node_label":"worker-a","node_role":"worker"},'
        b'"destination":{"group_label":"p2p-cluster","node_id":"22222222-2222-4222-8222-222222222222",'
        b'"node_label":"master","node_role":"master"},"payload":{"host":NaN,"port":9101}}\n'
    )

    assert extract_complete_messages(bytearray(duplicate)) == []
    assert extract_complete_messages(bytearray(invalid_nan)) == []


def test_extract_complete_messages_rejects_messages_over_65536_bytes():
    valid_message = {
        "version": "sd_2026_2_v1",
        "type": "register_worker",
        "message_id": "m-big",
        "request_id": "r-big",
        "timestamp_ms": 1727170000123,
        "source": {
            "group_label": "p2p-cluster",
            "node_id": "11111111-1111-4111-8111-111111111111",
            "node_label": "worker-a",
            "node_role": "worker",
        },
        "destination": {
            "group_label": "p2p-cluster",
            "node_id": "22222222-2222-4222-8222-222222222222",
            "node_label": "master",
            "node_role": "master",
        },
        "payload": {**build_register_payload({"group_label": "p2p-cluster", "node_id": "22222222-2222-4222-8222-222222222222", "node_label": "master", "node_role": "master", "host": "127.0.0.1", "port": 9000}, "127.0.0.1", 9101, assignment_epoch=1, loan_id="loan-big"), "blob": "x" * 70000},
    }

    payload = (json.dumps(valid_message, separators=(",", ":"), ensure_ascii=False) + "\n").encode("utf-8")
    assert len(payload) > 65536
    assert extract_complete_messages(bytearray(payload)) == []


def test_master_registry_updates_existing_uuid():
    registry = MasterRegistry()
    first = {"uuid": "u-9", "label": "worker-a", "host": "127.0.0.1", "port": 9001}
    second = {"uuid": "u-9", "label": "worker-a", "host": "127.0.0.1", "port": 9002}

    registry.upsert_worker(first)
    registry.upsert_worker(second)

    assert len(registry.workers) == 1
    assert registry.workers["u-9"]["port"] == 9002


def test_master_registry_is_idempotent_for_same_node_id_and_rejects_conflicts():
    registry = MasterRegistry()
    first = {
        "node_id": "11111111-1111-4111-8111-111111111111",
        "node_label": "worker-a",
        "group_label": "p2p-cluster",
        "node_role": "worker",
        "host": "127.0.0.1",
        "port": 9101,
    }
    second = {
        "node_id": "11111111-1111-4111-8111-111111111111",
        "node_label": "worker-a",
        "group_label": "p2p-cluster",
        "node_role": "worker",
        "host": "127.0.0.1",
        "port": 9102,
    }
    duplicate_label = {
        "node_id": "22222222-2222-4222-8222-222222222222",
        "node_label": "worker-a",
        "group_label": "p2p-cluster",
        "node_role": "worker",
        "host": "127.0.0.1",
        "port": 9103,
    }
    incompatible_same_id = {
        "node_id": "11111111-1111-4111-8111-111111111111",
        "node_label": "worker-b",
        "group_label": "p2p-cluster",
        "node_role": "worker",
        "host": "127.0.0.1",
        "port": 9104,
    }

    registry.upsert_worker(first)
    registry.upsert_worker(second)
    assert len(registry.workers) == 1
    assert registry.workers["11111111-1111-4111-8111-111111111111"]["port"] == 9102

    try:
        registry.upsert_worker(duplicate_label)
    except ValueError:
        pass
    else:
        raise AssertionError("duplicate node_label should be rejected")

    try:
        registry.upsert_worker(incompatible_same_id)
    except ValueError:
        pass
    else:
        raise AssertionError("same node_id with incompatible identity should be rejected")


def test_worker_config_embeds_master_identity_without_repo_dependency(tmp_path):
    a_path = tmp_path / "worker-a.json"
    b_path = tmp_path / "worker-b.json"

    worker_a = load_worker_config(a_path)
    worker_b = load_worker_config(b_path)

    assert worker_a["node_label"] == "worker-a"
    assert worker_b["node_label"] == "worker-b"
    assert worker_a["node_id"] != worker_b["node_id"]

    assert worker_a["master"]["node_role"] == "master"
    assert worker_a["master"]["node_label"] == "master"
    assert worker_a["master"]["host"] == "127.0.0.1"
    assert worker_a["master"]["port"] == 9000
    assert uuid.UUID(worker_a["master"]["node_id"]).version == 4
    assert worker_a["master"] == worker_b["master"]


def test_worker_config_persists_master_metadata_between_reloads(tmp_path):
    config_path = tmp_path / "worker-a.json"

    first = load_worker_config(config_path)
    second = load_worker_config(config_path)

    assert first["node_id"] == second["node_id"]
    assert first["master"] == second["master"]
    assert first["master"]["port"] == 9000


def test_config_identity_is_persistent_and_v4_for_master_and_worker(tmp_path):
    master_path = tmp_path / "master-config.json"
    worker_path = tmp_path / "worker-config.json"

    master_config = load_master_config(master_path)
    worker_config = load_worker_config(worker_path)

    for config, expected_role in ((master_config, "master"), (worker_config, "worker")):
        assert config["group_label"]
        assert config["node_role"] == expected_role
        assert config["node_label"]
        assert config["host"]
        assert isinstance(config["port"], int)
        assert config["node_id"]
        parsed = uuid.UUID(config["node_id"])
        assert parsed.version == 4

    master_again = load_master_config(master_path)
    worker_again = load_worker_config(worker_path)
    assert master_again["node_id"] == master_config["node_id"]
    assert worker_again["node_id"] == worker_config["node_id"]


def test_validate_message_accepts_sd_2026_2_v1_envelope():
    master_uuid = str(uuid.uuid4())
    worker_uuid = str(uuid.uuid4())
    message = {
        "version": "sd_2026_2_v1",
        "type": "register_worker",
        "message_id": "msg-001",
        "request_id": "req-001",
        "timestamp_ms": 1727170000123,
        "source": {
            "group_label": "p2p-cluster",
            "node_id": worker_uuid,
            "node_label": "worker-a",
            "node_role": "worker",
        },
        "destination": {
            "group_label": "p2p-cluster",
            "node_id": master_uuid,
            "node_label": "master",
            "node_role": "master",
        },
        "payload": build_register_payload({"group_label": "p2p-cluster", "node_id": master_uuid, "node_label": "master", "node_role": "master", "host": "127.0.0.1", "port": 9000}, "127.0.0.1", 9101, assignment_epoch=1, loan_id="loan-valid-envelope"),
    }

    assert validate_message(message) is True


def test_register_worker_payload_requires_home_master_assignment_and_loan_id(tmp_path):
    registry = MasterRegistry()
    master_identity = {
        "group_label": "p2p-cluster",
        "node_id": "22222222-2222-4222-8222-222222222222",
        "node_label": "master",
        "node_role": "master",
    }

    def exchange(message):
        server_sock, client_sock = socket.socketpair()
        thread = threading.Thread(target=handle_client, args=(server_sock, None, registry, master_identity), daemon=True)
        thread.start()
        client_sock.settimeout(2)
        send_msg(client_sock, message)
        try:
            raw = client_sock.recv(4096)
        finally:
            client_sock.close()
        return json.loads(raw.decode("utf-8").strip())

    worker_config = load_worker_config(tmp_path / "worker-a.json")
    valid_request = {
        "version": "sd_2026_2_v1",
        "type": "register_worker",
        "message_id": "msg-contract-valid",
        "request_id": "req-contract-valid",
        "timestamp_ms": 1727170000123,
        "source": {
            "group_label": "p2p-cluster",
            "node_id": worker_config["node_id"],
            "node_label": worker_config["node_label"],
            "node_role": "worker",
        },
        "destination": {
            "group_label": "p2p-cluster",
            "node_id": master_identity["node_id"],
            "node_label": "master",
            "node_role": "master",
        },
        "payload": {
            "home_master": {
                "group_label": master_identity["group_label"],
                "node_id": master_identity["node_id"],
                "node_label": master_identity["node_label"],
                "node_role": "master",
                "host": "127.0.0.1",
                "port": 9000,
            },
            "assignment_epoch": 0,
            "loan_id": None,
            "host": worker_config["host"],
            "port": worker_config["port"],
        },
    }

    valid_reply = exchange(valid_request)
    assert valid_reply["type"] == "registration_ack"
    assert valid_reply["payload"]["status"] == "accepted"
    assert valid_reply["payload"]["assignment_epoch"] == 0
    assert valid_reply["payload"]["worker"]["node_label"] == "worker-a"

    invalid_request = dict(valid_request)
    invalid_request["payload"] = {
        "home_master": {
            "group_label": master_identity["group_label"],
            "node_id": master_identity["node_id"],
            "node_label": master_identity["node_label"],
            "node_role": "master",
            "host": "127.0.0.1",
            "port": 9000,
        },
        "loan_id": "loan-invalid",
        "host": worker_config["host"],
        "port": worker_config["port"],
    }
    invalid_reply = exchange(invalid_request)
    assert invalid_reply["type"] == "protocol_error"
    assert "assignment_epoch" in invalid_reply["payload"]["reason"].lower()


def test_register_worker_rejects_group_label_mismatch_and_master_stays_operational():
    registry = MasterRegistry()
    master_identity = {
        "group_label": "p2p-cluster",
        "node_id": "22222222-2222-4222-8222-222222222222",
        "node_label": "master",
        "node_role": "master",
    }

    def register_once(message):
        server_sock, client_sock = socket.socketpair()
        thread = threading.Thread(target=handle_client, args=(server_sock, None, registry, master_identity), daemon=True)
        thread.start()
        client_sock.settimeout(2)
        send_msg(client_sock, message)
        try:
            raw = client_sock.recv(4096)
        finally:
            client_sock.close()
        payload = json.loads(raw.decode("utf-8").strip())
        return payload

    rejected_request = {
        "version": "sd_2026_2_v1",
        "type": "register_worker",
        "message_id": "msg-reject-1",
        "request_id": "req-reject-1",
        "timestamp_ms": 1727170000123,
        "source": {
            "group_label": "other-group",
            "node_id": "33333333-3333-4333-8333-333333333333",
            "node_label": "worker-bad",
            "node_role": "worker",
        },
        "destination": {
            "group_label": "p2p-cluster",
            "node_id": "22222222-2222-4222-8222-222222222222",
            "node_label": "master",
            "node_role": "master",
        },
        "payload": build_register_payload({"group_label": "p2p-cluster", "node_id": "22222222-2222-4222-8222-222222222222", "node_label": "master", "node_role": "master", "host": "127.0.0.1", "port": 9000}, "127.0.0.1", 9105, assignment_epoch=1, loan_id="loan-reject"),
    }

    rejected_reply = register_once(rejected_request)
    assert rejected_reply["payload"]["status"] == "rejected"
    assert "group_label" in str(rejected_reply["payload"]["reason"]).lower()
    assert len(registry.workers) == 0

    accepted_request = {
        "version": "sd_2026_2_v1",
        "type": "register_worker",
        "message_id": "msg-accept-1",
        "request_id": "req-accept-1",
        "timestamp_ms": 1727170000456,
        "source": {
            "group_label": "p2p-cluster",
            "node_id": "44444444-4444-4444-8444-444444444444",
            "node_label": "worker-good",
            "node_role": "worker",
        },
        "destination": {
            "group_label": "p2p-cluster",
            "node_id": "22222222-2222-4222-8222-222222222222",
            "node_label": "master",
            "node_role": "master",
        },
        "payload": build_register_payload({"group_label": "p2p-cluster", "node_id": "22222222-2222-4222-8222-222222222222", "node_label": "master", "node_role": "master", "host": "127.0.0.1", "port": 9000}, "127.0.0.1", 9106, assignment_epoch=1, loan_id="loan-accept"),
    }

    accepted_reply = register_once(accepted_request)
    assert accepted_reply["payload"]["status"] == "accepted"
    assert len(registry.workers) == 1
    assert registry.workers["44444444-4444-4444-8444-444444444444"]["node_label"] == "worker-good"


def test_heartbeat_messages_follow_sd_2026_2_v1_and_correlate_sequence():
    heart = build_heartbeat_message(
        source={
            "group_label": "p2p-cluster",
            "node_id": "11111111-1111-4111-8111-111111111111",
            "node_label": "worker-a",
            "node_role": "worker",
        },
        destination={
            "group_label": "p2p-cluster",
            "node_id": "22222222-2222-4222-8222-222222222222",
            "node_label": "master",
            "node_role": "master",
        },
        sequence=7,
        request_id="req-heart-7",
        assignment_epoch=0,
    )
    ack = build_heartbeat_ack_message(heart, source={
        "group_label": "p2p-cluster",
        "node_id": "22222222-2222-4222-8222-222222222222",
        "node_label": "master",
        "node_role": "master",
    }, destination={
        "group_label": "p2p-cluster",
        "node_id": "11111111-1111-4111-8111-111111111111",
        "node_label": "worker-a",
        "node_role": "worker",
    })

    assert validate_message(heart) is True
    assert heart["type"] == "heartbeat"
    assert heart["payload"]["sequence"] == 7
    assert heart["payload"]["assignment_epoch"] == 0
    assert ack["type"] == "heartbeat_ack"
    assert ack["request_id"] == heart["request_id"]
    assert ack["payload"]["sequence"] == 7
    assert ack["payload"]["status"] == "alive"


def test_worker_heartbeat_uses_current_assignment_epoch_from_config_and_preserves_it_across_retries():
    config = {"group_label": "p2p-cluster", "node_id": "11111111-1111-4111-8111-111111111111", "node_label": "worker-a", "node_role": "worker", "assignment_epoch": 3}
    heart = build_heartbeat_message(
        source={
            "group_label": config["group_label"],
            "node_id": config["node_id"],
            "node_label": config["node_label"],
            "node_role": config["node_role"],
        },
        destination={
            "group_label": "p2p-cluster",
            "node_id": "22222222-2222-4222-8222-222222222222",
            "node_label": "master",
            "node_role": "master",
        },
        sequence=12,
        request_id="req-heart-config-3",
        assignment_epoch=config["assignment_epoch"],
    )
    retry_copy = dict(heart)
    retry_copy["payload"] = dict(heart["payload"])

    assert validate_message(heart) is True
    assert heart["payload"]["assignment_epoch"] == 3
    assert retry_copy == heart


def test_heartbeat_rejects_invalid_assignment_epoch_values():
    base = {
        "version": "sd_2026_2_v1",
        "type": "heartbeat",
        "message_id": "msg-heart-invalid-assignment",
        "request_id": "req-heart-invalid-assignment",
        "timestamp_ms": 1727170000123,
        "source": {
            "group_label": "p2p-cluster",
            "node_id": "11111111-1111-4111-8111-111111111111",
            "node_label": "worker-a",
            "node_role": "worker",
        },
        "destination": {
            "group_label": "p2p-cluster",
            "node_id": "22222222-2222-4222-8222-222222222222",
            "node_label": "master",
            "node_role": "master",
        },
        "payload": {"sequence": 1, "assignment_epoch": 0},
    }

    for invalid_value in (-1, True, "1", 1.5, None):
        payload = {**base["payload"], "assignment_epoch": invalid_value}
        assert validate_message({**base, "payload": payload}) is False


def test_valid_heartbeat_still_produces_heartbeat_ack_when_assignment_epoch_is_present():
    heart = build_heartbeat_message(
        source={
            "group_label": "p2p-cluster",
            "node_id": "11111111-1111-4111-8111-111111111111",
            "node_label": "worker-a",
            "node_role": "worker",
        },
        destination={
            "group_label": "p2p-cluster",
            "node_id": "22222222-2222-4222-8222-222222222222",
            "node_label": "master",
            "node_role": "master",
        },
        sequence=5,
        request_id="req-heart-valid-ack",
        assignment_epoch=0,
    )

    assert validate_message(heart) is True
    ack = build_heartbeat_ack_message(heart, source={
        "group_label": "p2p-cluster",
        "node_id": "22222222-2222-4222-8222-222222222222",
        "node_label": "master",
        "node_role": "master",
    }, destination=heart["source"])
    assert ack["type"] == "heartbeat_ack"
    assert ack["payload"]["status"] == "alive"
    assert ack["payload"]["sequence"] == heart["payload"]["sequence"]


def test_master_replies_to_worker_heartbeat():
    registry = MasterRegistry()
    master_identity = {
        "group_label": "p2p-cluster",
        "node_id": "22222222-2222-4222-8222-222222222222",
        "node_label": "master",
        "node_role": "master",
    }

    def exchange(message):
        server_sock, client_sock = socket.socketpair()
        thread = threading.Thread(target=handle_client, args=(server_sock, None, registry, master_identity), daemon=True)
        thread.start()
        client_sock.settimeout(2)
        send_msg(client_sock, message)
        try:
            raw = client_sock.recv(4096)
        finally:
            client_sock.close()
        return json.loads(raw.decode("utf-8").strip())

    heartbeat = {
        "version": "sd_2026_2_v1",
        "type": "heartbeat",
        "message_id": "hb-1",
        "request_id": "req-hb-1",
        "timestamp_ms": 1727170000123,
        "source": {
            "group_label": "p2p-cluster",
            "node_id": "11111111-1111-4111-8111-111111111111",
            "node_label": "worker-a",
            "node_role": "worker",
        },
        "destination": {
            "group_label": "p2p-cluster",
            "node_id": "22222222-2222-4222-8222-222222222222",
            "node_label": "master",
            "node_role": "master",
        },
        "payload": {"sequence": 9, "assignment_epoch": 0},
    }

    reply = exchange(heartbeat)
    assert reply["type"] == "heartbeat_ack"
    assert reply["request_id"] == heartbeat["request_id"]
    assert reply["payload"]["sequence"] == 9
    assert reply["payload"]["status"] == "alive"


def test_build_ack_message_uses_official_envelope_and_echoes_request_id():
    request = {
        "version": "sd_2026_2_v1",
        "type": "register_worker",
        "message_id": "msg-req",
        "request_id": "req-ack",
        "timestamp_ms": 1727170000123,
        "source": {
            "group_label": "p2p-cluster",
            "node_id": "11111111-1111-4111-8111-111111111111",
            "node_label": "worker-a",
            "node_role": "worker",
        },
        "destination": {
            "group_label": "p2p-cluster",
            "node_id": "22222222-2222-4222-8222-222222222222",
            "node_label": "master",
            "node_role": "master",
        },
        "payload": {"host": "127.0.0.1", "port": 9101},
    }
    master_identity = {
        "group_label": "p2p-cluster",
        "node_id": "22222222-2222-4222-8222-222222222222",
        "node_label": "master",
        "node_role": "master",
    }

    ack = build_ack_message(request, "accepted", master_identity)
    rejected = build_ack_message(request, "rejected", master_identity, reason="duplicate node_label")

    assert ack["version"] == "sd_2026_2_v1"
    assert ack["request_id"] == request["request_id"]
    assert ack["message_id"] != request["message_id"]
    assert ack["source"]["node_role"] == "master"
    assert ack["destination"]["node_role"] == "worker"
    assert ack["payload"]["status"] == "accepted"

    assert rejected["payload"]["status"] == "rejected"
    assert rejected["payload"]["reason"] == "duplicate node_label"


def test_master_send_heartbeat_uses_existing_socket_and_preserves_sequence():
    server_sock, client_sock = socket.socketpair()
    master_identity = {
        "group_label": "p2p-cluster",
        "node_id": "22222222-2222-4222-8222-222222222222",
        "node_label": "master",
        "node_role": "master",
    }
    worker_identity = {
        "group_label": "p2p-cluster",
        "node_id": "11111111-1111-4111-8111-111111111111",
        "node_label": "worker-a",
        "node_role": "worker",
    }
    write_lock = threading.Lock()

    send_heartbeat_to_worker(server_sock, master_identity, worker_identity, write_lock, sequence=21, request_id="req-master-heart-21")
    raw = client_sock.recv(4096)
    message = json.loads(raw.decode("utf-8").strip())

    assert message["type"] == "heartbeat"
    assert message["request_id"] == "req-master-heart-21"
    assert message["payload"]["sequence"] == 21
    assert message["source"]["node_label"] == "master"
    assert message["destination"]["node_label"] == "worker-a"


def test_worker_control_session_reuses_one_socket_for_successive_heartbeats():
    server_sock, client_sock = socket.socketpair()
    session = WorkerControlSession(client_sock)

    heartbeat_one = build_heartbeat_message(
        source={
            "group_label": "p2p-cluster",
            "node_id": "11111111-1111-4111-8111-111111111111",
            "node_label": "worker-a",
            "node_role": "worker",
        },
        destination={
            "group_label": "p2p-cluster",
            "node_id": "22222222-2222-4222-8222-222222222222",
            "node_label": "master",
            "node_role": "master",
        },
        sequence=11,
        request_id="req-hb-11",
    )
    heartbeat_two = build_heartbeat_message(
        source=heartbeat_one["source"],
        destination=heartbeat_one["destination"],
        sequence=12,
        request_id="req-hb-12",
    )

    session.send_message(heartbeat_one)
    session.send_message(heartbeat_two)

    first_ack = build_heartbeat_ack_message(heartbeat_one, source=heartbeat_one["destination"], destination=heartbeat_one["source"])
    second_ack = build_heartbeat_ack_message(heartbeat_two, source=heartbeat_two["destination"], destination=heartbeat_two["source"])

    send_msg(server_sock, first_ack)
    send_msg(server_sock, second_ack)

    first_response = session.wait_for_message(types={"heartbeat_ack"}, request_id="req-hb-11", timeout=2)
    second_response = session.wait_for_message(types={"heartbeat_ack"}, request_id="req-hb-12", timeout=2)

    assert first_response["payload"]["sequence"] == 11
    assert second_response["payload"]["sequence"] == 12
    assert session.sock is client_sock


def test_master_registry_expired_expectation_counts_once_per_period():
    registry = MasterRegistry()
    registry.upsert_worker({
        "node_id": "11111111-1111-4111-8111-111111111111",
        "node_label": "worker-a",
        "group_label": "p2p-cluster",
        "node_role": "worker",
        "host": "127.0.0.1",
        "port": 9101,
    })

    registry.begin_heartbeat_expectation("11111111-1111-4111-8111-111111111111", "req-1", 1)
    registry.begin_heartbeat_expectation("11111111-1111-4111-8111-111111111111", "req-2", 2)

    worker = registry.get_worker("11111111-1111-4111-8111-111111111111")
    assert worker["heartbeat_misses"] == 1
    assert worker["expected_heartbeat_request_id"] == "req-2"
    assert worker["expected_heartbeat_sequence"] == 2


def test_master_registry_heartbeat_ack_valid_resets_failure_counter():
    registry = MasterRegistry()
    registry.upsert_worker({
        "node_id": "11111111-1111-4111-8111-111111111111",
        "node_label": "worker-a",
        "group_label": "p2p-cluster",
        "node_role": "worker",
        "host": "127.0.0.1",
        "port": 9101,
    })

    registry.begin_heartbeat_expectation("11111111-1111-4111-8111-111111111111", "req-1", 1)
    assert registry.process_heartbeat_ack("11111111-1111-4111-8111-111111111111", "req-1", 1, "alive") is True
    assert registry.get_worker("11111111-1111-4111-8111-111111111111")["heartbeat_misses"] == 0
    assert registry.get_worker("11111111-1111-4111-8111-111111111111")["status"] == "available"


def test_master_registry_liveness_transitions_from_available_to_suspect_to_unavailable():
    registry = MasterRegistry()
    registry.upsert_worker({
        "node_id": "11111111-1111-4111-8111-111111111111",
        "node_label": "worker-a",
        "group_label": "p2p-cluster",
        "node_role": "worker",
        "host": "127.0.0.1",
        "port": 9101,
    })

    worker = registry.get_worker("11111111-1111-4111-8111-111111111111")
    assert worker["status"] == "available"
    assert worker["heartbeat_misses"] == 0

    registry.begin_heartbeat_expectation("11111111-1111-4111-8111-111111111111", "req-1", 1)
    assert registry.process_heartbeat_ack("11111111-1111-4111-8111-111111111111", "req-other", 1, "alive") is False
    assert registry.get_worker("11111111-1111-4111-8111-111111111111")["status"] == "suspect"
    assert registry.get_worker("11111111-1111-4111-8111-111111111111")["heartbeat_misses"] == 1

    registry.begin_heartbeat_expectation("11111111-1111-4111-8111-111111111111", "req-2", 2)
    assert registry.process_heartbeat_ack("11111111-1111-4111-8111-111111111111", "req-other-2", 2, "alive") is False
    assert registry.get_worker("11111111-1111-4111-8111-111111111111")["status"] == "suspect"
    assert registry.get_worker("11111111-1111-4111-8111-111111111111")["heartbeat_misses"] == 2

    registry.begin_heartbeat_expectation("11111111-1111-4111-8111-111111111111", "req-3", 3)
    assert registry.process_heartbeat_ack("11111111-1111-4111-8111-111111111111", "req-other-3", 3, "alive") is False
    assert registry.get_worker("11111111-1111-4111-8111-111111111111")["status"] == "unavailable"
    assert registry.get_worker("11111111-1111-4111-8111-111111111111")["heartbeat_misses"] == 3


def test_master_registry_valid_heartbeat_restores_available_and_clears_misses():
    registry = MasterRegistry()
    registry.upsert_worker({
        "node_id": "11111111-1111-4111-8111-111111111111",
        "node_label": "worker-a",
        "group_label": "p2p-cluster",
        "node_role": "worker",
        "host": "127.0.0.1",
        "port": 9101,
        "status": "suspect",
        "heartbeat_misses": 2,
    })

    registry.begin_heartbeat_expectation("11111111-1111-4111-8111-111111111111", "req-expected", 7)
    assert registry.process_heartbeat_ack("11111111-1111-4111-8111-111111111111", "req-expected", 7, "alive") is True
    worker = registry.get_worker("11111111-1111-4111-8111-111111111111")
    assert worker["status"] == "available"
    assert worker["heartbeat_misses"] == 0


def test_master_registry_reconnect_validates_and_restores_worker_available():
    registry = MasterRegistry()
    registry.upsert_worker({
        "node_id": "11111111-1111-4111-8111-111111111111",
        "node_label": "worker-a",
        "group_label": "p2p-cluster",
        "node_role": "worker",
        "host": "127.0.0.1",
        "port": 9101,
        "status": "unavailable",
        "heartbeat_misses": 3,
    })

    registry.upsert_worker({
        "node_id": "11111111-1111-4111-8111-111111111111",
        "node_label": "worker-a",
        "group_label": "p2p-cluster",
        "node_role": "worker",
        "host": "127.0.0.1",
        "port": 9101,
        "status": "available",
        "heartbeat_misses": 0,
    })
    restored = registry.get_worker("11111111-1111-4111-8111-111111111111")
    assert restored["status"] == "available"
    assert restored["heartbeat_misses"] == 0


def test_master_registry_worker_a_failure_does_not_change_worker_b_status():
    registry = MasterRegistry()
    registry.upsert_worker({
        "node_id": "11111111-1111-4111-8111-111111111111",
        "node_label": "worker-a",
        "group_label": "p2p-cluster",
        "node_role": "worker",
        "host": "127.0.0.1",
        "port": 9101,
    })
    registry.upsert_worker({
        "node_id": "22222222-2222-4222-8222-222222222222",
        "node_label": "worker-b",
        "group_label": "p2p-cluster",
        "node_role": "worker",
        "host": "127.0.0.1",
        "port": 9102,
    })

    for sequence, request_id in ((1, "req-a-1"), (2, "req-a-2"), (3, "req-a-3")):
        registry.begin_heartbeat_expectation("11111111-1111-4111-8111-111111111111", request_id, sequence)
        registry.process_heartbeat_ack("11111111-1111-4111-8111-111111111111", f"wrong-{request_id}", sequence, "alive")

    assert registry.get_worker("11111111-1111-4111-8111-111111111111")["status"] == "unavailable"
    assert registry.get_worker("22222222-2222-4222-8222-222222222222")["status"] == "available"


def test_master_registry_one_or_two_lost_heartbeats_mark_worker_as_suspect():
    registry = MasterRegistry()
    registry.upsert_worker({
        "node_id": "11111111-1111-4111-8111-111111111111",
        "node_label": "worker-a",
        "group_label": "p2p-cluster",
        "node_role": "worker",
        "host": "127.0.0.1",
        "port": 9101,
    })

    registry.begin_heartbeat_expectation("11111111-1111-4111-8111-111111111111", "req-1", 1)
    assert registry.process_heartbeat_ack("11111111-1111-4111-8111-111111111111", "req-2", 1, "alive") is False
    assert registry.get_worker("11111111-1111-4111-8111-111111111111")["status"] == "suspect"

    registry.begin_heartbeat_expectation("11111111-1111-4111-8111-111111111111", "req-2", 2)
    assert registry.process_heartbeat_ack("11111111-1111-4111-8111-111111111111", "req-2", 3, "alive") is False
    assert registry.get_worker("11111111-1111-4111-8111-111111111111")["status"] == "suspect"
    assert registry.get_worker("11111111-1111-4111-8111-111111111111")["heartbeat_misses"] == 2


def test_master_registry_three_consecutive_lost_heartbeats_marks_worker_unavailable():
    registry = MasterRegistry()
    registry.upsert_worker({
        "node_id": "11111111-1111-4111-8111-111111111111",
        "node_label": "worker-a",
        "group_label": "p2p-cluster",
        "node_role": "worker",
        "host": "127.0.0.1",
        "port": 9101,
    })

    for sequence, request_id in ((1, "req-1"), (2, "req-2"), (3, "req-3")):
        registry.begin_heartbeat_expectation("11111111-1111-4111-8111-111111111111", request_id, sequence)
        assert registry.process_heartbeat_ack("11111111-1111-4111-8111-111111111111", f"wrong-{request_id}", sequence, "alive") is False

    worker = registry.get_worker("11111111-1111-4111-8111-111111111111")
    assert worker["status"] == "unavailable"
    assert worker["heartbeat_misses"] == 3


def test_master_registry_rejects_heartbeat_ack_with_wrong_request_id_or_sequence():
    registry = MasterRegistry()
    registry.upsert_worker({
        "node_id": "11111111-1111-4111-8111-111111111111",
        "node_label": "worker-a",
        "group_label": "p2p-cluster",
        "node_role": "worker",
        "host": "127.0.0.1",
        "port": 9101,
    })

    registry.begin_heartbeat_expectation("11111111-1111-4111-8111-111111111111", "req-expected", 7)
    assert registry.process_heartbeat_ack("11111111-1111-4111-8111-111111111111", "req-other", 7, "alive") is False
    assert registry.get_worker("11111111-1111-4111-8111-111111111111")["heartbeat_misses"] == 1

    registry.begin_heartbeat_expectation("11111111-1111-4111-8111-111111111111", "req-expected-2", 8)
    assert registry.process_heartbeat_ack("11111111-1111-4111-8111-111111111111", "req-expected-2", 9, "alive") is False
    assert registry.get_worker("11111111-1111-4111-8111-111111111111")["heartbeat_misses"] == 2


def test_master_stays_operational_after_worker_is_marked_unavailable():
    registry = MasterRegistry()
    registry.upsert_worker({
        "node_id": "11111111-1111-4111-8111-111111111111",
        "node_label": "worker-a",
        "group_label": "p2p-cluster",
        "node_role": "worker",
        "host": "127.0.0.1",
        "port": 9101,
    })
    registry.upsert_worker({
        "node_id": "22222222-2222-4222-8222-222222222222",
        "node_label": "worker-b",
        "group_label": "p2p-cluster",
        "node_role": "worker",
        "host": "127.0.0.1",
        "port": 9102,
    })

    for sequence, request_id in ((1, "req-1"), (2, "req-2"), (3, "req-3")):
        registry.begin_heartbeat_expectation("11111111-1111-4111-8111-111111111111", request_id, sequence)
        registry.process_heartbeat_ack("11111111-1111-4111-8111-111111111111", f"wrong-{request_id}", sequence, "alive")

    assert registry.get_worker("11111111-1111-4111-8111-111111111111")["status"] == "unavailable"
    assert registry.get_worker("22222222-2222-4222-8222-222222222222")["status"] == "available"
    assert len(registry.workers) == 2


def test_master_heartbeat_loop_can_be_stopped_cleanly():
    registry = MasterRegistry()
    registry.upsert_worker({
        "node_id": "11111111-1111-4111-8111-111111111111",
        "node_label": "worker-a",
        "group_label": "p2p-cluster",
        "node_role": "worker",
        "host": "127.0.0.1",
        "port": 9101,
    })
    server_sock, client_sock = socket.socketpair()
    stop_event = threading.Event()
    thread, stop_event = start_master_heartbeat_loop(server_sock, registry, {
        "group_label": "p2p-cluster",
        "node_id": "22222222-2222-4222-8222-222222222222",
        "node_label": "master",
        "node_role": "master",
    }, {
        "group_label": "p2p-cluster",
        "node_id": "11111111-1111-4111-8111-111111111111",
        "node_label": "worker-a",
        "node_role": "worker",
    }, threading.Lock(), stop_event=stop_event, interval_seconds=0.05)

    stop_event.set()
    thread.join(timeout=1)

    assert not thread.is_alive()
    client_sock.close()


def test_handle_client_closes_session_without_infinite_loop_when_peer_disconnects():
    registry = MasterRegistry()
    master_identity = {
        "group_label": "p2p-cluster",
        "node_id": "22222222-2222-4222-8222-222222222222",
        "node_label": "master",
        "node_role": "master",
    }
    server_sock, client_sock = socket.socketpair()

    def tw():
        handle_client(server_sock, None, registry, master_identity)

    thread = threading.Thread(target=tw, daemon=True)
    thread.start()
    client_sock.close()
    thread.join(timeout=1)

    assert not thread.is_alive()


class FakeReplySession:
    def __init__(self, responses):
        self.responses = list(responses)
        self.sent_messages = []
        self.timeouts = []

    def send_message(self, message):
        self.sent_messages.append(message)

    def wait_for_message(self, *, types=None, request_id=None, timeout=5.0, stop_event=None):
        self.timeouts.append(timeout)
        if not self.responses:
            raise TimeoutError("no matching reply")
        response = self.responses.pop(0)
        if isinstance(response, BaseException):
            raise response
        return response


def test_send_with_retry_success_on_first_attempt():
    message = {
        "version": "sd_2026_2_v1",
        "type": "register_worker",
        "message_id": "msg-try-1",
        "request_id": "req-try-1",
        "timestamp_ms": 1727170000123,
        "source": {"group_label": "p2p-cluster", "node_id": "11111111-1111-4111-8111-111111111111", "node_label": "worker-a", "node_role": "worker"},
        "destination": {"group_label": "p2p-cluster", "node_id": "22222222-2222-4222-8222-222222222222", "node_label": "master", "node_role": "master"},
        "payload": {"home_master": {"group_label": "p2p-cluster", "node_id": "22222222-2222-4222-8222-222222222222", "node_label": "master", "node_role": "master", "host": "127.0.0.1", "port": 9000}, "assignment_epoch": 0, "loan_id": None, "host": "127.0.0.1", "port": 9101},
    }
    ack = {"type": "registration_ack", "request_id": message["request_id"], "payload": {"status": "accepted"}}
    fake = FakeReplySession([ack])

    result = send_with_retry(fake, message, expect_type="registration_ack", timeout=5.0, max_attempts=3)

    assert result is ack
    assert len(fake.sent_messages) == 1
    assert fake.sent_messages[0] is message
    assert fake.timeouts == [5.0]


def test_send_with_retry_retries_once_then_succeeds():
    message = {
        "version": "sd_2026_2_v1",
        "type": "heartbeat",
        "message_id": "msg-heart-1",
        "request_id": "req-heart-1",
        "timestamp_ms": 1727170000124,
        "source": {"group_label": "p2p-cluster", "node_id": "11111111-1111-4111-8111-111111111111", "node_label": "worker-a", "node_role": "worker"},
        "destination": {"group_label": "p2p-cluster", "node_id": "22222222-2222-4222-8222-222222222222", "node_label": "master", "node_role": "master"},
        "payload": {"sequence": 1},
    }
    ack = {"type": "heartbeat_ack", "request_id": message["request_id"], "payload": {"sequence": 1, "status": "alive"}}
    fake = FakeReplySession([TimeoutError("timed out"), ack])

    result = send_with_retry(fake, message, expect_type="heartbeat_ack", timeout=5.0, max_attempts=3)

    assert result is ack
    assert len(fake.sent_messages) == 2
    assert fake.sent_messages[0] is message
    assert fake.sent_messages[1] is message
    assert fake.timeouts == [5.0, 5.0]


def test_send_with_retry_fails_after_three_total_attempts():
    message = {
        "version": "sd_2026_2_v1",
        "type": "register_worker",
        "message_id": "msg-try-3",
        "request_id": "req-try-3",
        "timestamp_ms": 1727170000125,
        "source": {"group_label": "p2p-cluster", "node_id": "11111111-1111-4111-8111-111111111111", "node_label": "worker-a", "node_role": "worker"},
        "destination": {"group_label": "p2p-cluster", "node_id": "22222222-2222-4222-8222-222222222222", "node_label": "master", "node_role": "master"},
        "payload": {"home_master": {"group_label": "p2p-cluster", "node_id": "22222222-2222-4222-8222-222222222222", "node_label": "master", "node_role": "master", "host": "127.0.0.1", "port": 9000}, "assignment_epoch": 0, "loan_id": None, "host": "127.0.0.1", "port": 9101},
    }
    fake = FakeReplySession([TimeoutError("timed out"), TimeoutError("timed out"), TimeoutError("timed out")])

    with pytest.raises(TimeoutError):
        send_with_retry(fake, message, expect_type="registration_ack", timeout=5.0, max_attempts=3)

    assert len(fake.sent_messages) == 3
    assert fake.sent_messages[0] is message
    assert fake.sent_messages[1] is message
    assert fake.sent_messages[2] is message
    assert fake.timeouts == [5.0, 5.0, 5.0]


def test_send_with_retry_preserves_same_message_logical_content_across_attempts():
    message = {
        "version": "sd_2026_2_v1",
        "type": "register_worker",
        "message_id": "msg-preserve-1",
        "request_id": "req-preserve-1",
        "timestamp_ms": 1727170000126,
        "source": {"group_label": "p2p-cluster", "node_id": "11111111-1111-4111-8111-111111111111", "node_label": "worker-a", "node_role": "worker"},
        "destination": {"group_label": "p2p-cluster", "node_id": "22222222-2222-4222-8222-222222222222", "node_label": "master", "node_role": "master"},
        "payload": {"home_master": {"group_label": "p2p-cluster", "node_id": "22222222-2222-4222-8222-222222222222", "node_label": "master", "node_role": "master", "host": "127.0.0.1", "port": 9000}, "assignment_epoch": 0, "loan_id": None, "host": "127.0.0.1", "port": 9101},
    }
    ack = {"type": "registration_ack", "request_id": message["request_id"], "payload": {"status": "accepted"}}
    fake = FakeReplySession([TimeoutError("timed out"), TimeoutError("timed out"), ack])

    result = send_with_retry(fake, message, expect_type="registration_ack", timeout=5.0, max_attempts=3)

    assert result is ack
    assert len(fake.sent_messages) == 3
    for sent in fake.sent_messages:
        assert sent["message_id"] == message["message_id"]
        assert sent["request_id"] == message["request_id"]
        assert sent["type"] == message["type"]
        assert sent["source"] == message["source"]
        assert sent["destination"] == message["destination"]
        assert sent["payload"] == message["payload"]
        assert sent["timestamp_ms"] == message["timestamp_ms"]
    assert fake.sent_messages[0] is fake.sent_messages[1] is fake.sent_messages[2]


def test_send_with_retry_uses_five_second_timeout():
    message = {
        "version": "sd_2026_2_v1",
        "type": "heartbeat",
        "message_id": "msg-heart-timeout",
        "request_id": "req-heart-timeout",
        "timestamp_ms": 1727170000127,
        "source": {"group_label": "p2p-cluster", "node_id": "11111111-1111-4111-8111-111111111111", "node_label": "worker-a", "node_role": "worker"},
        "destination": {"group_label": "p2p-cluster", "node_id": "22222222-2222-4222-8222-222222222222", "node_label": "master", "node_role": "master"},
        "payload": {"sequence": 7},
    }
    ack = {"type": "heartbeat_ack", "request_id": message["request_id"], "payload": {"sequence": 7, "status": "alive"}}
    fake = FakeReplySession([TimeoutError("timed out"), ack])

    result = send_with_retry(fake, message, expect_type="heartbeat_ack", timeout=5.0, max_attempts=3)

    assert result is ack
    assert fake.timeouts == [5.0, 5.0]


def test_send_heartbeat_uses_send_with_retry(monkeypatch):
    config = {
        "group_label": "p2p-cluster",
        "node_id": "77777777-7777-4777-8777-777777777777",
        "node_label": "worker-f",
        "node_role": "worker",
        "host": "127.0.0.1",
        "port": 9106,
    }
    call_log = {}
    ack = {"type": "heartbeat_ack", "request_id": "req-heart-send-with-retry", "payload": {"sequence": 7, "status": "alive"}}

    def fake_send_with_retry(session, message, *, expect_type, timeout, max_attempts):
        call_log["message"] = message
        call_log["expect_type"] = expect_type
        call_log["timeout"] = timeout
        call_log["max_attempts"] = max_attempts
        call_log["session"] = session
        return ack

    monkeypatch.setattr(worker_module, "send_with_retry", fake_send_with_retry)
    session = FakeReplySession([])

    send_heartbeat("127.0.0.1", 9000, config, 7, control_session=session)

    assert call_log["session"] is session
    assert call_log["expect_type"] == "heartbeat_ack"
    assert call_log["timeout"] == 5.0
    assert call_log["max_attempts"] == 3
    assert call_log["message"]["type"] == "heartbeat"
    assert call_log["message"]["request_id"] == session.sent_messages[0]["request_id"] if session.sent_messages else call_log["message"]["request_id"]


def test_master_registry_deduplicates_by_source_request_id_and_type():
    registry = MasterRegistry()
    message = {
        "version": "sd_2026_2_v1",
        "type": "heartbeat",
        "message_id": "msg-dup-1",
        "request_id": "req-dup-1",
        "timestamp_ms": 1727170000123,
        "source": {"group_label": "p2p-cluster", "node_id": "11111111-1111-4111-8111-111111111111", "node_label": "worker-a", "node_role": "worker"},
        "destination": {"group_label": "p2p-cluster", "node_id": "22222222-2222-4222-8222-222222222222", "node_label": "master", "node_role": "master"},
        "payload": {"sequence": 1},
    }
    response = {"type": "heartbeat_ack", "request_id": "req-dup-1", "payload": {"status": "alive"}}

    registry.remember_processed_message(message, response)
    assert registry.get_processed_response(message) is response
    assert registry.get_processed_response({**message, "request_id": "req-dup-2"}) is None
    assert registry.get_processed_response({**message, "source": {**message["source"], "node_id": "22222222-2222-4222-8222-222222222222"}}) is None
    assert registry.get_processed_response({**message, "type": "register_worker"}) is None


def test_handle_client_deduplicates_retransmitted_register_worker_and_reuses_cached_ack():
    registry = MasterRegistry()
    master_identity = {"group_label": "p2p-cluster", "node_id": "22222222-2222-4222-8222-222222222222", "node_label": "master", "node_role": "master"}
    source = {"group_label": "p2p-cluster", "node_id": "11111111-1111-4111-8111-111111111111", "node_label": "worker-a", "node_role": "worker"}
    message = {
        "version": "sd_2026_2_v1",
        "type": "register_worker",
        "message_id": "msg-dup-register-real-1",
        "request_id": "req-dup-register-real-1",
        "timestamp_ms": 1727170000124,
        "source": source,
        "destination": {"group_label": "p2p-cluster", "node_id": master_identity["node_id"], "node_label": master_identity["node_label"], "node_role": master_identity["node_role"]},
        "payload": {"home_master": {"group_label": master_identity["group_label"], "node_id": master_identity["node_id"], "node_label": master_identity["node_label"], "node_role": "master", "host": "127.0.0.1", "port": 9000}, "assignment_epoch": 0, "loan_id": None, "host": "127.0.0.1", "port": 9101},
    }

    server_sock, client_sock = socket.socketpair()
    thread = threading.Thread(target=handle_client, args=(server_sock, None, registry, master_identity), daemon=True)
    thread.start()
    client_sock.settimeout(2)

    send_msg(client_sock, message)
    first_reply = json.loads(client_sock.recv(4096).decode("utf-8").strip())
    assert first_reply["type"] == "registration_ack"
    assert first_reply["payload"]["status"] == "accepted"
    assert len(registry.workers) == 1

    send_msg(client_sock, message)
    second_reply = json.loads(client_sock.recv(4096).decode("utf-8").strip())
    assert second_reply["type"] == "registration_ack"
    assert second_reply == first_reply
    assert len(registry.workers) == 1

    client_sock.close()
    thread.join(timeout=1)


def test_handle_client_deduplicates_retransmitted_heartbeat_and_reuses_cached_ack():
    registry = MasterRegistry()
    master_identity = {"group_label": "p2p-cluster", "node_id": "22222222-2222-4222-8222-222222222222", "node_label": "master", "node_role": "master"}
    source = {"group_label": "p2p-cluster", "node_id": "11111111-1111-4111-8111-111111111111", "node_label": "worker-a", "node_role": "worker"}
    message = {
        "version": "sd_2026_2_v1",
        "type": "heartbeat",
        "message_id": "msg-dup-heart-real-1",
        "request_id": "req-dup-heart-real-1",
        "timestamp_ms": 1727170000125,
        "source": source,
        "destination": {"group_label": "p2p-cluster", "node_id": master_identity["node_id"], "node_label": master_identity["node_label"], "node_role": master_identity["node_role"]},
        "payload": {"sequence": 2, "assignment_epoch": 0},
    }

    server_sock, client_sock = socket.socketpair()
    thread = threading.Thread(target=handle_client, args=(server_sock, None, registry, master_identity), daemon=True)
    thread.start()
    client_sock.settimeout(2)

    send_msg(client_sock, message)
    first_reply = json.loads(client_sock.recv(4096).decode("utf-8").strip())
    assert first_reply["type"] == "heartbeat_ack"
    assert first_reply["request_id"] == message["request_id"]

    send_msg(client_sock, message)
    second_reply = json.loads(client_sock.recv(4096).decode("utf-8").strip())
    assert second_reply["type"] == "heartbeat_ack"
    assert second_reply == first_reply
    assert registry.get_processed_response(message) == first_reply

    client_sock.close()
    thread.join(timeout=1)


def test_worker_compute_retry_delay_grows_and_caps_at_five_seconds():
    assert compute_retry_delay(1) == 0.5
    assert compute_retry_delay(2) == 1.0
    assert compute_retry_delay(3) == 2.0
    assert compute_retry_delay(4) == 4.0
    assert compute_retry_delay(5) == 5.0
    assert compute_retry_delay(6) == 5.0


def test_worker_session_stays_open_when_master_remains_healthy():
    config = {
        "group_label": "p2p-cluster",
        "node_id": "77777777-7777-4777-8777-777777777777",
        "node_label": "worker-f",
        "node_role": "worker",
        "host": "127.0.0.1",
        "port": 9106,
    }
    master_identity = {
        "group_label": "p2p-cluster",
        "node_id": "22222222-2222-4222-8222-222222222222",
        "node_label": "master",
        "node_role": "master",
    }
    listener = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    listener.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    listener.bind(("127.0.0.1", 0))
    listener.listen(5)
    port = listener.getsockname()[1]
    stats = {"connects": 0, "registers": 0}
    stop_server = threading.Event()

    def server_loop():
        try:
            while not stop_server.is_set():
                conn, _ = listener.accept()
                stats["connects"] += 1
                buffer = bytearray()
                while not stop_server.is_set():
                    data = conn.recv(4096)
                    if not data:
                        break
                    buffer.extend(data)
                    messages = extract_complete_messages(buffer)
                    for message in messages:
                        if message["type"] == "register_worker":
                            stats["registers"] += 1
                            send_msg(conn, build_ack_message(message, "accepted", master_identity))
                        elif message["type"] == "heartbeat":
                            send_msg(conn, build_heartbeat_ack_message(message, source=master_identity, destination=message["source"]))
                conn.close()
        except OSError:
            pass

    server_thread = threading.Thread(target=server_loop, daemon=True)
    server_thread.start()

    stop_event = threading.Event()
    manager = WorkerSessionManager("127.0.0.1", port, config, stop_event=stop_event, retry_delay_factory=lambda attempt: 0.05)
    manager.start()

    deadline = time.monotonic() + 5.0
    while time.monotonic() < deadline:
        time.sleep(0.05)

    manager.stop(); stop_server.set(); listener.close(); time.sleep(0.2)

    assert stats["connects"] == 1
    assert stats["registers"] == 1


def test_worker_run_worker_reconnects_after_master_restart():
    config = {
        "group_label": "p2p-cluster",
        "node_id": "11111111-1111-4111-8111-111111111111",
        "node_label": "worker-a",
        "node_role": "worker",
        "host": "127.0.0.1",
        "port": 9101,
    }
    master_identity = {
        "group_label": "p2p-cluster",
        "node_id": "22222222-2222-4222-8222-222222222222",
        "node_label": "master",
        "node_role": "master",
    }
    listener = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    listener.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    listener.bind(("127.0.0.1", 0))
    listener.listen(5)
    port = listener.getsockname()[1]
    attempts = {"count": 0}

    def server_loop():
        while attempts["count"] < 2:
            try:
                conn, _ = listener.accept()
            except OSError:
                break
            attempts["count"] += 1
            buffer = bytearray()
            while True:
                data = conn.recv(4096)
                if not data:
                    break
                buffer.extend(data)
                messages = extract_complete_messages(buffer)
                if messages:
                    send_msg(conn, build_ack_message(messages[0], "accepted", master_identity))
                    break
            conn.close()

    server_thread = threading.Thread(target=server_loop, daemon=True)
    server_thread.start()

    stop_event = threading.Event()
    thread = threading.Thread(target=run_worker, args=("127.0.0.1", port, config, stop_event), daemon=True)
    thread.start()

    deadline = time.monotonic() + 3.0
    while time.monotonic() < deadline:
        if attempts["count"] >= 2:
            break
        time.sleep(0.05)

    stop_event.set()
    thread.join(timeout=2)
    listener.close()

    assert attempts["count"] >= 2
    assert config["node_id"] == "11111111-1111-4111-8111-111111111111"


def test_worker_session_stays_open_for_fragmented_ndjson_and_real_eof_only():
    server_sock, client_sock = socket.socketpair()
    session = WorkerControlSession(client_sock)

    heartbeat = build_heartbeat_message(
        source={
            "group_label": "p2p-cluster",
            "node_id": "11111111-1111-4111-8111-111111111111",
            "node_label": "worker-a",
            "node_role": "worker",
        },
        destination={
            "group_label": "p2p-cluster",
            "node_id": "22222222-2222-4222-8222-222222222222",
            "node_label": "master",
            "node_role": "master",
        },
        sequence=77,
        request_id="req-hb-fragment-77",
    )

    fragment = (json.dumps(heartbeat, separators=(",", ":"), ensure_ascii=False) + "\n").encode("utf-8")[:20]
    server_sock.sendall(fragment)
    time.sleep(0.1)
    assert not session.is_closed()

    server_sock.sendall((json.dumps(heartbeat, separators=(",", ":"), ensure_ascii=False) + "\n").encode("utf-8")[20:])
    ack = build_heartbeat_ack_message(heartbeat, source=heartbeat["destination"], destination=heartbeat["source"])
    server_sock.sendall((json.dumps(ack, separators=(",", ":"), ensure_ascii=False) + "\n").encode("utf-8"))

    response = session.wait_for_message(types={"heartbeat_ack"}, request_id="req-hb-fragment-77", timeout=2)
    assert response["payload"]["status"] == "alive"
    assert not session.is_closed()

    session.close()
    server_sock.close()


def test_worker_session_manager_reuses_same_node_id_after_reconnect():
    config = {
        "group_label": "p2p-cluster",
        "node_id": "11111111-1111-4111-8111-111111111111",
        "node_label": "worker-a",
        "node_role": "worker",
        "host": "127.0.0.1",
        "port": 9101,
    }
    master_identity = {
        "group_label": "p2p-cluster",
        "node_id": "22222222-2222-4222-8222-222222222222",
        "node_label": "master",
        "node_role": "master",
    }
    listener = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    listener.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    listener.bind(("127.0.0.1", 0))
    listener.listen(5)
    port = listener.getsockname()[1]

    def server_loop():
        for _ in range(2):
            try:
                conn, _ = listener.accept()
            except OSError:
                break
            buffer = bytearray()
            while True:
                data = conn.recv(4096)
                if not data:
                    break
                buffer.extend(data)
                if not buffer:
                    break
                messages = extract_complete_messages(buffer)
                if messages:
                    msg = messages[0]
                    send_msg(conn, build_ack_message(msg, "accepted", master_identity))
                    break
            conn.close()

    server_thread = threading.Thread(target=server_loop, daemon=True)
    server_thread.start()

    session_one = open_control_session("127.0.0.1", port, timeout=1.0)
    assert register_worker("127.0.0.1", port, config, control_session=session_one) is True

    session_two = open_control_session("127.0.0.1", port, timeout=1.0)
    assert register_worker("127.0.0.1", port, config, control_session=session_two) is True

    session_one.close(); session_two.close(); listener.close()


def test_worker_reconnect_loop_retries_after_connection_failure():
    config = {
        "group_label": "p2p-cluster",
        "node_id": "33333333-3333-4333-8333-333333333333",
        "node_label": "worker-b",
        "node_role": "worker",
        "host": "127.0.0.1",
        "port": 9102,
    }
    stop_event = threading.Event()
    manager = WorkerSessionManager("127.0.0.1", 1, config, stop_event=stop_event, retry_delay_factory=lambda attempt: 0.01)
    assert manager._connect_and_register() is None
    assert compute_retry_delay(1) == 0.5
    assert compute_retry_delay(5) == 5.0


def test_worker_reconnect_rejected_registration_does_not_resume_session():
    config = {
        "group_label": "p2p-cluster",
        "node_id": "44444444-4444-4444-8444-444444444444",
        "node_label": "worker-c",
        "node_role": "worker",
        "host": "127.0.0.1",
        "port": 9103,
    }
    master_identity = {
        "group_label": "p2p-cluster",
        "node_id": "22222222-2222-4222-8222-222222222222",
        "node_label": "master",
        "node_role": "master",
    }
    listener = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    listener.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    listener.bind(("127.0.0.1", 0))
    listener.listen(5)
    port = listener.getsockname()[1]

    def server_loop():
        try:
            conn, _ = listener.accept()
        except OSError:
            return
        buffer = bytearray()
        while True:
            data = conn.recv(4096)
            if not data:
                break
            buffer.extend(data)
            if not buffer:
                break
            messages = extract_complete_messages(buffer)
            if messages:
                send_msg(conn, build_ack_message(messages[0], "rejected", master_identity, reason="duplicate node_label"))
                break
        conn.close()

    server_thread = threading.Thread(target=server_loop, daemon=True)
    server_thread.start()

    session = open_control_session("127.0.0.1", port, timeout=1.0)
    assert register_worker("127.0.0.1", port, config, control_session=session) is False
    listener.close()


def test_worker_session_manager_stops_when_explicitly_stopped():
    config = {
        "group_label": "p2p-cluster",
        "node_id": "55555555-5555-4555-8555-555555555555",
        "node_label": "worker-d",
        "node_role": "worker",
        "host": "127.0.0.1",
        "port": 9104,
    }
    stop_event = threading.Event()
    stop_event.set()
    manager = WorkerSessionManager("127.0.0.1", 1, config, stop_event=stop_event, retry_delay_factory=lambda _: 0.01)
    assert manager.start() is None


def test_worker_main_does_not_start_fixed_listener(monkeypatch, tmp_path):
    config = {
        "group_label": "p2p-cluster",
        "node_id": "77777777-7777-4777-8777-777777777777",
        "node_label": "worker-f",
        "node_role": "worker",
        "host": "127.0.0.1",
        "port": 0,
        "assignment_epoch": 0,
        "loan_id": None,
        "master": {
            "group_label": "p2p-cluster",
            "node_id": "22222222-2222-4222-8222-222222222222",
            "node_label": "master",
            "node_role": "master",
            "host": "127.0.0.1",
            "port": 9000,
        },
    }
    seen = []

    class DummyThread:
        def __init__(self, target=None, args=(), **kwargs):
            self.target = target
            self.args = args
            self.kwargs = kwargs
            seen.append(getattr(target, "__name__", str(target)))

        def start(self):
            return None

        def join(self, *args, **kwargs):
            return None

    monkeypatch.setattr(worker_module, "load_or_create_config", lambda path: config)
    monkeypatch.setattr(worker_module, "run_worker", lambda *args, **kwargs: seen.append("run_worker") or None)
    monkeypatch.setattr(worker_module.threading, "Thread", DummyThread)
    monkeypatch.setattr(sys, "argv", ["worker.py", "--config", str(tmp_path / "worker-f.json")])

    worker_module.main()

    assert "run_listener" not in seen
    assert "run_worker" in seen


def test_received_log_uses_real_request_id(monkeypatch):
    received = {}
    heartbeat = build_heartbeat_message(
        source={
            "group_label": "p2p-cluster",
            "node_id": "11111111-1111-4111-8111-111111111111",
            "node_label": "worker-a",
            "node_role": "worker",
        },
        destination={
            "group_label": "p2p-cluster",
            "node_id": "22222222-2222-4222-8222-222222222222",
            "node_label": "master",
            "node_role": "master",
        },
        sequence=12,
        request_id="req-real-12",
    )
    ack = build_heartbeat_ack_message(heartbeat, source=heartbeat["destination"], destination=heartbeat["source"])
    server_sock, client_sock = socket.socketpair()
    session = WorkerControlSession(client_sock)
    monkeypatch.setattr(worker_module, "log_event", lambda *args: received.setdefault("args", args))

    try:
        send_msg(server_sock, ack)
        assert session.wait_for_message(types={"heartbeat_ack"}, request_id="req-real-12", timeout=1)["request_id"] == "req-real-12"
        assert received["args"][4] == "req-real-12"
    finally:
        session.close()
        server_sock.close()


def test_worker_heartbeat_resumes_after_reconnect():
    config = {
        "group_label": "p2p-cluster",
        "node_id": "66666666-6666-4666-8666-666666666666",
        "node_label": "worker-e",
        "node_role": "worker",
        "host": "127.0.0.1",
        "port": 9105,
    }
    listener = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    listener.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    listener.bind(("127.0.0.1", 0))
    listener.listen(5)
    port = listener.getsockname()[1]

    def server_loop():
        try:
            conn, _ = listener.accept()
        except OSError:
            return
        buffer = bytearray()
        while True:
            data = conn.recv(4096)
            if not data:
                break
            buffer.extend(data)
            if not buffer:
                break
            messages = extract_complete_messages(buffer)
            if messages:
                send_msg(conn, build_ack_message(messages[0], "accepted", {
                    "group_label": "p2p-cluster",
                    "node_id": "22222222-2222-4222-8222-222222222222",
                    "node_label": "master",
                    "node_role": "master",
                }))
                break
        conn.close()

    server_thread = threading.Thread(target=server_loop, daemon=True)
    server_thread.start()
    session = open_control_session("127.0.0.1", port, timeout=1.0)
    assert register_worker("127.0.0.1", port, config, control_session=session) is True
    session.close()
    listener.close()


def test_worker_send_on_closed_socket_does_not_raise_or_traceback(capsys):
    server_sock, client_sock = socket.socketpair()
    session = WorkerControlSession(client_sock)
    config = {
        "group_label": "p2p-cluster",
        "node_id": "11111111-1111-4111-8111-111111111111",
        "node_label": "worker-a",
        "node_role": "worker",
        "host": "127.0.0.1",
        "port": 9101,
    }
    server_sock.close()

    message = build_heartbeat_message(
        source={
            "group_label": "p2p-cluster",
            "node_id": "11111111-1111-4111-8111-111111111111",
            "node_label": "worker-a",
            "node_role": "worker",
        },
        destination={
            "group_label": "p2p-cluster",
            "node_id": "22222222-2222-4222-8222-222222222222",
            "node_label": "master",
            "node_role": "master",
        },
        sequence=42,
    )

    session.send_message(message)
    assert session.is_closed()
    captured = capsys.readouterr()
    assert "Traceback" not in captured.err


def test_worker_heartbeat_loop_stops_when_session_is_closed():
    server_sock, client_sock = socket.socketpair()
    session = WorkerControlSession(client_sock)
    config = {
        "group_label": "p2p-cluster",
        "node_id": "11111111-1111-4111-8111-111111111111",
        "node_label": "worker-a",
        "node_role": "worker",
        "host": "127.0.0.1",
        "port": 9101,
    }

    thread = start_heartbeat_loop("127.0.0.1", 9000, config, control_session=session, interval_seconds=0.05)
    assert thread is not None

    server_sock.close()
    for _ in range(100):
        if session.is_closed():
            break
        threading.Event().wait(0.02)

    thread.join(timeout=1)
    assert session.is_closed()
    assert not thread.is_alive()


def test_worker_reader_marks_session_closed_on_eof():
    server_sock, client_sock = socket.socketpair()
    session = WorkerControlSession(client_sock)

    server_sock.close()
    for _ in range(100):
        if session.is_closed():
            break
        threading.Event().wait(0.02)

    assert session.is_closed()


def test_worker_close_is_safe_and_idempotent():
    server_sock, client_sock = socket.socketpair()
    session = WorkerControlSession(client_sock)

    session.close()
    session.close()

    assert session.is_closed()
    assert session.sock is client_sock
    server_sock.close()
