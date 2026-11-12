import json
import socket
import sys
import threading
import time
import uuid
from pathlib import Path

import pytest

from collector.collector import export_reports, request_metrics, run_cli
from master.master import (
    MasterTaskQueue,
    PeerLoanManager,
    build_ack_message,
    handle_client,
    load_or_create_config as load_master_config,
    send_heartbeat_to_worker,
    start_master_heartbeat_loop,
)
from protocol import (
    MasterRegistry,
    build_command_redirect_message,
    build_command_release_message,
    build_confirm_loan_message,
    build_get_loan_status_message,
    build_loan_committed_message,
    build_loan_status_message,
    build_notify_worker_returned_message,
    build_request_help_message,
    build_response_accepted_message,
    build_response_rejected_message,
    build_return_ack_message,
    build_heartbeat_ack_message,
    build_heartbeat_message,
    build_metrics_request_message,
    build_metrics_response_message,
    extract_complete_messages,
    recv_msgs,
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


def test_peer_loan_messages_match_protocol_contract():
    requester = {
        "group_label": "p2p-cluster",
        "node_id": "11111111-1111-4111-8111-111111111111",
        "node_label": "master-a",
        "node_role": "master",
    }
    target = {
        "group_label": "p2p-cluster",
        "node_id": "22222222-2222-4222-8222-222222222222",
        "node_label": "master-b",
        "node_role": "master",
    }
    worker = {
        "group_label": "p2p-cluster",
        "node_id": "33333333-3333-4333-8333-333333333333",
        "node_label": "worker-a",
        "node_role": "worker",
    }
    loan_id = "44444444-4444-4444-8444-444444444444"

    request_help = {
        "version": "sd_2026_2_v1",
        "type": "request_help",
        "message_id": str(uuid.uuid4()),
        "request_id": str(uuid.uuid4()),
        "timestamp_ms": 1727170000123,
        "source": requester,
        "destination": target,
        "payload": {
            "requester_master": requester,
            "workers_needed": 2,
            "tasks_pending": 8,
        },
    }
    assert validate_message(request_help) is True

    accepted = build_response_accepted_message(
        request_help,
        source=target,
        destination=requester,
        loan_id=loan_id,
        requester_master=requester,
        target_master=target,
        home_master=requester,
        worker=worker,
        reservation_timeout_ms=15000,
        assignment_epoch=3,
        loan_state="offered",
    )
    assert validate_message(accepted) is True

    committed = build_loan_committed_message(
        accepted,
        source=requester,
        destination=target,
        loan_id=loan_id,
        requester_master=requester,
        target_master=target,
        home_master=requester,
        worker=worker,
        assignment_epoch=3,
        reservation_timeout_ms=15000,
        loan_state="committed",
    )
    assert validate_message(committed) is True

    redirected = build_command_redirect_message(
        committed,
        source=requester,
        destination=target,
        loan_id=loan_id,
        worker=worker,
        target_host="127.0.0.1",
        target_port=9102,
        assignment_epoch=3,
        reason="redirect to peer",
    )
    assert validate_message(redirected) is True

    status = build_loan_status_message(
        committed,
        source=requester,
        destination=target,
        loan_id=loan_id,
        requester_master=requester,
        target_master=target,
        home_master=requester,
        worker=worker,
        assignment_epoch=3,
        loan_state="committed",
    )
    assert validate_message(status) is True

    rejected = build_response_rejected_message(
        request_help,
        source=target,
        destination=requester,
        loan_id=loan_id,
        reason={"code": "capacity_exceeded", "message": "No workers available"},
    )
    assert validate_message(rejected) is True


def test_peer_loan_messages_reject_invalid_payloads_and_bad_states():
    requester = {
        "group_label": "p2p-cluster",
        "node_id": "11111111-1111-4111-8111-111111111111",
        "node_label": "master-a",
        "node_role": "master",
    }
    target = {
        "group_label": "p2p-cluster",
        "node_id": "22222222-2222-4222-8222-222222222222",
        "node_label": "master-b",
        "node_role": "master",
    }
    worker = {
        "group_label": "p2p-cluster",
        "node_id": "33333333-3333-4333-8333-333333333333",
        "node_label": "worker-a",
        "node_role": "worker",
    }

    invalid_state = {
        "version": "sd_2026_2_v1",
        "type": "loan_status",
        "message_id": str(uuid.uuid4()),
        "request_id": str(uuid.uuid4()),
        "timestamp_ms": 1727170000124,
        "source": requester,
        "destination": target,
        "payload": {
            "loan_id": "44444444-4444-4444-8444-444444444444",
            "requester_master": requester,
            "target_master": target,
            "home_master": requester,
            "worker": worker,
            "assignment_epoch": 5,
            "loan_state": "active",
        },
    }
    assert validate_message(invalid_state) is False

    invalid_uuid = {
        "version": "sd_2026_2_v1",
        "type": "response_accepted",
        "message_id": str(uuid.uuid4()),
        "request_id": str(uuid.uuid4()),
        "timestamp_ms": 1727170000125,
        "source": requester,
        "destination": target,
        "payload": {
            "loan_id": "not-a-uuid",
            "requester_master": requester,
            "target_master": target,
            "home_master": requester,
            "worker": worker,
            "assignment_epoch": 1,
            "reservation_timeout_ms": 15000,
            "loan_state": "offered",
        },
    }
    assert validate_message(invalid_uuid) is False

    invalid_epoch = {
        "version": "sd_2026_2_v1",
        "type": "confirm_loan",
        "message_id": str(uuid.uuid4()),
        "request_id": str(uuid.uuid4()),
        "timestamp_ms": 1727170000126,
        "source": requester,
        "destination": target,
        "payload": {
            "loan_id": "44444444-4444-4444-8444-444444444444",
            "requester_master": requester,
            "target_master": target,
            "home_master": requester,
            "worker": worker,
            "assignment_epoch": -1,
            "reservation_timeout_ms": 15000,
            "loan_state": "committed",
        },
    }
    assert validate_message(invalid_epoch) is False

    invalid_target_master = {
        "version": "sd_2026_2_v1",
        "type": "request_help",
        "message_id": str(uuid.uuid4()),
        "request_id": str(uuid.uuid4()),
        "timestamp_ms": 1727170000127,
        "source": requester,
        "destination": worker,
        "payload": {
            "requester_master": requester,
            "workers_needed": 1,
            "tasks_pending": 4,
        },
    }
    assert validate_message(invalid_target_master) is False


def test_peer_loan_manager_reserves_an_idle_worker_and_confirms_it():
    master_identity = {
        "group_label": "p2p-cluster",
        "node_id": "44444444-4444-4444-8444-444444444444",
        "node_label": "master-a",
        "node_role": "master",
        "host": "127.0.0.1",
        "port": 9001,
        "saturation_threshold": 3,
        "release_threshold": 1,
        "stable_samples": 1,
        "monitor_interval_ms": 1000,
    }
    registry = MasterRegistry()
    worker = {
        "group_label": "p2p-cluster",
        "node_id": "55555555-5555-4555-8555-555555555555",
        "node_label": "worker-r1",
        "node_role": "worker",
        "host": "127.0.0.1",
        "port": 9105,
        "status": "available",
    }
    registry.upsert_worker(worker)
    manager = PeerLoanManager(registry, master_identity=master_identity)

    request = {
        "requester_master": master_identity,
        "workers_needed": 1,
        "tasks_pending": 2,
    }
    decision = manager.process_request_help(request, source={
        "group_label": "p2p-cluster",
        "node_id": "66666666-6666-4666-8666-666666666666",
        "node_label": "master-b",
        "node_role": "master",
    })

    assert decision["status"] == "accepted"
    assert decision["loan_state"] == "offered"
    assert manager.get_loan_status(decision["loan_id"])["loan_state"] == "offered"

    committed = manager.confirm_loan(decision["loan_id"], requester_master=request["requester_master"], request_id=str(uuid.uuid4()))
    assert committed["loan_state"] == "committed"
    assert manager.get_loan_status(decision["loan_id"])["loan_state"] == "committed"


def test_peer_loan_manager_redirects_only_committed_loan_to_valid_peer_master(tmp_path):
    master_identity = {
        "group_label": "p2p-cluster",
        "node_id": "77777777-7777-4777-8777-777777777777",
        "node_label": "master-c",
        "node_role": "master",
        "host": "127.0.0.1",
        "port": 9002,
        "min_home_workers": 0,
        "saturation_threshold": 3,
        "release_threshold": 1,
        "stable_samples": 1,
        "monitor_interval_ms": 1000,
    }
    peer_master = {
        "group_label": "p2p-cluster",
        "node_id": "88888888-8888-4888-8888-888888888888",
        "node_label": "master-d",
        "node_role": "master",
        "host": "127.0.0.1",
        "port": 9003,
    }
    registry = MasterRegistry()
    worker = {
        "group_label": "p2p-cluster",
        "node_id": "55555555-5555-4555-8555-555555555555",
        "node_label": "worker-r1",
        "node_role": "worker",
        "host": "127.0.0.1",
        "port": 9105,
        "status": "available",
    }
    registry.upsert_worker(worker)
    manager = PeerLoanManager(registry, master_identity=master_identity, ledger_path=tmp_path / "loan_ledger.json")

    decision = manager.process_request_help({
        "requester_master": peer_master,
        "workers_needed": 1,
        "tasks_pending": 1,
    }, source={
        "group_label": "p2p-cluster",
        "node_id": "66666666-6666-4666-8666-666666666666",
        "node_label": "master-b",
        "node_role": "master",
    })
    assert decision["status"] == "accepted"

    committed = manager.confirm_loan(decision["loan_id"], requester_master=peer_master, request_id=str(uuid.uuid4()))
    assert committed["status"] == "accepted"

    socket_stub = object()
    registry.active_sessions[worker["node_id"]] = socket_stub

    redirect = manager.issue_redirect_command(decision["loan_id"], target_master=peer_master, reason="redirect to peer")
    assert redirect["status"] == "accepted"
    assert redirect["message"]["type"] == "command_redirect"
    assert redirect["message"]["destination"]["node_id"] == peer_master["node_id"]
    assert redirect["message"]["payload"]["loan_id"] == decision["loan_id"]
    assert redirect["message"]["payload"]["home_master"]["node_id"] == master_identity["node_id"]


def test_worker_session_manager_redirects_to_target_master_and_registers(tmp_path):
    home_master = {
        "group_label": "p2p-cluster",
        "node_id": "11111111-1111-4111-8111-111111111111",
        "node_label": "master-a",
        "node_role": "master",
        "host": "127.0.0.1",
        "port": 9101,
    }
    target_master = {
        "group_label": "p2p-cluster",
        "node_id": "22222222-2222-4222-8222-222222222222",
        "node_label": "master-b",
        "node_role": "master",
        "host": "127.0.0.1",
        "port": 0,
    }
    worker_identity = {
        "group_label": "p2p-cluster",
        "node_id": "33333333-3333-4333-8333-333333333333",
        "node_label": "worker-redirect",
        "node_role": "worker",
    }
    config = load_worker_config(tmp_path / "worker-redirect.json")
    config["group_label"] = "p2p-cluster"
    config["node_label"] = "worker-redirect"
    config["master"] = home_master
    config["assignment_epoch"] = 7
    config["loan_id"] = "44444444-4444-4444-8444-444444444444"

    server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    server.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    server.bind(("127.0.0.1", 0))
    server.listen(1)
    target_master["port"] = server.getsockname()[1]
    seen = {}

    def accept_registration():
        conn, _ = server.accept()
        try:
            buffer = bytearray()
            messages = recv_msgs(conn, buffer)
            assert messages and messages[0]["type"] == "register_worker"
            seen["destination_id"] = messages[0]["destination"]["node_id"]
            seen["accepted"] = True
            ack = {
                "version": "sd_2026_2_v1",
                "type": "registration_ack",
                "message_id": str(uuid.uuid4()),
                "request_id": messages[0]["request_id"],
                "timestamp_ms": int(time.time() * 1000),
                "source": target_master,
                "destination": worker_identity,
                "payload": {"accepted": True, "reason": None, "assignment_epoch": 7},
            }
            send_msg(conn, ack)
        finally:
            conn.close()
            server.close()

    threading.Thread(target=accept_registration, daemon=True).start()

    manager = WorkerSessionManager(home_master["host"], home_master["port"], config, stop_event=threading.Event())
    redirect = build_command_redirect_message(
        {"request_id": str(uuid.uuid4()), "source": home_master, "destination": target_master},
        source=home_master,
        destination=target_master,
        loan_id=config["loan_id"],
        worker=worker_identity,
        target_host=target_master["host"],
        target_port=target_master["port"],
        assignment_epoch=7,
        reason="redirect to peer",
        target_master=target_master,
        home_master=home_master,
        home=home_master,
        request_id=str(uuid.uuid4()),
    )

    assert manager.process_inbound_message(redirect) is True
    assert manager.master_host == target_master["host"]
    assert manager.master_port == target_master["port"]
    assert seen["destination_id"] == target_master["node_id"]
    assert seen["accepted"] is True


def test_peer_loan_manager_processes_worker_return_and_closes_loan():
    master_identity = {
        "group_label": "p2p-cluster",
        "node_id": "77777777-7777-4777-8777-777777777777",
        "node_label": "master-c",
        "node_role": "master",
        "host": "127.0.0.1",
        "port": 9002,
    }
    registry = MasterRegistry()
    worker = {
        "group_label": "p2p-cluster",
        "node_id": "55555555-5555-4555-8555-555555555555",
        "node_label": "worker-r1",
        "node_role": "worker",
        "host": "127.0.0.1",
        "port": 9105,
        "status": "available",
    }
    registry.upsert_worker(worker)
    manager = PeerLoanManager(registry, master_identity=master_identity)

    requester_master = {**master_identity, "port": 9005}
    decision = manager.process_request_help({"requester_master": requester_master, "workers_needed": 1, "tasks_pending": 1}, source={"group_label": "p2p-cluster", "node_id": "66666666-6666-4666-8666-666666666666", "node_label": "master-b", "node_role": "master"})
    assert decision["status"] == "accepted"
    assert decision["requester_master"]["node_id"] == requester_master["node_id"]

    committed = manager.confirm_loan(decision["loan_id"], requester_master=decision["requester_master"], request_id=str(uuid.uuid4()))
    assert committed["status"] == "accepted"
    assert committed["requester_master"]["node_id"] == requester_master["node_id"]

    notify = manager.process_notify_worker_returned(decision["loan_id"], worker=worker, assignment_epoch=0)
    assert notify["status"] == "accepted"
    assert manager.get_loan_status(decision["loan_id"])["loan_state"] == "closed"

    ack = manager.process_return_ack(decision["loan_id"], worker=worker, assignment_epoch=0, status="returned")
    assert ack["status"] == "accepted"
    assert manager.get_loan_status(decision["loan_id"])["loan_state"] == "closed"


def test_worker_manager_processes_command_release_and_returns_to_home_master(tmp_path):
    home_master = {
        "group_label": "p2p-cluster",
        "node_id": "11111111-1111-4111-8111-111111111111",
        "node_label": "master-a",
        "node_role": "master",
        "host": "127.0.0.1",
        "port": 9001,
    }
    target_master = {
        "group_label": "p2p-cluster",
        "node_id": "22222222-2222-4222-8222-222222222222",
        "node_label": "master-b",
        "node_role": "master",
        "host": "127.0.0.1",
        "port": 9002,
    }
    config = load_worker_config(tmp_path / "worker-return.json")
    config["group_label"] = "p2p-cluster"
    config["node_label"] = "worker-return"
    config["home_master"] = home_master
    config["master"] = target_master
    config["assignment_epoch"] = 3
    config["loan_id"] = "44444444-4444-4444-8444-444444444444"

    manager = WorkerSessionManager(target_master["host"], target_master["port"], config, stop_event=threading.Event())
    command = build_command_release_message(
        {"request_id": str(uuid.uuid4())},
        source=target_master,
        destination={
            "group_label": config["group_label"],
            "node_id": config["node_id"],
            "node_label": config["node_label"],
            "node_role": "worker",
        },
        loan_id=config["loan_id"],
        assignment_epoch=3,
        reason="return to home",
        request_id=str(uuid.uuid4()),
    )

    assert manager.process_inbound_message(command) is True
    assert manager.master_host == home_master["host"]
    assert manager.master_port == home_master["port"]
    assert config["home_master"]["node_id"] == home_master["node_id"]


def test_peer_loan_manager_rejects_when_local_capacity_is_too_low():
    master_identity = {
        "group_label": "p2p-cluster",
        "node_id": "77777777-7777-4777-8777-777777777777",
        "node_label": "master-c",
        "node_role": "master",
        "host": "127.0.0.1",
        "port": 9002,
        "saturation_threshold": 3,
        "release_threshold": 1,
        "stable_samples": 1,
        "monitor_interval_ms": 1000,
    }
    registry = MasterRegistry()
    for idx in range(1, 3):
        registry.upsert_worker({
            "group_label": "p2p-cluster",
            "node_id": f"{idx:02d}000000-0000-4000-8000-000000000000",
            "node_label": f"worker-{idx}",
            "node_role": "worker",
            "host": "127.0.0.1",
            "port": 9100 + idx,
            "status": "available",
        })
    manager = PeerLoanManager(registry, master_identity=master_identity)
    decision = manager.process_request_help({
        "requester_master": master_identity,
        "workers_needed": 1,
        "tasks_pending": 5,
    }, source={
        "group_label": "p2p-cluster",
        "node_id": "88888888-8888-4888-8888-888888888888",
        "node_label": "master-d",
        "node_role": "master",
    })

    assert decision["status"] == "rejected"
    assert decision["reason"]["code"] in {"high_load", "min_home_workers", "no_workers_available"}


def test_valid_task_submit_message_is_accepted_by_protocol():
    message = {
        "version": "sd_2026_2_v1",
        "type": "task_submit",
        "message_id": "11111111-1111-4111-8111-111111111111",
        "request_id": "22222222-2222-4222-8222-222222222222",
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
        "message_id": "c8beebf1-fe69-4b64-856a-f63b0bf60da4",
        "request_id": "9924b97d-d844-4a6b-abdc-598e1e753aa8",
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
        "message_id": str(uuid.uuid4()),
        "request_id": str(uuid.uuid4()),
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
        "message_id": str(uuid.uuid4()),
        "request_id": str(uuid.uuid4()),
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
        "message_id": str(uuid.uuid4()),
        "request_id": str(uuid.uuid4()),
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
        "message_id": str(uuid.uuid4()),
        "request_id": str(uuid.uuid4()),
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
        "message_id": str(uuid.uuid4()),
        "request_id": str(uuid.uuid4()),
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
        request_id=str(uuid.uuid4()),
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
        "message_id": "1aab0392-39ee-48dc-b6ec-263ef279ae64",
        "request_id": "1f9ae44f-31d6-4574-86e5-b52e089593c9",
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
        "message_id": "f298b5ca-c846-45b3-9256-737ba18f5ffa",
        "request_id": "dda62732-3f97-47b1-9a30-e18edfcd2a9b",
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


def test_master_parallel_submissions_keep_unique_ids_and_fifo_registration():
    from master.master import MasterTaskQueue

    queue = MasterTaskQueue()
    barrier = threading.Barrier(10)
    decisions = []

    def submitter(index):
        barrier.wait()
        decision = queue.submit({
            "task_id": f"parallel-registration-{index}",
            "task_kind": "sum_range",
            "parameters": {"start": index, "end": index + 10},
        })
        decisions.append(decision)

    threads = [threading.Thread(target=submitter, args=(i,)) for i in range(10)]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join()

    assert len(decisions) == 10
    assert len({item["task_id"] for item in decisions}) == 10
    assert len({item["task_label"] for item in decisions}) == 10
    assert all(item["accepted"] for item in decisions)
    assert len(queue.pending()) == 10


def test_load_monitor_counts_only_pending_tasks_for_saturation():
    from master.master import LoadMonitor, MasterTaskQueue

    queue = MasterTaskQueue()
    for index in range(13):
        queue.submit({"task_id": f"load-pending-{index}", "task_kind": "sum_range", "parameters": {"start": 1, "end": 10}})

    for index in range(3):
        queue.claim_next_task(f"worker-{index}", assignment_epoch=1, loan_id=None)

    monitor = LoadMonitor(queue, saturation_threshold=10, release_threshold=3, stable_samples=3, monitor_interval_ms=1000)
    for _ in range(3):
        monitor.sample()

    assert monitor.pending_tasks == 10
    assert monitor.state == "normal"
    assert monitor.consecutive_above == 0


def test_master_maintains_single_active_attempt_under_timeout_disconnect_and_result_race():
    from master.master import MasterTaskQueue

    queue = MasterTaskQueue(task_timeout_ms=30000, max_task_attempts=3)
    queue.submit({"task_id": "task-race-1", "task_kind": "sum_range", "parameters": {"start": 1, "end": 12}})

    assignment = queue.claim_next_task("worker-a", assignment_epoch=1, loan_id=None)
    assert assignment is not None
    attempt_id = assignment["attempt_id"]

    barrier = threading.Barrier(3)
    outcomes = []

    def mark_timeout():
        barrier.wait()
        outcomes.append(("timeout", queue.mark_task_timed_out("task-race-1", reason="timeout")))

    def mark_disconnect():
        barrier.wait()
        outcomes.append(("disconnect", queue.handle_worker_unavailable("worker-a", reason="socket closed")))

    def complete_result():
        barrier.wait()
        outcomes.append(("result", queue.complete_task("task-race-1", attempt_id, accepted=True, reason="completed", payload={"result": {"sum": 78}})))

    threads = [
        threading.Thread(target=mark_timeout),
        threading.Thread(target=mark_disconnect),
        threading.Thread(target=complete_result),
    ]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join()

    current = queue.get_task("task-race-1")
    assert current["status"] in {"pending", "failed", "completed"}
    assert current["attempt_count"] == 1
    assert current["last_attempt_id"] == attempt_id
    assert current["assigned_to"] in {None, "worker-a"} or current["assigned_to"] is None
    result_outcomes = [item for item in outcomes if item[0] == "result"]
    assert len(result_outcomes) == 1
    accepted_result = result_outcomes[0][1]
    assert accepted_result[0] in {True, False}


def test_master_load_monitor_detects_saturation_after_three_stable_samples():
    from master.master import LoadMonitor, MasterTaskQueue

    queue = MasterTaskQueue()
    for index in range(11):
        queue.submit({"task_id": f"load-sat-{index}", "task_kind": "sum_range", "parameters": {"start": 1, "end": 10}})

    monitor = LoadMonitor(queue, saturation_threshold=10, release_threshold=3, stable_samples=3, monitor_interval_ms=1000)
    for _ in range(3):
        monitor.sample()

    assert monitor.state == "saturated"
    assert monitor.consecutive_above == 3
    assert monitor.pending_tasks == 11


def test_master_load_monitor_releases_after_three_stable_low_samples_and_resets_on_break():
    from master.master import LoadMonitor, MasterTaskQueue

    queue = MasterTaskQueue()
    for index in range(11):
        queue.submit({"task_id": f"load-release-{index}", "task_kind": "sum_range", "parameters": {"start": 1, "end": 10}})

    monitor = LoadMonitor(queue, saturation_threshold=10, release_threshold=3, stable_samples=3, monitor_interval_ms=1000)
    for _ in range(3):
        monitor.sample()
    assert monitor.state == "saturated"

    queue._tasks = {}
    queue._order = []
    for index in range(3):
        queue.submit({"task_id": f"load-release-low-{index}", "task_kind": "sum_range", "parameters": {"start": 1, "end": 10}})

    for _ in range(3):
        monitor.sample()

    assert monitor.state == "released"
    assert monitor.consecutive_below == 3

    queue.submit({"task_id": "load-release-bridge", "task_kind": "sum_range", "parameters": {"start": 1, "end": 10}})
    monitor.sample()
    assert monitor.consecutive_below == 0


def test_master_load_monitor_validates_threshold_config():
    from master.master import LoadMonitor, MasterTaskQueue

    queue = MasterTaskQueue()
    with pytest.raises(ValueError):
        LoadMonitor(queue, saturation_threshold=3, release_threshold=3, stable_samples=3, monitor_interval_ms=1000)
    with pytest.raises(ValueError):
        LoadMonitor(queue, saturation_threshold=10, release_threshold=10, stable_samples=3, monitor_interval_ms=1000)
    with pytest.raises(ValueError):
        LoadMonitor(queue, saturation_threshold=10, release_threshold=3, stable_samples=0, monitor_interval_ms=1000)


def test_client_load_generator_is_deterministic_and_unique():
    from client.load_generator import build_load_plan

    plan_a = build_load_plan(task_count=10, start=1, end=1000, interval_ms=20, seed=7)
    plan_b = build_load_plan(task_count=10, start=1, end=1000, interval_ms=20, seed=7)
    assert plan_a == plan_b
    assert len(plan_a) == 10
    assert len({item["task_id"] for item in plan_a}) == 10
    assert all(item["parameters"]["start"] == 1 for item in plan_a)
    assert all(item["parameters"]["end"] == 1000 for item in plan_a)


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
        request_id="a270a527-a7d7-40bd-b3bf-d8b22f62a2da",
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
                "message_id": "384db959-1d4b-473c-8c02-ff6904c34687",
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
        "message_id": "b80d1fe6-ea47-46d2-8474-cfc83b5284bd",
        "request_id": "829bc052-5e97-4432-bd7b-f61dccf1e11e",
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
        "message_id": "b0162e6a-8334-45c2-a90a-e21b8532f908",
        "request_id": "a47ca1c8-a881-495a-a098-a5da7640f7fc",
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


def test_worker_keeps_sending_heartbeats_while_running_sum_range(monkeypatch):
    master_identity = {
        "group_label": "p2p-cluster",
        "node_id": "22222222-2222-4222-8222-222222222222",
        "node_label": "master",
        "node_role": "master",
        "host": "127.0.0.1",
        "port": 9000,
    }
    config = {
        "group_label": "p2p-cluster",
        "node_id": "11111111-1111-4111-8111-111111111111",
        "node_label": "worker-a",
        "node_role": "worker",
        "host": "127.0.0.1",
        "port": 9101,
        "assignment_epoch": 0,
        "loan_id": None,
        "master": {**master_identity},
    }
    queue = MasterTaskQueue()
    queue.submit({"task_id": "task-heartbeat-busy", "task_kind": "sum_range", "parameters": {"start": 1, "end": 5}})

    registry = MasterRegistry()
    server_sock, client_sock = socket.socketpair()
    client_sock.settimeout(2.0)
    thread = threading.Thread(target=handle_client, args=(server_sock, ("127.0.0.1", 9000), registry, master_identity, queue), daemon=True)
    thread.start()
    session = WorkerControlSession(client_sock)
    heartbeat_received = threading.Event()
    original_send_with_retry = worker_module.send_with_retry

    def tracking_send_with_retry(session_obj, message, *, expect_type, timeout=5.0, max_attempts=3):
        reply = original_send_with_retry(session_obj, message, expect_type=expect_type, timeout=timeout, max_attempts=max_attempts)
        if isinstance(message, dict) and message.get("type") == "heartbeat":
            heartbeat_received.set()
        return reply

    original_start_heartbeat_loop = worker_module.start_heartbeat_loop

    def fast_heartbeat_loop(*args, **kwargs):
        kwargs["interval_seconds"] = 0.05
        return original_start_heartbeat_loop(*args, **kwargs)

    real_sum_range = worker_module.sum_range
    slow_started = threading.Event()

    def slow_sum_range(start: int, end: int) -> int:
        slow_started.set()
        assert heartbeat_received.wait(timeout=1.0), "heartbeat was not acknowledged before task completion"
        return real_sum_range(start, end)

    monkeypatch.setattr(worker_module, "send_with_retry", tracking_send_with_retry)
    monkeypatch.setattr(worker_module, "start_heartbeat_loop", fast_heartbeat_loop)
    monkeypatch.setattr(worker_module, "sum_range", slow_sum_range)

    try:
        assert register_worker("127.0.0.1", 9000, config, control_session=session, stop_event=None) is True

        manager = WorkerSessionManager("127.0.0.1", 9000, config)
        manager._set_session(session)
        manager._start_heartbeat_for_session(session)

        assert manager.request_task(session) is True
        assignment = session.wait_for_message(types={"task_assignment"}, timeout=2.0)
        assert assignment["type"] == "task_assignment"
        assert assignment["payload"]["task_id"] == "task-heartbeat-busy"
        assert manager.busy is False

        manager.process_inbound_message(assignment)
        assert manager.busy is True
        assert manager.current_task is not None
        assert slow_started.is_set()
        assert heartbeat_received.wait(timeout=2.0), "at least one heartbeat should be acknowledged before the task completes"

        task_ack = session.wait_for_message(types={"task_ack"}, timeout=2.0)
        assert task_ack["type"] == "task_ack"
        assert task_ack["payload"]["accepted"] is True
        manager.process_inbound_message(task_ack)
        assert manager.busy is False
        assert manager.current_task is None

        completed = queue.get_task("task-heartbeat-busy")
        assert completed["status"] == "completed"
        assert completed["result_payload"]["status"] == "ok"
        assert completed["result_payload"]["result"]["sum"] == 15
    finally:
        session.close()
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
        "message_id": "4de2961f-2225-44d4-80fd-a930b7d676b8",
        "request_id": "1e7ecbdd-0a11-4d0f-b501-8e59aaab36a6",
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
        "message_id": "2514bdad-7518-492b-bac6-105465b8072e",
        "request_id": "5c60a982-75a3-4130-840a-d7150c9999a6",
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
        "message_id": "063c325d-02fa-438a-b440-e870197a9369",
        "request_id": "c8222893-f688-4e45-be17-84eedfb3cc78",
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
    assert reply["payload"]["error_code"] in {
        "invalid_message",
        "invalid_state",
        "stale_epoch",
        "reservation_expired",
        "identity_conflict",
        "request_conflict",
        "unsupported_version",
        "unsupported_type",
    }
    assert isinstance(reply["payload"]["error_message"], str)
    assert reply["payload"]["error_message"].strip()
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



def test_ct_02_reconnect_with_cached_ack_closes_old_socket():
    import uuid

    registry = MasterRegistry()
    master = {
        "group_label": "p2p-cluster",
        "node_id": "22222222-2222-4222-8222-222222222222",
        "node_label": "master",
        "node_role": "master",
    }
    worker = {
        "group_label": "p2p-cluster",
        "node_id": "11111111-1111-4111-8111-111111111111",
        "node_label": "worker-a",
        "node_role": "worker",
    }
    message = {
        "version": "sd_2026_2_v1",
        "type": "register_worker",
        "message_id": str(uuid.uuid4()),
        "request_id": str(uuid.uuid4()),
        "timestamp_ms": 1727170000124,
        "source": worker,
        "destination": master,
        "payload": {
            "home_master": {**master, "host": "127.0.0.1", "port": 9000},
            "assignment_epoch": 0,
            "loan_id": None,
            "host": "127.0.0.1",
            "port": 9101,
        },
    }

    s1, c1 = socket.socketpair()
    s2, c2 = socket.socketpair()
    threads = []

    try:
        for server in (s1, s2):
            thread = threading.Thread(
                target=handle_client,
                args=(server, None, registry, master),
                daemon=True,
            )
            thread.start()
            threads.append(thread)

        c1.settimeout(2)
        c2.settimeout(2)

        send_msg(c1, message)
        first = recv_msgs(c1, bytearray())
        assert first and first[0]["type"] == "registration_ack"

        registry.mark_worker_unavailable(worker["node_id"], "connection_lost")
        assert registry.get_worker(worker["node_id"])["status"] == "unavailable"

        send_msg(c2, message)
        second = recv_msgs(c2, bytearray())
        assert second and second[0] == first[0]

        assert registry.active_sessions[worker["node_id"]] is s2

        threads[0].join(timeout=2)
        assert not threads[0].is_alive()
        assert registry.get_worker(worker["node_id"])["status"] == "available"
    finally:
        for sock in (c1, c2, s1, s2):
            try:
                sock.close()
            except OSError:
                pass
        for thread in threads:
            thread.join(timeout=2)


def test_ct_02_request_conflict_over_tcp():
    import uuid

    registry = MasterRegistry()
    master = {
        "group_label": "p2p-cluster",
        "node_id": "22222222-2222-4222-8222-222222222222",
        "node_label": "master",
        "node_role": "master",
    }
    worker = {
        "group_label": "p2p-cluster",
        "node_id": "11111111-1111-4111-8111-111111111111",
        "node_label": "worker-a",
        "node_role": "worker",
    }
    message = {
        "version": "sd_2026_2_v1",
        "type": "register_worker",
        "message_id": str(uuid.uuid4()),
        "request_id": str(uuid.uuid4()),
        "timestamp_ms": 1727170000124,
        "source": worker,
        "destination": master,
        "payload": {
            "home_master": {**master, "host": "127.0.0.1", "port": 9000},
            "assignment_epoch": 0,
            "loan_id": None,
            "host": "127.0.0.1",
            "port": 9101,
        },
    }

    server, client = socket.socketpair()
    thread = threading.Thread(
        target=handle_client,
        args=(server, None, registry, master),
        daemon=True,
    )
    thread.start()

    try:
        client.settimeout(2)
        buffer = bytearray()

        send_msg(client, message)
        first = recv_msgs(client, buffer)
        assert first and first[0]["type"] == "registration_ack"

        conflicting = {
            **message,
            "message_id": str(uuid.uuid4()),
            "payload": {**message["payload"], "port": 9999},
        }

        send_msg(client, conflicting)
        second = recv_msgs(client, buffer)

        assert second and second[0]["type"] == "protocol_error"
        assert second[0]["payload"]["error_code"] == "request_conflict"
    finally:
        client.close()
        thread.join(timeout=2)
        server.close()


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
            "message_id": "b0162e6a-8334-45c2-a90a-e21b8532f908",
            "request_id": "a47ca1c8-a881-495a-a098-a5da7640f7fc",
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
            "message_id": "1950c83a-b9f2-4da1-ae3e-a60313aebba2",
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
            "message_id": "08fd627f-a80c-49be-9565-906e9d8242c8",
            "request_id": "0f8421ab-9ab1-4863-89ac-22525bb78998",
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
            "message_id": "b000cc11-3de7-4e8b-a076-256d48045285",
            "request_id": "0f8421ab-9ab1-4863-89ac-22525bb78998",
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
        with session._message_condition:
            session._pending_messages.append(assignment)
            session._message_condition.notify_all()
        time.sleep(0.25)
        assert manager.busy is True

        with session._message_condition:
            session._pending_messages.append(task_ack)
            session._message_condition.notify_all()
        time.sleep(0.30)

        assert manager.busy is False
        assert manager.current_task is None
        manager.stop_event.set()
    finally:
        manager.stop_event.set()
        server_sock.close()
        client_sock.close()


def test_worker_clears_task_request_pending_on_no_task_and_ack():
    from worker.worker import WorkerControlSession, WorkerSessionManager

    config = {
        "group_label": "p2p-cluster",
        "node_id": "11111111-1111-4111-8111-111111111111",
        "node_label": "worker-pending-cleanup",
        "node_role": "worker",
        "host": "127.0.0.1",
        "port": 9104,
        "assignment_epoch": 4,
        "loan_id": None,
    }
    manager = WorkerSessionManager("127.0.0.1", 9000, config)
    server_sock, client_sock = socket.socketpair()
    try:
        session = WorkerControlSession(client_sock)
        manager._set_session(session)
        manager.task_request_pending = True

        no_task = {
            "version": "sd_2026_2_v1",
            "type": "no_task",
            "message_id": "fa494687-0cff-4d8b-bbe7-2699296bf3f8",
            "request_id": "5239299c-ab19-46a9-92c9-38c360f1e529",
            "timestamp_ms": 1727170000500,
            "source": {
                "group_label": "p2p-cluster",
                "node_id": "22222222-2222-4222-8222-222222222222",
                "node_label": "master",
                "node_role": "master",
            },
            "destination": {
                "group_label": "p2p-cluster",
                "node_id": "11111111-1111-4111-8111-111111111111",
                "node_label": "worker-pending-cleanup",
                "node_role": "worker",
            },
            "payload": {"retry_after_ms": 250},
        }
        assert manager.process_inbound_message(no_task) is True
        assert manager.task_request_pending is False
        assert manager.next_task_ready_at > time.monotonic()

        manager.task_request_pending = True
        manager.current_task = {
            "task_id": "task-pending-ack",
            "task_label": "task-pending-ack-label",
            "attempt_id": "attempt-pending-ack",
        }
        ack = {
            "version": "sd_2026_2_v1",
            "type": "task_ack",
            "message_id": "29e57aad-9481-41c0-a0f4-ab353d2d06cb",
            "request_id": "91712a7b-2542-48df-9267-5981b452d7da",
            "timestamp_ms": 1727170000510,
            "source": no_task["source"],
            "destination": no_task["destination"],
            "payload": {
                "task_id": "task-pending-ack",
                "attempt_id": "attempt-pending-ack",
                "accepted": True,
                "reason": "completed",
            },
        }
        assert manager.process_task_ack(ack) is True
        assert manager.task_request_pending is False
    finally:
        manager.stop_event.set()
        server_sock.close()
        client_sock.close()


def test_master_does_not_assign_multiple_running_tasks_to_same_worker():
    from master.master import MasterTaskQueue

    queue = MasterTaskQueue(task_timeout_ms=30000, max_task_attempts=3)
    queue.submit({"task_id": "task-assign-1", "task_kind": "sum_range", "parameters": {"start": 1, "end": 5}})
    queue.submit({"task_id": "task-assign-2", "task_kind": "sum_range", "parameters": {"start": 6, "end": 10}})

    first_assignment = queue.claim_next_task("worker-a", assignment_epoch=1, loan_id=None)
    assert first_assignment is not None
    assert first_assignment["task_id"] == "task-assign-1"

    second_assignment = queue.claim_next_task("worker-a", assignment_epoch=1, loan_id=None)
    assert second_assignment is None

    task_one = queue.get_task("task-assign-1")
    assert task_one["status"] == "running"
    assert task_one["assigned_to"] == "worker-a"


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
            "message_id": "928dec5d-c0b2-417c-af01-ded315271690",
            "request_id": "5258310e-e065-4ec6-a8de-95a12711fef6",
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
            "message_id": "4dbbd8ad-faeb-4c00-98a2-f28d8994a13f",
            "request_id": "4ded63bf-42a5-45d6-85f9-4dbd72e27b5d",
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
            "message_id": "f41cb385-a867-4dde-ba16-5bc94308819f",
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
            "message_id": "5456fd95-de8e-4a18-a365-6bdf9a7d63bc",
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


def test_worker_retransmits_pending_result_without_recalculating():
    from worker.worker import WorkerControlSession, WorkerSessionManager

    config = {
        "group_label": "p2p-cluster",
        "node_id": "11111111-1111-4111-8111-111111111111",
        "node_label": "worker-retry",
        "node_role": "worker",
        "host": "127.0.0.1",
        "port": 9103,
        "assignment_epoch": 9,
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
            "message_id": "8e8cf42e-a3b0-45de-85c8-d627dfd270b5",
            "request_id": "42fe9737-501f-470a-8797-19ae558c0bcb",
            "timestamp_ms": 1727170000400,
            "source": {
                "group_label": "p2p-cluster",
                "node_id": "22222222-2222-4222-8222-222222222222",
                "node_label": "master",
                "node_role": "master",
            },
            "destination": {
                "group_label": "p2p-cluster",
                "node_id": "11111111-1111-4111-8111-111111111111",
                "node_label": "worker-retry",
                "node_role": "worker",
            },
            "payload": {
                "task_id": "task-040",
                "task_label": "task-0008",
                "attempt_id": "attempt-retry-1",
                "task_kind": "sum_range",
                "parameters": {"start": 1, "end": 10},
                "assignment_epoch": 9,
                "loan_id": None,
            },
        }

        sent = manager.process_inbound_message(assignment)
        assert sent is not None
        assert sent["payload"]["result"] == {"sum": 55}
        assert len(manager.pending_results) == 1

        raw = server_sock.recv(4096)
        first = json.loads(raw.decode("utf-8").strip())
        assert first["payload"]["task_id"] == "task-040"

        assert manager.retransmit_pending_results(session) == 0

        pending = next(iter(manager.pending_results.values()))
        pending["last_sent_at"] -= 5.0

        resent = manager.retransmit_pending_results(session)
        assert resent == 1
        raw_retry = server_sock.recv(4096)
        retry = json.loads(raw_retry.decode("utf-8").strip())
        assert retry["payload"]["attempt_id"] == "attempt-retry-1"
        assert retry["payload"]["result"] == {"sum": 55}

        ack = {
            "version": "sd_2026_2_v1",
            "type": "task_ack",
            "message_id": "b1881628-06ab-4727-b9d6-a568da3841b7",
            "request_id": sent["request_id"],
            "timestamp_ms": 1727170000410,
            "source": assignment["source"],
            "destination": assignment["destination"],
            "payload": {
                "task_id": "task-040",
                "attempt_id": "attempt-retry-1",
                "accepted": True,
                "reason": "completed",
            },
        }
        assert manager.process_task_ack(ack) is True
        assert len(manager.pending_results) == 0
    finally:
        server_sock.close()
        client_sock.close()


def test_worker_accepts_positive_duplicate_ack_for_retransmitted_result():
    from worker.worker import WorkerControlSession, WorkerSessionManager

    config = {
        "group_label": "p2p-cluster",
        "node_id": "11111111-1111-4111-8111-111111111111",
        "node_label": "worker-duplicate-ack",
        "node_role": "worker",
        "host": "127.0.0.1",
        "port": 9106,
        "assignment_epoch": 12,
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
            "message_id": "06600f6f-f439-4a2d-bd76-a9ab488be5d5",
            "request_id": "16af4887-c352-4bb9-b8cf-46c4e8226642",
            "timestamp_ms": 1727170000700,
            "source": {
                "group_label": "p2p-cluster",
                "node_id": "22222222-2222-4222-8222-222222222222",
                "node_label": "master",
                "node_role": "master",
            },
            "destination": {
                "group_label": "p2p-cluster",
                "node_id": "11111111-1111-4111-8111-111111111111",
                "node_label": "worker-duplicate-ack",
                "node_role": "worker",
            },
            "payload": {
                "task_id": "task-043",
                "task_label": "task-0011",
                "attempt_id": "attempt-duplicate-ack",
                "task_kind": "sum_range",
                "parameters": {"start": 5, "end": 8},
                "assignment_epoch": 12,
                "loan_id": None,
            },
        }
        result = manager.process_inbound_message(assignment)
        assert result is not None
        assert len(manager.pending_results) == 1

        manager.current_task = None
        ack = {
            "version": "sd_2026_2_v1",
            "type": "task_ack",
            "message_id": "f04f8f47-05ad-460a-90a8-b7a7ac9ef24b",
            "request_id": result["request_id"],
            "timestamp_ms": 1727170000710,
            "source": assignment["source"],
            "destination": assignment["destination"],
            "payload": {
                "task_id": "task-043",
                "attempt_id": "attempt-duplicate-ack",
                "accepted": True,
                "reason": "duplicate result",
            },
        }
        assert manager.process_task_ack(ack) is True
        assert len(manager.pending_results) == 0
    finally:
        server_sock.close()
        client_sock.close()


def test_worker_replays_pending_result_after_reconnect():
    from worker.worker import WorkerControlSession, WorkerSessionManager

    config = {
        "group_label": "p2p-cluster",
        "node_id": "11111111-1111-4111-8111-111111111111",
        "node_label": "worker-reconnect",
        "node_role": "worker",
        "host": "127.0.0.1",
        "port": 9104,
        "assignment_epoch": 10,
        "loan_id": None,
    }
    manager = WorkerSessionManager("127.0.0.1", 9000, config)
    server_one, client_one = socket.socketpair()
    server_two, client_two = socket.socketpair()
    try:
        first_session = WorkerControlSession(client_one)
        manager._set_session(first_session)
        assignment = {
            "version": "sd_2026_2_v1",
            "type": "task_assignment",
            "message_id": "056a7a3d-1fb8-458d-ae47-ed62e526c922",
            "request_id": "5d80ae21-b5e8-4ec3-9104-976a13134541",
            "timestamp_ms": 1727170000500,
            "source": {
                "group_label": "p2p-cluster",
                "node_id": "22222222-2222-4222-8222-222222222222",
                "node_label": "master",
                "node_role": "master",
            },
            "destination": {
                "group_label": "p2p-cluster",
                "node_id": "11111111-1111-4111-8111-111111111111",
                "node_label": "worker-reconnect",
                "node_role": "worker",
            },
            "payload": {
                "task_id": "task-041",
                "task_label": "task-0009",
                "attempt_id": "attempt-reconnect-1",
                "task_kind": "sum_range",
                "parameters": {"start": 10, "end": 20},
                "assignment_epoch": 10,
                "loan_id": None,
            },
        }
        sent = manager.process_inbound_message(assignment)
        assert sent is not None
        assert len(manager.pending_results) == 1
        first_session._mark_closed()

        second_session = WorkerControlSession(client_two)
        resent = manager.retransmit_pending_results(second_session)
        assert resent == 1
        raw = server_two.recv(4096)
        replay = json.loads(raw.decode("utf-8").strip())
        assert replay["payload"]["task_id"] == "task-041"
        assert replay["payload"]["attempt_id"] == "attempt-reconnect-1"
        assert replay["payload"]["result"] == {"sum": 165}
    finally:
        server_one.close()
        client_one.close()
        server_two.close()
        client_two.close()


def test_worker_stops_retransmitting_when_master_rejects_stale_attempt():
    from worker.worker import WorkerControlSession, WorkerSessionManager

    config = {
        "group_label": "p2p-cluster",
        "node_id": "11111111-1111-4111-8111-111111111111",
        "node_label": "worker-stale",
        "node_role": "worker",
        "host": "127.0.0.1",
        "port": 9105,
        "assignment_epoch": 11,
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
            "message_id": "c5d5a319-ae07-43d0-a310-08953303b279",
            "request_id": "f3c33877-69f0-4148-9a9d-40776236d98a",
            "timestamp_ms": 1727170000600,
            "source": {
                "group_label": "p2p-cluster",
                "node_id": "22222222-2222-4222-8222-222222222222",
                "node_label": "master",
                "node_role": "master",
            },
            "destination": {
                "group_label": "p2p-cluster",
                "node_id": "11111111-1111-4111-8111-111111111111",
                "node_label": "worker-stale",
                "node_role": "worker",
            },
            "payload": {
                "task_id": "task-042",
                "task_label": "task-0010",
                "attempt_id": "attempt-old-1",
                "task_kind": "sum_range",
                "parameters": {"start": 1, "end": 3},
                "assignment_epoch": 11,
                "loan_id": None,
            },
        }
        sent = manager.process_inbound_message(assignment)
        assert sent is not None
        assert len(manager.pending_results) == 1

        stale = {
            "version": "sd_2026_2_v1",
            "type": "task_ack",
            "message_id": "f37ea86b-0158-4d83-a6b9-dc2bab71ce83",
            "request_id": sent["request_id"],
            "timestamp_ms": 1727170000610,
            "source": assignment["source"],
            "destination": assignment["destination"],
            "payload": {
                "task_id": "task-042",
                "attempt_id": "attempt-old-1",
                "accepted": False,
                "reason": "stale_attempt",
            },
        }
        assert manager.process_task_ack(stale) is False
        assert len(manager.pending_results) == 0
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
        "message_id": "fc997ae3-1eed-4375-bb0f-d9337ff35af2",
        "request_id": "bf9aeed4-fa72-44bc-950c-138d0cb3cf7b",
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


def test_master_task_queue_snapshot_reports_statuses_timeouts_and_retries():
    from master.master import MasterTaskQueue

    queue = MasterTaskQueue(task_timeout_ms=5, max_task_attempts=3)
    queue.submit({"task_id": "task-snapshot-complete", "task_kind": "sum_range", "parameters": {"start": 1, "end": 5}})
    queue.submit({"task_id": "task-snapshot-retry", "task_kind": "sum_range", "parameters": {"start": 6, "end": 10}})
    queue.submit({"task_id": "task-snapshot-pending", "task_kind": "sum_range", "parameters": {"start": 11, "end": 15}})

    first_assignment = queue.claim_next_task("worker-a", assignment_epoch=1, loan_id=None)
    assert first_assignment is not None
    assert queue.complete_task("task-snapshot-complete", first_assignment["attempt_id"], accepted=True, reason="completed", payload={"status": "ok", "result": {"sum": 15}})[0] is True

    second_assignment = queue.claim_next_task("worker-b", assignment_epoch=1, loan_id=None)
    assert second_assignment is not None
    task_id_retried = second_assignment["task_id"]
    assert queue.mark_task_timed_out(task_id_retried, reason="timeout") is True

    retry_assignment = queue.claim_next_task("worker-c", assignment_epoch=2, loan_id=None)
    assert retry_assignment is not None
    assert retry_assignment["task_id"] == task_id_retried
    assert queue.complete_task(task_id_retried, retry_assignment["attempt_id"], accepted=True, reason="completed", payload={"status": "ok", "result": {"sum": 36}})[0] is True

    pending_assignment = queue.claim_next_task("worker-a", assignment_epoch=1, loan_id=None)
    assert pending_assignment is not None
    assert queue.release_task("task-snapshot-pending", attempt_id=pending_assignment["attempt_id"], reason="released") is True

    snapshot = queue.snapshot()
    assert snapshot["total_tasks"] == 3
    assert snapshot["completed"] == 2
    assert snapshot["pending"] == 1
    assert snapshot["running"] == 0
    assert snapshot["failed"] == 0
    assert snapshot["timeout_attempts"] == 1
    assert snapshot["recovered_after_timeout"] == 1
    assert snapshot["tasks_with_multiple_attempts"] == 1
    assert "task-snapshot-pending" in snapshot["pending_task_ids"]
    assert task_id_retried in snapshot["completed_task_ids"]
    assert "task-snapshot-complete" in snapshot["completed_task_ids"]

    failed_queue = MasterTaskQueue(task_timeout_ms=5, max_task_attempts=2)
    failed_queue.submit({"task_id": "task-snapshot-maxfail", "task_kind": "sum_range", "parameters": {"start": 1, "end": 5}})
    first_failed = failed_queue.claim_next_task("worker-a", assignment_epoch=1, loan_id=None)
    assert first_failed is not None
    failed_queue.mark_task_timed_out("task-snapshot-maxfail", reason="timeout")
    second_failed = failed_queue.claim_next_task("worker-b", assignment_epoch=2, loan_id=None)
    assert second_failed is not None
    failed_queue.mark_task_timed_out("task-snapshot-maxfail", reason="timeout")

    failed_snapshot = failed_queue.snapshot()
    assert failed_snapshot["failed"] == 1
    assert failed_snapshot["failed_task_ids"] == ["task-snapshot-maxfail"]


def test_metrics_request_and_response_messages_match_protocol_contract():
    collector = {
        "group_label": "p2p-cluster",
        "node_id": "33333333-3333-4333-8333-333333333333",
        "node_label": "collector-a",
        "node_role": "collector",
    }
    master = {
        "group_label": "p2p-cluster",
        "node_id": "22222222-2222-4222-8222-222222222222",
        "node_label": "master",
        "node_role": "master",
    }

    request = build_metrics_request_message(collector, master, request_id="74f7e687-53d7-43bf-95f9-24f6e32a238f")
    assert validate_message(request) is True
    snapshot = {
        "total_tasks": 2,
        "completed": 1,
        "pending": 1,
        "running": 0,
        "failed": 0,
        "timeout_attempts": 0,
        "recovered_after_timeout": 0,
        "tasks_with_multiple_attempts": 0,
        "pending_task_ids": ["task-10"],
        "running_task_ids": [],
        "failed_task_ids": [],
        "completed_task_ids": ["task-09"],
    }
    response = build_metrics_response_message(request, master, collector, metrics=snapshot)
    assert validate_message(response) is True
    assert response["request_id"] == "74f7e687-53d7-43bf-95f9-24f6e32a238f"
    assert response["payload"] == snapshot


def test_master_responds_to_metrics_request_without_changing_task_state():
    queue = MasterTaskQueue(task_timeout_ms=30000, max_task_attempts=3)
    queue.submit({"task_id": "task-metrics-1", "task_kind": "sum_range", "parameters": {"start": 1, "end": 5}})
    queue.submit({"task_id": "task-metrics-2", "task_kind": "sum_range", "parameters": {"start": 6, "end": 10}})
    before = queue.snapshot()

    master_identity = {
        "group_label": "p2p-cluster",
        "node_id": "22222222-2222-4222-8222-222222222222",
        "node_label": "master",
        "node_role": "master",
    }
    collector_identity = {
        "group_label": "p2p-cluster",
        "node_id": "33333333-3333-4333-8333-333333333333",
        "node_label": "collector-a",
        "node_role": "collector",
    }
    registry = MasterRegistry()
    server_sock, client_sock = socket.socketpair()
    client_sock.settimeout(2.0)
    try:
        thread = threading.Thread(
            target=handle_client,
            args=(server_sock, ("127.0.0.1", 9000), registry, master_identity, queue),
            daemon=True,
        )
        thread.start()

        request = build_metrics_request_message(collector_identity, master_identity, request_id="4255db65-bb39-4668-b203-f0deda7970d0")
        send_msg(client_sock, request)
        messages = recv_msgs(client_sock, bytearray())
        assert messages is not None and any(msg.get("type") == "metrics_response" for msg in messages)

        response = next(msg for msg in messages if msg.get("type") == "metrics_response")
        assert response["request_id"] == "4255db65-bb39-4668-b203-f0deda7970d0"
        assert response["payload"] == before
        assert queue.snapshot() == before
    finally:
        client_sock.close()
        server_sock.close()


def test_collector_exports_json_and_csv_reports(tmp_path):
    metrics = {
        "total_tasks": 3,
        "completed": 2,
        "pending": 1,
        "running": 0,
        "failed": 0,
        "timeout_attempts": 1,
        "recovered_after_timeout": 1,
        "tasks_with_multiple_attempts": 1,
        "pending_task_ids": ["task-1"],
        "running_task_ids": [],
        "failed_task_ids": [],
        "completed_task_ids": ["task-0", "task-2"],
    }

    json_path, csv_path = export_reports(metrics, tmp_path, timestamp="2026-10-15T12-00-00Z")
    assert json_path.exists()
    assert csv_path.exists()

    payload = json.loads(json_path.read_text(encoding="utf-8"))
    assert payload == metrics
    csv_text = csv_path.read_text(encoding="utf-8")
    assert "total_tasks" in csv_text
    assert "task-1" in csv_text


def test_run_cli_periodic_collection_exports_unique_reports_and_handles_keyboard_interrupt(monkeypatch, tmp_path):
    calls = []
    collected = {"total_tasks": 2, "completed": 1, "pending": 1, "running": 0, "failed": 0, "timeout_attempts": 0, "recovered_after_timeout": 0, "tasks_with_multiple_attempts": 0, "pending_task_ids": ["task-10"], "running_task_ids": [], "failed_task_ids": [], "completed_task_ids": ["task-09"]}

    def fake_request_metrics(host, port, *, timeout=5.0, request_id=None, collector_identity=None):
        calls.append((host, port, request_id, collector_identity))
        return collected

    def fake_sleep(_seconds):
        raise KeyboardInterrupt

    monkeypatch.setattr("collector.collector.request_metrics", fake_request_metrics)
    monkeypatch.setattr("collector.collector.time.sleep", fake_sleep)

    exit_code = run_cli(["--host", "127.0.0.1", "--port", "9000", "--output-dir", str(tmp_path), "--interval-seconds", "0.01", "--once"])
    assert exit_code == 0
    assert len(calls) == 1

    calls.clear()
    exit_code = run_cli(["--host", "127.0.0.1", "--port", "9000", "--output-dir", str(tmp_path), "--interval-seconds", "0.01"])
    assert exit_code == 0
    assert len(calls) == 1

    json_files = sorted(tmp_path.glob("metrics_*.json"))
    assert json_files
    assert len(json_files) >= 1


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


def test_master_does_not_requeue_completed_task_after_timeout_or_worker_unavailable():
    from master.master import MasterTaskQueue

    queue = MasterTaskQueue(task_timeout_ms=5, max_task_attempts=3)
    queue.submit({"task_id": "task-completed-no-requeue", "task_kind": "sum_range", "parameters": {"start": 1, "end": 5}})

    assignment = queue.claim_next_task("worker-a", assignment_epoch=1, loan_id=None)
    assert assignment is not None

    accepted, reason = queue.complete_task(
        "task-completed-no-requeue",
        assignment["attempt_id"],
        accepted=True,
        reason="completed",
        payload={"status": "ok", "result": {"sum": 15}},
    )
    assert accepted is True
    assert reason == "completed"
    assert queue.get_task("task-completed-no-requeue")["status"] == "completed"

    timeout_result = queue.mark_task_timed_out("task-completed-no-requeue", reason="timeout")
    unavailable_result = queue.handle_worker_unavailable("worker-a", reason="socket closed")

    assert timeout_result is False
    assert unavailable_result == []
    assert queue.get_task("task-completed-no-requeue")["status"] == "completed"
    assert queue.get_task("task-completed-no-requeue")["active_attempt_id"] is None


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


def test_master_requeues_task_after_nok_result_and_keeps_same_identity():
    from master.master import MasterTaskQueue

    queue = MasterTaskQueue(task_timeout_ms=30000, max_task_attempts=3)
    queue.submit({"task_id": "task-nok-1", "task_kind": "sum_range", "parameters": {"start": 10, "end": 3}})
    assignment = queue.claim_next_task("worker-a", assignment_epoch=1, loan_id=None)
    assert assignment is not None

    accepted, reason = queue.complete_task(
        "task-nok-1",
        assignment["attempt_id"],
        accepted=False,
        reason="attempt_failed",
        payload={
            "task_id": "task-nok-1",
            "task_label": assignment["task_label"],
            "attempt_id": assignment["attempt_id"],
            "assignment_epoch": 1,
            "loan_id": None,
            "status": "nok",
            "result": None,
            "error_code": "invalid_range",
        },
    )
    assert accepted is True
    assert reason == "attempt_failed"

    current = queue.get_task("task-nok-1")
    assert current["status"] == "pending"
    assert current["assigned_to"] is None

    retry = queue.claim_next_task("worker-b", assignment_epoch=2, loan_id=None)
    assert retry is not None
    assert retry["task_id"] == "task-nok-1"
    assert retry["task_label"] == assignment["task_label"]
    assert retry["attempt_id"] != assignment["attempt_id"]


def test_master_marks_task_failed_after_three_nok_attempts():
    from master.master import MasterTaskQueue

    queue = MasterTaskQueue(task_timeout_ms=30000, max_task_attempts=3)
    queue.submit({"task_id": "task-nok-3", "task_kind": "sum_range", "parameters": {"start": 1, "end": 5}})

    first = queue.claim_next_task("worker-a", assignment_epoch=1, loan_id=None)
    assert first is not None
    queue.complete_task(
        "task-nok-3",
        first["attempt_id"],
        accepted=False,
        reason="attempt_failed",
        payload={
            "task_id": "task-nok-3",
            "task_label": first["task_label"],
            "attempt_id": first["attempt_id"],
            "assignment_epoch": 1,
            "loan_id": None,
            "status": "nok",
            "result": None,
            "error_code": "invalid_range",
        },
    )

    second = queue.claim_next_task("worker-b", assignment_epoch=2, loan_id=None)
    assert second is not None
    queue.complete_task(
        "task-nok-3",
        second["attempt_id"],
        accepted=False,
        reason="attempt_failed",
        payload={
            "task_id": "task-nok-3",
            "task_label": second["task_label"],
            "attempt_id": second["attempt_id"],
            "assignment_epoch": 2,
            "loan_id": None,
            "status": "nok",
            "result": None,
            "error_code": "invalid_range",
        },
    )

    third = queue.claim_next_task("worker-c", assignment_epoch=3, loan_id=None)
    assert third is not None
    queue.complete_task(
        "task-nok-3",
        third["attempt_id"],
        accepted=False,
        reason="attempt_failed",
        payload={
            "task_id": "task-nok-3",
            "task_label": third["task_label"],
            "attempt_id": third["attempt_id"],
            "assignment_epoch": 3,
            "loan_id": None,
            "status": "nok",
            "result": None,
            "error_code": "invalid_range",
        },
    )

    failed = queue.get_task("task-nok-3")
    assert failed["status"] == "failed"
    assert failed["failed_reason"] == "max_task_attempts_exceeded"


def test_master_rejects_stale_nok_result_after_reassignment():
    from master.master import MasterTaskQueue

    queue = MasterTaskQueue(task_timeout_ms=30000, max_task_attempts=3)
    queue.submit({"task_id": "task-nok-stale", "task_kind": "sum_range", "parameters": {"start": 1, "end": 5}})

    first = queue.claim_next_task("worker-a", assignment_epoch=1, loan_id=None)
    assert first is not None
    queue.handle_worker_unavailable("worker-a", reason="socket closed")

    second = queue.claim_next_task("worker-b", assignment_epoch=2, loan_id=None)
    assert second is not None

    stale_accepted, stale_reason = queue.complete_task(
        "task-nok-stale",
        first["attempt_id"],
        accepted=False,
        reason="attempt_failed",
        payload={
            "task_id": "task-nok-stale",
            "task_label": first["task_label"],
            "attempt_id": first["attempt_id"],
            "assignment_epoch": 1,
            "loan_id": None,
            "status": "nok",
            "result": None,
            "error_code": "invalid_range",
        },
    )
    assert stale_accepted is False
    assert stale_reason == "stale_attempt"

    current = queue.get_task("task-nok-stale")
    assert current["status"] == "running"
    assert current["active_attempt_id"] == second["attempt_id"]


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
        "message_id": "80e71052-a436-4888-9de5-1482a3df0a0f",
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
        "message_id": "89a240c7-7176-4568-92a4-2b4d404f4b2f",
        "request_id": "88512301-81de-4dc2-a7be-84a699a12e6d",
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
        "message_id": "c7ceb31c-3263-4003-a3d1-4f222fc38afe",
        "request_id": "e02ddeea-709c-4edf-88de-737e78b4e400",
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
        "message_id": "48c18c15-e509-48f0-83cb-2d53cc5a4f29",
        "request_id": "1be08558-c30c-4ee4-9e98-d8d08f53cbee",
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

    bad_key = dict(unknown_type)
    bad_key["type"] = "heartbeat"
    bad_key["BadKey"] = "invalid"

    list_type = dict(unknown_type)
    list_type["type"] = []

    cases = (
        (missing_field, "invalid_message"),
        (wrong_capitalization, "invalid_message"),
        (unknown_type, "unsupported_type"),
        (unknown_version, "unsupported_version"),
        (bad_key, "invalid_message"),
        (list_type, "invalid_message"),
    )

    for message, expected_code in cases:
        reply = exchange(message)
        assert reply is not None
        assert reply["type"] == "protocol_error"
        assert reply["payload"]["error_code"] == expected_code
        assert reply["payload"]["error_code"] in {
            "invalid_message",
            "invalid_state",
            "stale_epoch",
            "reservation_expired",
            "identity_conflict",
            "request_conflict",
            "unsupported_version",
            "unsupported_type",
        }
        assert isinstance(reply["payload"]["error_message"], str)
        assert reply["payload"]["error_message"].strip()

    assert exchange(malformed_json) is None

    valid_message = {
        "version": "sd_2026_2_v1",
        "type": "register_worker",
        "message_id": "f75b01d1-57b2-48d5-9550-1f063234d331",
        "request_id": "99c8b37a-04b5-44a9-b430-ef0f848ddfc3",
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
    assert valid_reply["payload"]["accepted"] is True
    assert len(registry.workers) == 1
    assert registry.workers["44444444-4444-4444-8444-444444444444"]["node_label"] == "worker-good"



def test_ct_03_recovers_on_same_connection():
    registry = MasterRegistry()
    master_identity = {
        "group_label": "p2p-cluster",
        "node_id": "22222222-2222-4222-8222-222222222222",
        "node_label": "master",
        "node_role": "master",
    }

    worker_identity = {
        "group_label": "p2p-cluster",
        "node_id": "44444444-4444-4444-8444-444444444444",
        "node_label": "worker-good",
        "node_role": "worker",
    }

    server_sock, client_sock = socket.socketpair()
    thread = threading.Thread(
        target=handle_client,
        args=(server_sock, None, registry, master_identity),
        daemon=True,
    )
    thread.start()
    client_sock.settimeout(2)

    def receive():
        data = bytearray()
        while b"\n" not in data:
            chunk = client_sock.recv(4096)
            if not chunk:
                raise AssertionError("Conexao encerrada antes da resposta")
            data.extend(chunk)
        return json.loads(data.split(b"\n", 1)[0].decode("utf-8"))

    try:
        invalid = {
            "version": "sd_2026_2_v1",
            "type": "unknown_event",
            "message_id": str(uuid.uuid4()),
            "request_id": str(uuid.uuid4()),
            "timestamp_ms": 1727170000123,
            "source": worker_identity,
            "destination": master_identity,
            "payload": {},
        }

        send_msg(client_sock, invalid)
        error = receive()

        assert error["type"] == "protocol_error"
        assert error["payload"]["error_code"] == "unsupported_type"
        assert error["request_id"] == invalid["request_id"]

        valid = {
            "version": "sd_2026_2_v1",
            "type": "register_worker",
            "message_id": str(uuid.uuid4()),
            "request_id": str(uuid.uuid4()),
            "timestamp_ms": 1727170000456,
            "source": worker_identity,
            "destination": master_identity,
            "payload": build_register_payload(
                {**master_identity, "host": "127.0.0.1", "port": 9000},
                "127.0.0.1",
                9106,
                assignment_epoch=1,
                loan_id="loan-recovery",
            ),
        }

        send_msg(client_sock, valid)
        response = receive()

        assert response["type"] == "registration_ack"
        assert response["request_id"] == valid["request_id"]
        assert worker_identity["node_id"] in registry.workers

    finally:
        client_sock.close()
        thread.join(timeout=2)


def test_ct_03_protocol_error_does_not_trigger_another_error():
    registry = MasterRegistry()
    master_identity = {
        "group_label": "p2p-cluster",
        "node_id": "22222222-2222-4222-8222-222222222222",
        "node_label": "master",
        "node_role": "master",
    }
    worker_identity = {
        "group_label": "p2p-cluster",
        "node_id": "44444444-4444-4444-8444-444444444444",
        "node_label": "worker-good",
        "node_role": "worker",
    }

    server_sock, client_sock = socket.socketpair()
    thread = threading.Thread(
        target=handle_client,
        args=(server_sock, None, registry, master_identity),
        daemon=True,
    )
    thread.start()
    client_sock.settimeout(0.5)

    try:
        message = {
            "version": "sd_2026_2_v1",
            "type": "protocol_error",
            "message_id": str(uuid.uuid4()),
            "request_id": str(uuid.uuid4()),
            "timestamp_ms": 1727170000123,
            "source": worker_identity,
            "destination": master_identity,
            "payload": {
                "error_code": "invalid_message",
                "error_message": "Erro recebido do worker",
            },
        }

        send_msg(client_sock, message)

        try:
            response = client_sock.recv(4096)
        except socket.timeout:
            response = None

        assert response is None, (
            "Master respondeu a um protocol_error recebido"
        )
        assert thread.is_alive(), (
            "Master encerrou a conexao inesperadamente"
        )

    finally:
        client_sock.close()
        thread.join(timeout=2)


def test_ct_01_oversized_residual_closes_only_bad_connection():
    registry = MasterRegistry()
    master_identity = {
        "group_label": "p2p-cluster",
        "node_id": str(uuid.uuid4()),
        "node_label": "master",
        "node_role": "master",
    }

    def connect():
        server, client = socket.socketpair()
        thread = threading.Thread(
            target=handle_client,
            args=(server, None, registry, master_identity),
            daemon=True,
        )
        thread.start()
        client.settimeout(3)
        return client, thread

    bad_client, bad_thread = connect()

    try:
        bad_client.sendall(b"A" * 65537)

        assert bad_client.recv(4096) == b""

        bad_thread.join(timeout=2)
        assert not bad_thread.is_alive()

        good_client, good_thread = connect()

        try:
            worker_identity = {
                "group_label": "p2p-cluster",
                "node_id": str(uuid.uuid4()),
                "node_label": "worker-test",
                "node_role": "worker",
            }

            message = {
                "version": "sd_2026_2_v1",
                "type": "unknown_event",
                "message_id": str(uuid.uuid4()),
                "request_id": str(uuid.uuid4()),
                "timestamp_ms": 1727170000123,
                "source": worker_identity,
                "destination": master_identity,
                "payload": {},
            }

            send_msg(good_client, message)

            replies = recv_msgs(good_client, bytearray())

            assert replies is not None
            assert len(replies) == 1
            assert replies[0]["type"] == "protocol_error"
            assert replies[0]["payload"]["error_code"] == "unsupported_type"

        finally:
            good_client.close()
            good_thread.join(timeout=2)

    finally:
        bad_client.close()

def test_ct_01_ndjson_tcp_framing_acceptance():
    buffer = bytearray()
    messages = [
        {
            "version": "sd_2026_2_v1",
            "type": "register_worker",
            "message_id": "a9e9e9bb-8a9d-497a-9c48-24bc05ac8415",
            "request_id": "e909535b-7d5b-4b06-8ed2-d50a92e06af9",
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
            "message_id": "2bbae45b-2e14-4b9e-9b01-3a293f8f60b0",
            "request_id": "3eb301ff-e43b-499f-b3a0-0143c096b826",
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
            "message_id": "a2279565-d992-4a7a-9604-f1c513902789",
            "request_id": "875f6a91-d774-44d6-ac19-0744942d927b",
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
    assert [message["message_id"] for message in received] == ["a9e9e9bb-8a9d-497a-9c48-24bc05ac8415", "2bbae45b-2e14-4b9e-9b01-3a293f8f60b0", "a2279565-d992-4a7a-9604-f1c513902789"]
    assert received[0]["source"]["node_label"] == "worker-a"
    assert received[1]["source"]["node_label"] == "worker-b"
    assert received[2]["source"]["node_label"] == "worker-c"


def test_validate_message_rejects_missing_required_fields():
    invalid = {
        "version": "sd_2026_2_v1",
        "type": "register_worker",
        "message_id": "a9e9e9bb-8a9d-497a-9c48-24bc05ac8415",
        "request_id": "e909535b-7d5b-4b06-8ed2-d50a92e06af9",
        "timestamp_ms": 1727170000123,
    }
    assert validate_message(invalid) is False

    valid = {
        "version": "sd_2026_2_v1",
        "type": "register_worker",
        "message_id": "a9e9e9bb-8a9d-497a-9c48-24bc05ac8415",
        "request_id": "e909535b-7d5b-4b06-8ed2-d50a92e06af9",
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
        "message_id": "479ebeeb-d552-450f-9495-2237faa44bf0",
        "request_id": "b0ad8650-d07a-4124-bc94-05d999f3b7a6",
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
        "message_id": "19edbe28-5188-470f-af37-f3553d92a2a7",
        "request_id": "080295ae-d6db-4c38-9f76-f1b438ef2d58",
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
    assert valid_reply["payload"]["accepted"] is True
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
    assert invalid_reply["payload"]["error_code"] == "invalid_message"
    assert "assignment_epoch" in invalid_reply["payload"]["error_message"].lower()


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
        "message_id": "c1f7d491-2caf-4b9b-85a1-da5e0226cfb8",
        "request_id": "8ec5d309-20ef-41af-8067-48449cd9509d",
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
    assert rejected_reply["payload"]["accepted"] is False
    assert "group_label" in str(rejected_reply["payload"]["reason"]).lower()
    assert len(registry.workers) == 0

    accepted_request = {
        "version": "sd_2026_2_v1",
        "type": "register_worker",
        "message_id": "185e670a-1494-49ad-b3ed-4241e9fe85b7",
        "request_id": "6f842f78-25e3-4d1d-9046-0f73c80c8f2c",
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
    assert accepted_reply["payload"]["accepted"] is True
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
        request_id="abfeeefc-3b2b-4621-be0e-fb93628608ea",
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
        request_id="63208bb3-4336-4dee-ad6a-18438f1c6edb",
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
        "message_id": "0a6cea38-ed63-4311-8e6c-3403e438874d",
        "request_id": "b82174b0-3b72-44ec-989f-5e9b0a4f8e9b",
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
        request_id="adf701e0-aff3-4e37-a6c0-7ed47a135884",
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
        "message_id": str(uuid.uuid4()),
        "request_id": "9453cc97-cbd3-4c29-ad18-ef667d2ebfe4",
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
        "message_id": "34999f89-49ad-412d-8088-14e19f5b05b2",
        "request_id": "a172085d-80a9-427a-a81e-edca66919920",
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
    assert ack["payload"]["accepted"] is True

    assert rejected["payload"]["accepted"] is False
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

    send_heartbeat_to_worker(server_sock, master_identity, worker_identity, write_lock, sequence=21, request_id="5aa00756-ba74-4d55-9aaf-fbc70bd2b500")
    raw = client_sock.recv(4096)
    message = json.loads(raw.decode("utf-8").strip())

    assert message["type"] == "heartbeat"
    assert message["request_id"] == "5aa00756-ba74-4d55-9aaf-fbc70bd2b500"
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
        request_id="7cb5cb3c-6fe8-41f7-9e96-47fd2662a02b",
    )
    heartbeat_two = build_heartbeat_message(
        source=heartbeat_one["source"],
        destination=heartbeat_one["destination"],
        sequence=12,
        request_id="857adaea-d0ce-488a-9bac-6c46c8219405",
    )

    session.send_message(heartbeat_one)
    session.send_message(heartbeat_two)

    first_ack = build_heartbeat_ack_message(heartbeat_one, source=heartbeat_one["destination"], destination=heartbeat_one["source"])
    second_ack = build_heartbeat_ack_message(heartbeat_two, source=heartbeat_two["destination"], destination=heartbeat_two["source"])

    send_msg(server_sock, first_ack)
    send_msg(server_sock, second_ack)

    first_response = session.wait_for_message(types={"heartbeat_ack"}, request_id="7cb5cb3c-6fe8-41f7-9e96-47fd2662a02b", timeout=2)
    second_response = session.wait_for_message(types={"heartbeat_ack"}, request_id="857adaea-d0ce-488a-9bac-6c46c8219405", timeout=2)

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

    registry.begin_heartbeat_expectation("11111111-1111-4111-8111-111111111111", "62dcc526-107a-4d5c-808f-ac8f1807b9b5", 1)
    registry.begin_heartbeat_expectation("11111111-1111-4111-8111-111111111111", "1ba185b1-5f0d-45e6-9dd1-3c3611054f9e", 2)

    worker = registry.get_worker("11111111-1111-4111-8111-111111111111")
    assert worker["heartbeat_misses"] == 1
    assert worker["expected_heartbeat_request_id"] == "1ba185b1-5f0d-45e6-9dd1-3c3611054f9e"
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

    registry.begin_heartbeat_expectation("11111111-1111-4111-8111-111111111111", "62dcc526-107a-4d5c-808f-ac8f1807b9b5", 1)
    assert registry.process_heartbeat_ack("11111111-1111-4111-8111-111111111111", "62dcc526-107a-4d5c-808f-ac8f1807b9b5", 1, "alive") is True
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

    registry.begin_heartbeat_expectation("11111111-1111-4111-8111-111111111111", "62dcc526-107a-4d5c-808f-ac8f1807b9b5", 1)
    assert registry.process_heartbeat_ack("11111111-1111-4111-8111-111111111111", "08c39a7f-79c5-42b8-8f27-486251acfe39", 1, "alive") is False
    assert registry.get_worker("11111111-1111-4111-8111-111111111111")["status"] == "suspect"
    assert registry.get_worker("11111111-1111-4111-8111-111111111111")["heartbeat_misses"] == 1

    registry.begin_heartbeat_expectation("11111111-1111-4111-8111-111111111111", "1ba185b1-5f0d-45e6-9dd1-3c3611054f9e", 2)
    assert registry.process_heartbeat_ack("11111111-1111-4111-8111-111111111111", "220746c5-e8c9-48ab-877f-b3a827fe5733", 2, "alive") is False
    assert registry.get_worker("11111111-1111-4111-8111-111111111111")["status"] == "suspect"
    assert registry.get_worker("11111111-1111-4111-8111-111111111111")["heartbeat_misses"] == 2

    registry.begin_heartbeat_expectation("11111111-1111-4111-8111-111111111111", "76a11e89-1bf3-4929-b81c-dcf864117527", 3)
    assert registry.process_heartbeat_ack("11111111-1111-4111-8111-111111111111", "d3dba2a7-d433-495a-a126-dd96af56e5d0", 3, "alive") is False
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

    registry.begin_heartbeat_expectation("11111111-1111-4111-8111-111111111111", "d1880b1e-cb4f-4b58-9b2b-5d4fb8b2890d", 7)
    assert registry.process_heartbeat_ack("11111111-1111-4111-8111-111111111111", "d1880b1e-cb4f-4b58-9b2b-5d4fb8b2890d", 7, "alive") is True
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

    for sequence, request_id in ((1, "0dadaacb-f5a7-440f-8bbd-007ed209e402"), (2, "da330919-6e7a-47f1-8613-8f6029170864"), (3, "168dd0f2-01b2-401f-8076-bd95a787d565")):
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

    registry.begin_heartbeat_expectation("11111111-1111-4111-8111-111111111111", "62dcc526-107a-4d5c-808f-ac8f1807b9b5", 1)
    assert registry.process_heartbeat_ack("11111111-1111-4111-8111-111111111111", "1ba185b1-5f0d-45e6-9dd1-3c3611054f9e", 1, "alive") is False
    assert registry.get_worker("11111111-1111-4111-8111-111111111111")["status"] == "suspect"

    registry.begin_heartbeat_expectation("11111111-1111-4111-8111-111111111111", "1ba185b1-5f0d-45e6-9dd1-3c3611054f9e", 2)
    assert registry.process_heartbeat_ack("11111111-1111-4111-8111-111111111111", "1ba185b1-5f0d-45e6-9dd1-3c3611054f9e", 3, "alive") is False
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

    for sequence, request_id in ((1, "62dcc526-107a-4d5c-808f-ac8f1807b9b5"), (2, "1ba185b1-5f0d-45e6-9dd1-3c3611054f9e"), (3, "76a11e89-1bf3-4929-b81c-dcf864117527")):
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

    registry.begin_heartbeat_expectation("11111111-1111-4111-8111-111111111111", "d1880b1e-cb4f-4b58-9b2b-5d4fb8b2890d", 7)
    assert registry.process_heartbeat_ack("11111111-1111-4111-8111-111111111111", "08c39a7f-79c5-42b8-8f27-486251acfe39", 7, "alive") is False
    assert registry.get_worker("11111111-1111-4111-8111-111111111111")["heartbeat_misses"] == 1

    registry.begin_heartbeat_expectation("11111111-1111-4111-8111-111111111111", "47f3d1a6-c09d-42ae-9ac2-83c870fb4da1", 8)
    assert registry.process_heartbeat_ack("11111111-1111-4111-8111-111111111111", "47f3d1a6-c09d-42ae-9ac2-83c870fb4da1", 9, "alive") is False
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

    for sequence, request_id in ((1, "62dcc526-107a-4d5c-808f-ac8f1807b9b5"), (2, "1ba185b1-5f0d-45e6-9dd1-3c3611054f9e"), (3, "76a11e89-1bf3-4929-b81c-dcf864117527")):
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
        "message_id": "2c2436aa-8089-4908-8e6f-b92f8176c976",
        "request_id": "4ee3932c-9f7d-43c4-a1ba-e643679a271f",
        "timestamp_ms": 1727170000123,
        "source": {"group_label": "p2p-cluster", "node_id": "11111111-1111-4111-8111-111111111111", "node_label": "worker-a", "node_role": "worker"},
        "destination": {"group_label": "p2p-cluster", "node_id": "22222222-2222-4222-8222-222222222222", "node_label": "master", "node_role": "master"},
        "payload": {"home_master": {"group_label": "p2p-cluster", "node_id": "22222222-2222-4222-8222-222222222222", "node_label": "master", "node_role": "master", "host": "127.0.0.1", "port": 9000}, "assignment_epoch": 0, "loan_id": None, "host": "127.0.0.1", "port": 9101},
    }
    ack = {"type": "registration_ack", "request_id": message["request_id"], "payload": {"accepted": True, "assignment_epoch": 0, "reason": None}}
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
        "message_id": "3747b145-2df0-48d9-b5c9-d1745fda09bd",
        "request_id": "7012d7fe-c6b0-4304-8f24-4d400cc4c0a9",
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
        "message_id": "f6007fdb-eb7a-4ad2-b4af-12286ccff936",
        "request_id": "f807e703-18f8-4293-bc8b-6a5e9914d224",
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
        "message_id": "cc0c6de4-bf47-43f6-bcf0-48689f6a3827",
        "request_id": "01dff6c5-d07c-4cd6-96ac-8ffc9c41b348",
        "timestamp_ms": 1727170000126,
        "source": {"group_label": "p2p-cluster", "node_id": "11111111-1111-4111-8111-111111111111", "node_label": "worker-a", "node_role": "worker"},
        "destination": {"group_label": "p2p-cluster", "node_id": "22222222-2222-4222-8222-222222222222", "node_label": "master", "node_role": "master"},
        "payload": {"home_master": {"group_label": "p2p-cluster", "node_id": "22222222-2222-4222-8222-222222222222", "node_label": "master", "node_role": "master", "host": "127.0.0.1", "port": 9000}, "assignment_epoch": 0, "loan_id": None, "host": "127.0.0.1", "port": 9101},
    }
    ack = {"type": "registration_ack", "request_id": message["request_id"], "payload": {"accepted": True, "assignment_epoch": 0, "reason": None}}
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
        "message_id": "4eedb791-4d93-4272-a74b-e4f8d5468d92",
        "request_id": "692d12be-0d8a-4fe1-8cde-7d01ad291fec",
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
    ack = {"type": "heartbeat_ack", "request_id": "26bf0b56-5b64-47fb-b6c4-49659a04e6cb", "payload": {"sequence": 7, "status": "alive"}}

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
        "message_id": "2b074ab8-bc3e-45d6-b5dc-257b1aa68972",
        "request_id": "cd8f77d6-be93-4f81-b38b-df915e3fe5a7",
        "timestamp_ms": 1727170000123,
        "source": {"group_label": "p2p-cluster", "node_id": "11111111-1111-4111-8111-111111111111", "node_label": "worker-a", "node_role": "worker"},
        "destination": {"group_label": "p2p-cluster", "node_id": "22222222-2222-4222-8222-222222222222", "node_label": "master", "node_role": "master"},
        "payload": {"sequence": 1},
    }
    response = {"type": "heartbeat_ack", "request_id": "cd8f77d6-be93-4f81-b38b-df915e3fe5a7", "payload": {"status": "alive"}}

    registry.remember_processed_message(message, response)
    assert registry.get_processed_response(message) is response
    assert registry.get_processed_response({**message, "request_id": "50785bef-b1eb-41a2-8a7a-98f4452fa5ec"}) is None
    assert registry.get_processed_response({**message, "source": {**message["source"], "node_id": "22222222-2222-4222-8222-222222222222"}}) is None
    assert registry.get_processed_response({**message, "type": "register_worker"}) is None


def test_handle_client_deduplicates_retransmitted_register_worker_and_reuses_cached_ack():
    registry = MasterRegistry()
    master_identity = {"group_label": "p2p-cluster", "node_id": "22222222-2222-4222-8222-222222222222", "node_label": "master", "node_role": "master"}
    source = {"group_label": "p2p-cluster", "node_id": "11111111-1111-4111-8111-111111111111", "node_label": "worker-a", "node_role": "worker"}
    message = {
        "version": "sd_2026_2_v1",
        "type": "register_worker",
        "message_id": "d967e0e5-b0fc-4de5-a02a-15b778458501",
        "request_id": "5e8a09b5-5f98-451f-b750-3e98428ef2fe",
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
    assert first_reply["payload"]["accepted"] is True
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
        "message_id": "9671f0a3-dcec-468a-a180-b42255fabaf9",
        "request_id": "63568542-dca6-44bb-b865-c235b9dfd376",
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
        request_id="ef6f3123-11a6-4c03-8437-28da0f60937c",
    )

    fragment = (json.dumps(heartbeat, separators=(",", ":"), ensure_ascii=False) + "\n").encode("utf-8")[:20]
    server_sock.sendall(fragment)
    time.sleep(0.1)
    assert not session.is_closed()

    server_sock.sendall((json.dumps(heartbeat, separators=(",", ":"), ensure_ascii=False) + "\n").encode("utf-8")[20:])
    ack = build_heartbeat_ack_message(heartbeat, source=heartbeat["destination"], destination=heartbeat["source"])
    server_sock.sendall((json.dumps(ack, separators=(",", ":"), ensure_ascii=False) + "\n").encode("utf-8"))

    response = session.wait_for_message(types={"heartbeat_ack"}, request_id="ef6f3123-11a6-4c03-8437-28da0f60937c", timeout=2)
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
        request_id="9eaf03ff-5f24-40a9-87a5-28ae134cd9de",
    )
    ack = build_heartbeat_ack_message(heartbeat, source=heartbeat["destination"], destination=heartbeat["source"])
    server_sock, client_sock = socket.socketpair()
    session = WorkerControlSession(client_sock)
    monkeypatch.setattr(worker_module, "log_event", lambda *args: received.setdefault("args", args))

    try:
        send_msg(server_sock, ack)
        assert session.wait_for_message(types={"heartbeat_ack"}, request_id="9eaf03ff-5f24-40a9-87a5-28ae134cd9de", timeout=1)["request_id"] == "9eaf03ff-5f24-40a9-87a5-28ae134cd9de"
        assert received["args"][4] == "9eaf03ff-5f24-40a9-87a5-28ae134cd9de"
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


def test_worker_preserves_interleaved_messages_for_other_waiters():
    import socket
    import threading
    import time

    from worker.worker import WorkerControlSession

    server_sock, client_sock = socket.socketpair()
    session = WorkerControlSession(client_sock)

    try:
        assignment = {
            "type": "task_assignment",
            "request_id": "assignment-request",
        }
        heartbeat_ack = {
            "type": "heartbeat_ack",
            "request_id": "heartbeat-request",
        }

        result = {}
        errors = []

        def wait_heartbeat():
            try:
                result["heartbeat"] = session.wait_for_message(
                    types={"heartbeat_ack"},
                    request_id="heartbeat-request",
                    timeout=1.0,
                )
            except Exception as exc:
                errors.append(exc)

        waiter = threading.Thread(target=wait_heartbeat)
        waiter.start()

        with session._message_condition:
            session._pending_messages.append(assignment)
            session._message_condition.notify_all()

        time.sleep(0.05)

        with session._message_condition:
            session._pending_messages.append(heartbeat_ack)
            session._message_condition.notify_all()

        waiter.join(timeout=2.0)

        assert not waiter.is_alive()
        assert not errors
        assert result["heartbeat"] == heartbeat_ack

        received_assignment = session.wait_for_message(
            types={"task_assignment"},
            timeout=0.5,
        )
        assert received_assignment == assignment
    finally:
        session.close()
        server_sock.close()


def test_worker_socket_preserves_task_assignment_during_heartbeat_ack():
    import socket
    import uuid

    from protocol import (
        build_heartbeat_message,
        build_heartbeat_ack_message,
        build_task_assignment_message,
        send_msg,
    )
    from worker.worker import WorkerControlSession

    worker = {
        "group_label": "p2p-cluster",
        "node_id": "11111111-1111-4111-8111-111111111111",
        "node_label": "worker-a",
        "node_role": "worker",
    }
    master = {
        "group_label": "p2p-cluster",
        "node_id": "22222222-2222-4222-8222-222222222222",
        "node_label": "master",
        "node_role": "master",
    }

    server_sock, client_sock = socket.socketpair()
    session = WorkerControlSession(client_sock)

    try:
        assignment = build_task_assignment_message(
            {"request_id": str(uuid.uuid4())},
            master,
            worker,
            task_id="task-integration-001",
            task_label="task-0001",
            attempt_id=str(uuid.uuid4()),
            task_kind="sum_range",
            parameters={"start": 1, "end": 100},
            assignment_epoch=1,
            loan_id=None,
        )

        heartbeat = build_heartbeat_message(
            source=worker,
            destination=master,
            sequence=1,
            request_id=str(uuid.uuid4()),
        )
        heartbeat_ack = build_heartbeat_ack_message(
            heartbeat,
            source=master,
            destination=worker,
        )

        send_msg(server_sock, assignment)
        send_msg(server_sock, heartbeat_ack)

        received_ack = session.wait_for_message(
            types={"heartbeat_ack"},
            request_id=heartbeat["request_id"],
            timeout=2.0,
        )

        assert received_ack["payload"]["status"] == "alive"

        received_assignment = session.wait_for_message(
            types={"task_assignment"},
            request_id=assignment["request_id"],
            timeout=2.0,
        )

        assert received_assignment == assignment
        assert received_assignment["payload"]["task_id"] == "task-integration-001"

    finally:
        session.close()
        server_sock.close()


def test_master_validates_task_result_worker_epoch_and_loan():
    from master.master import MasterTaskQueue

    queue = MasterTaskQueue()
    queue.submit({
        "task_id": "task-security-1",
        "task_kind": "sum_range",
        "parameters": {"start": 1, "end": 10},
    })

    assignment = queue.claim_next_task(
        "worker-a",
        assignment_epoch=7,
        loan_id="loan-security-1",
    )
    assert assignment is not None

    valid_payload = {
        "status": "ok",
        "result": {"sum": 55},
        "assignment_epoch": 7,
        "loan_id": "loan-security-1",
    }

    cases = [
        ("worker-b", valid_payload, "wrong_worker"),
        (
            "worker-a",
            {**valid_payload, "assignment_epoch": 6},
            "stale_assignment_epoch",
        ),
        (
            "worker-a",
            {**valid_payload, "loan_id": "wrong-loan"},
            "invalid_loan_id",
        ),
    ]

    for worker_id, payload, expected_reason in cases:
        accepted, reason = queue.complete_task(
            "task-security-1",
            assignment["attempt_id"],
            accepted=True,
            reason="completed",
            payload=payload,
            worker_id=worker_id,
        )

        assert accepted is False
        assert reason == expected_reason

        current = queue.get_task("task-security-1")
        assert current["status"] == "running"
        assert current["assigned_to"] == "worker-a"

    accepted, reason = queue.complete_task(
        "task-security-1",
        assignment["attempt_id"],
        accepted=True,
        reason="completed",
        payload=valid_payload,
        worker_id="worker-a",
    )

    assert accepted is True
    assert reason == "completed"
    assert queue.get_task("task-security-1")["status"] == "completed"

    duplicate_accepted, duplicate_reason = queue.complete_task(
        "task-security-1",
        assignment["attempt_id"],
        accepted=True,
        reason="completed",
        payload=valid_payload,
        worker_id="worker-a",
    )

    assert duplicate_accepted is True
    assert duplicate_reason == "duplicate result"

    invalid_duplicate, invalid_reason = queue.complete_task(
        "task-security-1",
        assignment["attempt_id"],
        accepted=True,
        reason="completed",
        payload=valid_payload,
        worker_id="worker-b",
    )

    assert invalid_duplicate is False
    assert invalid_reason == "wrong_worker"


def test_master_rejects_stale_worker_session_after_reconnect():
    registry = MasterRegistry()
    master = {
        "group_label": "p2p-cluster",
        "node_id": "22222222-2222-4222-8222-222222222222",
        "node_label": "master",
        "node_role": "master",
    }
    worker = {
        "group_label": "p2p-cluster",
        "node_id": "11111111-1111-4111-8111-111111111111",
        "node_label": "worker-a",
        "node_role": "worker",
    }

    queue = MasterTaskQueue()
    queue.submit({
        "task_id": "stale-session-task",
        "task_kind": "sum_range",
        "parameters": {"start": 1, "end": 10},
    })
    assignment = queue.claim_next_task(
        worker["node_id"],
        assignment_epoch=0,
        loan_id=None,
    )
    assert assignment is not None

    old_server, old_client = socket.socketpair()
    new_server, new_client = socket.socketpair()

    try:
        registry.bind_session(worker["node_id"], old_server)
        registry.bind_session(worker["node_id"], new_server)

        assert not registry.is_active_session(worker["node_id"], old_server)
        assert registry.is_active_session(worker["node_id"], new_server)

        def result_message():
            return {
                "version": "sd_2026_2_v1",
                "type": "task_result",
                "message_id": str(uuid.uuid4()),
                "request_id": str(uuid.uuid4()),
                "timestamp_ms": int(time.time() * 1000),
                "source": worker,
                "destination": master,
                "payload": {
                    "task_id": "stale-session-task",
                    "task_label": "stale-session-task",
                    "attempt_id": assignment["attempt_id"],
                    "status": "ok",
                    "result": {"sum": 55},
                    "error_code": None,
                    "assignment_epoch": 0,
                    "loan_id": None,
                },
            }

        stale_thread = threading.Thread(
            target=handle_client,
            args=(old_server, None, registry, master, queue),
            daemon=True,
        )
        stale_thread.start()
        send_msg(old_client, result_message())
        stale_thread.join(timeout=2)

        assert not stale_thread.is_alive()
        assert queue.get_task("stale-session-task")["status"] == "running"

        active_thread = threading.Thread(
            target=handle_client,
            args=(new_server, None, registry, master, queue),
            daemon=True,
        )
        active_thread.start()
        new_client.settimeout(2)

        send_msg(new_client, result_message())
        response = recv_msgs(new_client, bytearray())

        assert response
        assert response[0]["type"] == "task_ack"
        assert response[0]["payload"]["accepted"] is True
        assert queue.get_task("stale-session-task")["status"] == "completed"

    finally:
        for sock in (old_client, new_client, old_server, new_server):
            try:
                sock.close()
            except OSError:
                pass


def test_task_completion_blocks_concurrent_worker_reconnect():
    from unittest.mock import patch

    registry = MasterRegistry()
    worker_id = "11111111-1111-4111-8111-111111111111"
    old_server, old_client = socket.socketpair()
    new_server, new_client = socket.socketpair()

    queue = MasterTaskQueue()
    queue.submit({
        "task_id": "concurrent-reconnect-task",
        "task_kind": "sum_range",
        "parameters": {"start": 1, "end": 10},
    })
    assignment = queue.claim_next_task(
        worker_id,
        assignment_epoch=0,
        loan_id=None,
    )
    assert assignment is not None

    entered_completion = threading.Event()
    release_completion = threading.Event()
    reconnect_finished = threading.Event()
    errors = []
    result = {}

    original_complete = queue.complete_task

    def delayed_complete(*args, **kwargs):
        entered_completion.set()
        if not release_completion.wait(timeout=3):
            raise AssertionError("Timeout aguardando liberacao da conclusao")
        return original_complete(*args, **kwargs)

    def complete_result():
        try:
            with registry._lock:
                assert registry.is_active_session(worker_id, old_server)
                result["value"] = queue.complete_task(
                    "concurrent-reconnect-task",
                    assignment["attempt_id"],
                    accepted=True,
                    reason="completed",
                    payload={
                        "status": "ok",
                        "result": {"sum": 55},
                        "assignment_epoch": 0,
                        "loan_id": None,
                    },
                    worker_id=worker_id,
                )
        except BaseException as exc:
            errors.append(exc)

    def reconnect():
        try:
            registry.bind_session(worker_id, new_server)
            reconnect_finished.set()
        except BaseException as exc:
            errors.append(exc)

    try:
        registry.bind_session(worker_id, old_server)

        with patch.object(queue, "complete_task", side_effect=delayed_complete):
            completion_thread = threading.Thread(target=complete_result, daemon=True)
            completion_thread.start()

            assert entered_completion.wait(timeout=2)

            reconnect_thread = threading.Thread(target=reconnect, daemon=True)
            reconnect_thread.start()

            assert not reconnect_finished.wait(timeout=0.15)

            release_completion.set()

            completion_thread.join(timeout=3)
            reconnect_thread.join(timeout=3)

        assert not errors, errors
        assert not completion_thread.is_alive()
        assert not reconnect_thread.is_alive()
        assert result["value"][0] is True
        assert reconnect_finished.is_set()
        assert registry.is_active_session(worker_id, new_server)
        assert queue.get_task("concurrent-reconnect-task")["status"] == "completed"

    finally:
        release_completion.set()
        for sock in (old_client, new_client, old_server, new_server):
            try:
                sock.close()
            except OSError:
                pass


def test_master_reuses_cached_task_result_ack_without_completing_twice():
    from unittest.mock import patch

    registry = MasterRegistry()
    master = {
        "group_label": "p2p-cluster",
        "node_id": "22222222-2222-4222-8222-222222222222",
        "node_label": "master",
        "node_role": "master",
    }
    worker = {
        "group_label": "p2p-cluster",
        "node_id": "11111111-1111-4111-8111-111111111111",
        "node_label": "worker-a",
        "node_role": "worker",
    }

    queue = MasterTaskQueue()
    queue.submit({
        "task_id": "dedup-result-task",
        "task_kind": "sum_range",
        "parameters": {"start": 1, "end": 10},
    })
    assignment = queue.claim_next_task(
        worker["node_id"],
        assignment_epoch=0,
        loan_id=None,
    )
    assert assignment is not None

    message = {
        "version": "sd_2026_2_v1",
        "type": "task_result",
        "message_id": str(uuid.uuid4()),
        "request_id": str(uuid.uuid4()),
        "timestamp_ms": int(time.time() * 1000),
        "source": worker,
        "destination": master,
        "payload": {
            "task_id": "dedup-result-task",
            "task_label": "dedup-result-task",
            "attempt_id": assignment["attempt_id"],
            "status": "ok",
            "result": {"sum": 55},
            "error_code": None,
            "assignment_epoch": 0,
            "loan_id": None,
        },
    }

    server, client = socket.socketpair()
    registry.bind_session(worker["node_id"], server)

    thread = threading.Thread(
        target=handle_client,
        args=(server, None, registry, master, queue),
        daemon=True,
    )
    thread.start()
    client.settimeout(2)

    try:
        with patch.object(queue, "complete_task", wraps=queue.complete_task) as complete:
            send_msg(client, message)
            first = recv_msgs(client, bytearray())

            assert first
            assert first[0]["type"] == "task_ack"
            assert first[0]["payload"]["accepted"] is True

            send_msg(client, message)
            second = recv_msgs(client, bytearray())

            assert second
            assert second[0] == first[0]
            assert complete.call_count == 1

        assert queue.get_task("dedup-result-task")["status"] == "completed"
        assert registry.get_processed_response(message) == first[0]

    finally:
        client.close()
        thread.join(timeout=2)
        server.close()


def test_concurrent_task_result_retransmission_reuses_original_ack():
    from unittest.mock import patch
    from protocol import build_task_ack_message

    registry = MasterRegistry()
    master = {
        "group_label": "p2p-cluster",
        "node_id": "22222222-2222-4222-8222-222222222222",
        "node_label": "master",
        "node_role": "master",
    }
    worker = {
        "group_label": "p2p-cluster",
        "node_id": "11111111-1111-4111-8111-111111111111",
        "node_label": "worker-a",
        "node_role": "worker",
    }

    queue = MasterTaskQueue()
    queue.submit({
        "task_id": "concurrent-dedup-task",
        "task_kind": "sum_range",
        "parameters": {"start": 1, "end": 10},
    })
    assignment = queue.claim_next_task(
        worker["node_id"],
        assignment_epoch=0,
        loan_id=None,
    )
    assert assignment is not None

    message = {
        "version": "sd_2026_2_v1",
        "type": "task_result",
        "message_id": str(uuid.uuid4()),
        "request_id": str(uuid.uuid4()),
        "timestamp_ms": int(time.time() * 1000),
        "source": worker,
        "destination": master,
        "payload": {
            "task_id": "concurrent-dedup-task",
            "task_label": "concurrent-dedup-task",
            "attempt_id": assignment["attempt_id"],
            "status": "ok",
            "result": {"sum": 55},
            "error_code": None,
            "assignment_epoch": 0,
            "loan_id": None,
        },
    }

    server, client = socket.socketpair()
    registry.bind_session(worker["node_id"], server)

    entered = threading.Event()
    release = threading.Event()
    errors = []
    responses = []
    original_complete = queue.complete_task

    def delayed_complete(*args, **kwargs):
        entered.set()
        if not release.wait(timeout=3):
            raise AssertionError("Timeout aguardando liberacao")
        return original_complete(*args, **kwargs)

    def process_result():
        try:
            with registry._lock:
                assert registry.is_active_session(worker["node_id"], server)
                if registry.has_request_conflict(message):
                    raise AssertionError("Conflito inesperado")
                response = registry.get_processed_response(message)
                if response is None:
                    accepted, reason = queue.complete_task(
                        message["payload"]["task_id"],
                        message["payload"]["attempt_id"],
                        accepted=True,
                        reason="completed",
                        payload=message["payload"],
                        worker_id=worker["node_id"],
                    )
                    response = build_task_ack_message(
                        message,
                        source=master,
                        destination=worker,
                        task_id=message["payload"]["task_id"],
                        attempt_id=message["payload"]["attempt_id"],
                        accepted=accepted,
                        reason=reason,
                    )
                    registry.remember_processed_message(message, response)
                responses.append(response)
        except BaseException as exc:
            errors.append(exc)

    try:
        with patch.object(queue, "complete_task", side_effect=delayed_complete) as complete:
            first = threading.Thread(target=process_result, daemon=True)
            first.start()
            assert entered.wait(timeout=2)

            second = threading.Thread(target=process_result, daemon=True)
            second.start()

            release.set()
            first.join(timeout=3)
            second.join(timeout=3)

            assert not errors, errors
            assert complete.call_count == 1
        assert not first.is_alive()
        assert not second.is_alive()
        assert len(responses) == 2
        assert responses[0] == responses[1]
        assert responses[0]["payload"]["accepted"] is True
        assert queue.get_task("concurrent-dedup-task")["status"] == "completed"

    finally:
        release.set()
        client.close()
        server.close()


def test_worker_limits_result_to_three_sends_per_session():
    from worker.worker import WorkerSessionManager

    class FakeSession:
        def __init__(self):
            self.messages = []

        def is_closed(self):
            return False

        def send_message(self, message):
            self.messages.append(message)

    manager = WorkerSessionManager("127.0.0.1", 9000, {})
    session = FakeSession()

    manager.pending_results["task-1::attempt-1"] = {
        "task_id": "task-1",
        "attempt_id": "attempt-1",
        "message": {"type": "task_result"},
        "last_sent_at": 0.0,
        "send_count": 1,
        "retry_session_id": id(session),
        "session_send_count": 1,
    }

    assert manager.retransmit_pending_results(session) == 1

    pending = manager.pending_results["task-1::attempt-1"]
    pending["last_sent_at"] -= 5.0

    assert manager.retransmit_pending_results(session) == 1

    pending["last_sent_at"] -= 5.0

    assert manager.retransmit_pending_results(session) == 0
    assert len(session.messages) == 2
    assert pending["session_send_count"] == 3
    assert pending["send_count"] == 3
    assert "task-1::attempt-1" in manager.pending_results


def test_worker_failed_retransmission_does_not_consume_attempt():
    import pytest
    from worker.worker import WorkerSessionManager

    class FailingSession:
        def is_closed(self):
            return False

        def send_message(self, message):
            raise OSError("simulated connection failure")

    manager = WorkerSessionManager("127.0.0.1", 9000, {})
    session = FailingSession()

    key = "task-1::attempt-1"
    manager.pending_results[key] = {
        "task_id": "task-1",
        "attempt_id": "attempt-1",
        "message": {"type": "task_result"},
        "last_sent_at": 0.0,
        "send_count": 1,
        "retry_session_id": id(session),
        "session_send_count": 1,
    }

    with pytest.raises(OSError, match="simulated connection failure"):
        manager.retransmit_pending_results(session)

    pending = manager.pending_results[key]
    assert pending["session_send_count"] == 1
    assert pending["send_count"] == 1
    assert pending["last_sent_at"] == 0.0
    assert key in manager.pending_results


def test_performance_report_protocol_validation():
    from protocol import build_performance_report_message, validate_message

    master = {
        "group_label": "p2p-cluster",
        "node_id": "22222222-2222-4222-8222-222222222222",
        "node_label": "master",
        "node_role": "master",
    }
    collector = {
        "group_label": "p2p-cluster",
        "node_id": "33333333-3333-4333-8333-333333333333",
        "node_label": "collector",
        "node_role": "collector",
    }
    metrics = {
        "uptime_seconds": 30,
        "tasks_pending": 1,
        "tasks_running": 2,
        "tasks_completed": 5,
        "tasks_failed": 0,
        "workers_owned": 3,
        "workers_active_here": 3,
        "workers_busy": 2,
        "workers_idle": 1,
        "workers_reserved": 0,
        "workers_lent": 0,
        "workers_received": 0,
        "workers_unavailable": 0,
        "thresholds": {
            "saturation_threshold": 10,
            "release_threshold": 3,
        },
        "loans": [],
        "neighbors": [],
    }

    message = build_performance_report_message(
        master,
        collector,
        metrics=metrics,
    )

    assert validate_message(message) is True
    assert message["type"] == "performance_report"
    assert message["payload"] == metrics


def test_performance_report_rejects_invalid_counters():
    from protocol import validate_performance_report_payload

    metrics = {
        "uptime_seconds": 10,
        "tasks_pending": 0,
        "tasks_running": 1,
        "tasks_completed": 2,
        "tasks_failed": 0,
        "workers_owned": 2,
        "workers_active_here": 2,
        "workers_busy": 1,
        "workers_idle": 1,
        "workers_reserved": 0,
        "workers_lent": 0,
        "workers_received": 0,
        "workers_unavailable": 0,
        "thresholds": {},
        "loans": [],
        "neighbors": [],
    }

    assert validate_performance_report_payload(metrics) is True

    invalid = dict(metrics)
    invalid["tasks_completed"] = -1
    assert validate_performance_report_payload(invalid) is False

    invalid = dict(metrics)
    invalid["workers_active_here"] = 5
    assert validate_performance_report_payload(invalid) is False

    invalid = dict(metrics)
    invalid.pop("tasks_running")
    assert validate_performance_report_payload(invalid) is False

    invalid = dict(metrics)
    invalid["loans"] = {}
    assert validate_performance_report_payload(invalid) is False


def test_collector_tcp_receives_and_persists_performance_report(tmp_path):
    from collector.collector import receive_performance_reports
    from protocol import build_performance_report_message

    master = {
        "group_label": "p2p-cluster",
        "node_id": "22222222-2222-4222-8222-222222222222",
        "node_label": "master",
        "node_role": "master",
    }
    collector = {
        "group_label": "p2p-cluster",
        "node_id": "33333333-3333-4333-8333-333333333333",
        "node_label": "collector",
        "node_role": "collector",
    }
    metrics = {
        "uptime_seconds": 10,
        "tasks_pending": 0,
        "tasks_running": 1,
        "tasks_completed": 2,
        "tasks_failed": 0,
        "workers_owned": 2,
        "workers_active_here": 2,
        "workers_busy": 1,
        "workers_idle": 1,
        "workers_reserved": 0,
        "workers_lent": 0,
        "workers_received": 0,
        "workers_unavailable": 0,
        "thresholds": {},
        "loans": [],
        "neighbors": [],
    }

    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as probe:
        probe.bind(("127.0.0.1", 0))
        port = probe.getsockname()[1]

    thread = threading.Thread(
        target=receive_performance_reports,
        kwargs={
            "host": "127.0.0.1",
            "port": port,
            "output_dir": tmp_path,
            "collector_identity": collector,
        },
        daemon=True,
    )
    thread.start()

    message = build_performance_report_message(master, collector, metrics=metrics)

    deadline = time.monotonic() + 5.0
    while True:
        try:
            with socket.create_connection(("127.0.0.1", port), timeout=1.0) as conn:
                send_msg(conn, message)
            break
        except (ConnectionRefusedError, TimeoutError):
            if time.monotonic() >= deadline:
                raise
            time.sleep(0.05)

    report_path = tmp_path / "performance_reports.jsonl"

    while not report_path.exists():
        if time.monotonic() >= deadline:
            pytest.fail("Collector did not persist performance_report")
        time.sleep(0.02)

    lines = report_path.read_text(encoding="utf-8").splitlines()
    assert len(lines) == 1
    saved = json.loads(lines[0])
    assert saved["type"] == "performance_report"
    assert saved["payload"] == metrics
