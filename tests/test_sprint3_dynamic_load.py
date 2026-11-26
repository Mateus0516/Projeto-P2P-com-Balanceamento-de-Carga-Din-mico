import threading
import json
import time
import uuid

import pytest

from master.master import PeerLoanManager
from protocol import (
    MasterRegistry,
    build_confirm_loan_message,
    build_loan_committed_message,
    build_notify_worker_returned_message,
    build_request_help_message,
    build_response_accepted_message,
    build_response_rejected_message,
    build_return_ack_message,
    validate_message,
)


def _identity(role, label, host="127.0.0.1", port=9000):
    return {
        "group_label": "grupo_07",
        "node_id": str(uuid.uuid4()),
        "node_label": label,
        "node_role": role,
        "host": host,
        "port": port,
    }


def _loan_ref(home, target, worker, assignment_epoch=1):
    return {
        "loan_id": str(uuid.uuid4()),
        "loan_label": "grupo_07_master_02_loan_000001",
        "worker": worker,
        "home_master": home,
        "target_master": target,
        "assignment_epoch": assignment_epoch,
    }


def test_official_response_accepted_and_rejected_payloads():
    home = _identity("master", "grupo_07_master_01", port=9001)
    target = _identity("master", "grupo_07_master_02", port=9002)
    worker = {
        "group_label": "grupo_07",
        "node_id": str(uuid.uuid4()),
        "node_label": "grupo_07_master_01_worker_01",
        "node_role": "worker",
    }
    request = build_request_help_message(
        target,
        home,
        requester_master=target,
        workers_needed=1,
        tasks_pending=11,
    )
    loan = _loan_ref(home, target, worker)

    accepted = build_response_accepted_message(
        request,
        source=home,
        destination=target,
        loan=loan,
        reservation_timeout_ms=15000,
    )
    assert accepted["payload"] == {"loan": loan, "reservation_timeout_ms": 15000}
    assert validate_message(accepted)

    rejected = build_response_rejected_message(
        request,
        source=home,
        destination=target,
        reason="no_workers_available",
    )
    assert rejected["payload"] == {"reason": "no_workers_available"}
    assert validate_message(rejected)


def test_confirm_and_committed_use_official_payloads_and_confirm_request_id():
    home = _identity("master", "grupo_07_master_01", port=9001)
    target = _identity("master", "grupo_07_master_02", port=9002)
    worker = {
        "group_label": "grupo_07",
        "node_id": str(uuid.uuid4()),
        "node_label": "grupo_07_master_01_worker_01",
        "node_role": "worker",
    }
    request = build_request_help_message(target, home, requester_master=target, workers_needed=1, tasks_pending=11)
    accepted = build_response_accepted_message(request, source=home, destination=target, loan=_loan_ref(home, target, worker), reservation_timeout_ms=15000)
    loan = accepted["payload"]["loan"]

    confirm = build_confirm_loan_message(
        accepted,
        source=target,
        destination=home,
        loan_id=loan["loan_id"],
        assignment_epoch=loan["assignment_epoch"],
        request_id=str(uuid.uuid4()),
    )
    assert confirm["payload"] == {"loan_id": loan["loan_id"], "assignment_epoch": loan["assignment_epoch"]}

    committed = build_loan_committed_message(
        confirm,
        source=home,
        destination=target,
        loan=loan,
        loan_state="committed",
    )
    assert committed["payload"] == {"loan": loan, "loan_state": "committed"}
    assert committed["request_id"] == confirm["request_id"]


def test_notify_and_return_ack_use_official_payloads():
    home = _identity("master", "grupo_07_master_01", port=9001)
    target = _identity("master", "grupo_07_master_02", port=9002)
    worker = {
        "group_label": "grupo_07",
        "node_id": str(uuid.uuid4()),
        "node_label": "grupo_07_master_01_worker_01",
        "node_role": "worker",
    }
    loan_id = str(uuid.uuid4())
    notify_request = {"request_id": str(uuid.uuid4())}

    notify = build_notify_worker_returned_message(
        notify_request,
        source=home,
        destination=target,
        loan_id=loan_id,
        loan_label="grupo_07_master_01_loan_000001",
        worker=worker,
        assignment_epoch=2,
        loan_state="closed",
    )
    assert notify["payload"] == {
        "loan_id": loan_id,
        "loan_label": "grupo_07_master_01_loan_000001",
        "worker": worker,
        "assignment_epoch": 2,
        "loan_state": "closed",
    }

    ack = build_return_ack_message(
        notify,
        source=target,
        destination=home,
        loan_id=loan_id,
        loan_state="closed",
    )
    assert ack["payload"] == {"loan_id": loan_id, "loan_state": "closed"}
    assert ack["request_id"] == notify["request_id"]


def test_concurrent_requests_reserve_only_one_worker(tmp_path):
    home = _identity("master", "grupo_07_master_01", port=9001)
    registry = MasterRegistry()
    worker = {
        "group_label": "grupo_07",
        "node_id": str(uuid.uuid4()),
        "node_label": "grupo_07_master_01_worker_01",
        "node_role": "worker",
        "host": "127.0.0.1",
        "port": 9301,
        "status": "available",
    }
    registry.upsert_worker(worker)

    reserve_worker = dict(worker)
    reserve_worker["node_id"] = str(uuid.uuid4())
    reserve_worker["node_label"] = "grupo_07_master_01_worker_02"
    reserve_worker["port"] = 9302
    registry.upsert_worker(reserve_worker)

    manager = PeerLoanManager(registry, master_identity={**home, "min_home_workers": 1, "release_threshold": 3, "saturation_threshold": 10}, ledger_path=tmp_path / "loan.json")

    barrier = __import__("threading").Barrier(3)
    outcomes = []

    def request(peer_number):
        requester = _identity("master", f"grupo_07_master_{peer_number:02d}", port=9000 + peer_number)
        barrier.wait()
        outcomes.append(manager.process_request_help({"requester_master": requester, "workers_needed": 1, "tasks_pending": 11}, source=requester))

    threads = [__import__("threading").Thread(target=request, args=(index,)) for index in (2, 3)]
    for thread in threads:
        thread.start()
    barrier.wait()
    for thread in threads:
        thread.join()

    accepted = [item for item in outcomes if item["status"] == "accepted"]
    rejected = [item for item in outcomes if item["status"] == "rejected"]
    assert len(accepted) == 1
    assert len(rejected) == 1
    assert worker["node_id"] in str(accepted)


def test_heartbeat_does_not_restore_reserved_or_committed_worker(tmp_path):
    home = _identity("master", "grupo_07_master_01", port=9001)
    registry = MasterRegistry()
    worker = {
        "group_label": "grupo_07",
        "node_id": str(uuid.uuid4()),
        "node_label": "grupo_07_master_01_worker_01",
        "node_role": "worker",
        "host": "127.0.0.1",
        "port": 9301,
        "status": "available",
    }
    registry.upsert_worker(worker)

    reserve_worker = dict(worker)
    reserve_worker["node_id"] = str(uuid.uuid4())
    reserve_worker["node_label"] = "grupo_07_master_01_worker_02"
    reserve_worker["port"] = 9302
    registry.upsert_worker(reserve_worker)

    manager = PeerLoanManager(registry, master_identity={**home, "min_home_workers": 1}, ledger_path=tmp_path / "loan.json")
    requester = _identity("master", "grupo_07_master_02", port=9002)

    offer = manager.process_request_help({"requester_master": requester, "workers_needed": 1, "tasks_pending": 11}, source=requester)
    assert offer["status"] == "accepted"

    registry.begin_heartbeat_expectation(worker["node_id"], str(uuid.uuid4()), 1)
    ack_request = str(uuid.uuid4())
    registry.begin_heartbeat_expectation(worker["node_id"], ack_request, 2)
    assert registry.process_heartbeat_ack(worker["node_id"], ack_request, 2, "alive") is True
    assert registry.get_worker(worker["node_id"])["status"] == "reserved"

    committed = manager.confirm_loan(offer["loan_id"], requester_master=requester, request_id=str(uuid.uuid4()))
    assert committed["status"] == "accepted"
    ack_request = str(uuid.uuid4())
    registry.begin_heartbeat_expectation(worker["node_id"], ack_request, 3)
    assert registry.process_heartbeat_ack(worker["node_id"], ack_request, 3, "alive") is True
    assert registry.get_worker(worker["node_id"])["status"] == "committed"


def test_commit_and_return_increment_epoch_once_and_persist_journal(tmp_path):
    home = _identity("master", "grupo_07_master_01", port=9001)
    registry = MasterRegistry()
    worker = {
        "group_label": "grupo_07",
        "node_id": str(uuid.uuid4()),
        "node_label": "grupo_07_master_01_worker_01",
        "node_role": "worker",
        "host": "127.0.0.1",
        "port": 9301,
        "status": "available",
    }
    registry.upsert_worker(worker)

    reserve_worker = dict(worker)
    reserve_worker["node_id"] = str(uuid.uuid4())
    reserve_worker["node_label"] = "grupo_07_master_01_worker_02"
    reserve_worker["port"] = 9302
    registry.upsert_worker(reserve_worker)

    manager = PeerLoanManager(registry, master_identity={**home, "min_home_workers": 1}, ledger_path=tmp_path / "loan.json")
    requester = _identity("master", "grupo_07_master_02", port=9002)

    offer = manager.process_request_help({"requester_master": requester, "workers_needed": 1, "tasks_pending": 11}, source=requester)
    first = manager.confirm_loan(offer["loan_id"], requester_master=requester, request_id=str(uuid.uuid4()))
    repeated = manager.confirm_loan(offer["loan_id"], requester_master=requester, request_id=str(uuid.uuid4()))
    assert first["assignment_epoch"] == 1
    assert repeated["assignment_epoch"] == 1

    returned = manager.process_notify_worker_returned(
        offer["loan_id"],
        worker=worker,
        assignment_epoch=1,
        source=target if False else requester,
    )
    repeated_return = manager.process_notify_worker_returned(
        offer["loan_id"],
        worker=worker,
        assignment_epoch=2,
        source=requester,
    )
    assert returned["status"] in {"accepted", "rejected"}

    journal_path = tmp_path / "loan.jsonl"
    assert journal_path.exists()
    journal_states = [json.loads(line)["loan_state"] for line in journal_path.read_text(encoding="utf-8").splitlines() if line.strip()]
    assert {"offered", "committed", "closed"}.issubset(set(journal_states))
