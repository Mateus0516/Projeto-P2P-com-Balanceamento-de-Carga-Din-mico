import threading

from master.master import PeerLoanManager
from protocol import MasterRegistry


def test_peer_loan_manager_mirrors_committed_loan_for_requester(tmp_path):
    requester = {
        "group_label": "p2p-cluster",
        "node_id": "a1111111-1111-4111-8111-111111111111",
        "node_label": "master-requester",
        "node_role": "master",
        "host": "127.0.0.1",
        "port": 9201,
    }
    home = {
        "group_label": "p2p-cluster",
        "node_id": "a2222222-2222-4222-8222-222222222222",
        "node_label": "master-home",
        "node_role": "master",
        "host": "127.0.0.1",
        "port": 9202,
    }
    worker = {
        "group_label": "p2p-cluster",
        "node_id": "a3333333-3333-4333-8333-333333333333",
        "node_label": "worker-borrowed",
        "node_role": "worker",
        "host": "127.0.0.1",
        "port": 9301,
    }
    loan_id = "a4444444-4444-4444-8444-444444444444"

    manager = PeerLoanManager(
        MasterRegistry(),
        master_identity=requester,
        ledger_path=tmp_path / "requester-loans.json",
    )

    mirror = manager.mirror_committed_loan(
        loan_id=loan_id,
        requester_master=requester,
        target_master=home,
        home_master=home,
        worker=worker,
        assignment_epoch=5,
    )

    assert mirror["status"] == "accepted"
    status = manager.get_loan_status(loan_id)
    assert status["loan_state"] == "committed"
    assert status["assignment_epoch"] == 5


def test_peer_loan_manager_rejects_worker_mismatch_on_return(tmp_path):
    master_identity = {
        "group_label": "p2p-cluster",
        "node_id": "b1111111-1111-4111-8111-111111111111",
        "node_label": "master-home",
        "node_role": "master",
        "host": "127.0.0.1",
        "port": 9202,
    }
    worker = {
        "group_label": "p2p-cluster",
        "node_id": "b2222222-2222-4222-8222-222222222222",
        "node_label": "worker-return",
        "node_role": "worker",
        "host": "127.0.0.1",
        "port": 9302,
    }
    registry = MasterRegistry()
    registry.upsert_worker(worker)
    manager = PeerLoanManager(registry, master_identity=master_identity, ledger_path=tmp_path / "loan.json")
    decision = manager.process_request_help(
        {"requester_master": master_identity, "workers_needed": 1, "tasks_pending": 1},
        source={"group_label": "p2p-cluster", "node_id": "b3333333-3333-4333-8333-333333333333", "node_label": "master-requester", "node_role": "master"},
    )

    mismatch = dict(worker)
    mismatch["node_id"] = "b4444444-4444-4444-8444-444444444444"
    returned = manager.process_notify_worker_returned(
        decision["loan_id"],
        worker=mismatch,
        assignment_epoch=decision["assignment_epoch"],
        source=master_identity,
    )

    assert returned["status"] == "rejected"
    assert returned["reason"]["code"] == "worker_mismatch"


def test_worker_return_resets_loan_state(monkeypatch):
    from worker.worker import WorkerSessionManager
    from protocol import build_command_release_message

    home_master = {
        "group_label": "p2p-cluster",
        "node_id": "c1111111-1111-4111-8111-111111111111",
        "node_label": "master-home",
        "node_role": "master",
        "host": "127.0.0.1",
        "port": 9202,
    }
    target_master = {
        "group_label": "p2p-cluster",
        "node_id": "c2222222-2222-4222-8222-222222222222",
        "node_label": "master-requester",
        "node_role": "master",
        "host": "127.0.0.1",
        "port": 9201,
    }
    config = {
        "group_label": "p2p-cluster",
        "node_id": "c4444444-4444-4444-8444-444444444444",
        "node_label": "worker-return-reset",
        "node_role": "worker",
        "host": "127.0.0.1",
        "port": 9303,
        "home_master": home_master,
        "master": target_master,
        "assignment_epoch": 9,
        "loan_id": "c3333333-3333-4333-8333-333333333333",
    }

    manager = WorkerSessionManager(target_master["host"], target_master["port"], config, stop_event=threading.Event())
    command = build_command_release_message(
        {"request_id": "d1111111-1111-4111-8111-111111111111"},
        source=target_master,
        destination={
            "group_label": config["group_label"],
            "node_id": config["node_id"],
            "node_label": config["node_label"],
            "node_role": "worker",
        },
        loan_id=config["loan_id"],
        assignment_epoch=9,
        reason="return to home",
    )

    monkeypatch.setattr("worker.worker.open_control_session", lambda *args, **kwargs: (_ for _ in ()).throw(OSError("offline")))

    assert manager.process_inbound_message(command) is True
    assert config["assignment_epoch"] == 0
    assert config["loan_id"] is None
    assert manager.busy is False
