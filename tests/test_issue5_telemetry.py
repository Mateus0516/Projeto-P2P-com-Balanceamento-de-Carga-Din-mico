import pytest

from master.master import MasterTaskQueue, PeerLoanManager, build_master_performance_metrics
from protocol import MasterRegistry, build_performance_report_message, validate_performance_report_payload


def test_performance_report_source_destination_and_uptime_are_exact():
    source = {
        "group_label": "grupo_07",
        "node_id": "22222222-2222-4222-8222-222222222222",
        "node_label": "grupo_07_master_01",
        "node_role": "master",
        "host": "127.0.0.1",
        "peers": [],
    }
    destination = {
        "group_label": "grupo_00",
        "node_id": "c011ec70-0000-4000-8000-000000000001",
        "node_label": "grupo_00_collector_01",
        "node_role": "collector",
    }
    metrics = {
        "uptime_seconds": 12,
        "tasks_pending": 0,
        "tasks_running": 0,
        "tasks_completed": 0,
        "tasks_failed": 0,
        "workers_owned": 0,
        "workers_active_here": 0,
        "workers_busy": 0,
        "workers_idle": 0,
        "workers_reserved": 0,
        "workers_lent": 0,
        "workers_received": 0,
        "workers_unavailable": 0,
        "thresholds": {"saturation_threshold": 10, "release_threshold": 3, "stable_samples": 3},
        "loans": [],
        "neighbors": [],
    }

    with pytest.raises(ValueError, match="source"):
        build_performance_report_message(source, destination, metrics=metrics)

    exact_source = {key: source[key] for key in ("group_label", "node_id", "node_label", "node_role")}
    message = build_performance_report_message(exact_source, destination, metrics=metrics)
    assert set(message["source"]) == {"group_label", "node_id", "node_label", "node_role"}
    assert set(message["destination"]) == {"group_label", "node_id", "node_label", "node_role"}

    for invalid_uptime in (1.5, True, -1, "10"):
        invalid = dict(metrics)
        invalid["uptime_seconds"] = invalid_uptime
        assert validate_performance_report_payload(invalid) is False


def test_master_performance_metrics_use_offered_committed_and_closed_loans(tmp_path):
    home = {
        "group_label": "grupo_07", "node_id": "22222222-2222-4222-8222-222222222222",
        "node_label": "grupo_07_master_01", "node_role": "master", "host": "127.0.0.1", "port": 9000,
        "saturation_threshold": 10, "release_threshold": 3, "stable_samples": 3, "peers": [],
    }
    target = {
        "group_label": "grupo_07", "node_id": "33333333-3333-4333-8333-333333333333",
        "node_label": "grupo_07_master_02", "node_role": "master", "host": "127.0.0.1", "port": 9001,
    }
    offered_worker = {"group_label": "grupo_07", "node_id": "44444444-4444-4444-8444-444444444444", "node_label": "grupo_07_master_01_worker_01", "node_role": "worker"}
    committed_worker = {"group_label": "grupo_07", "node_id": "55555555-5555-4555-8555-555555555555", "node_label": "grupo_07_master_01_worker_02", "node_role": "worker"}

    registry = MasterRegistry()
    for worker in (offered_worker, committed_worker):
        registry.upsert_worker({**worker, "host": "127.0.0.1", "port": 9300, "status": "available"})

    manager = PeerLoanManager(registry, master_identity=home, ledger_path=tmp_path / "loan.json")
    manager._loans = {
        "offered-loan": {"loan_id": "aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa", "loan_label": "grupo_07_master_01_loan_000001", "worker": offered_worker, "home_master": home, "target_master": target, "loan_state": "offered", "assignment_epoch": 0},
        "committed-loan": {"loan_id": "bbbbbbbb-bbbb-4bbb-8bbb-bbbbbbbbbbbb", "loan_label": "grupo_07_master_01_loan_000002", "worker": committed_worker, "home_master": home, "target_master": target, "loan_state": "committed", "assignment_epoch": 1},
        "closed-loan": {"loan_id": "cccccccc-cccc-4ccc-8ccc-cccccccccccc", "loan_label": "grupo_07_master_01_loan_000003", "worker": offered_worker, "home_master": home, "target_master": target, "loan_state": "closed", "assignment_epoch": 2},
    }

    metrics = build_master_performance_metrics(registry, MasterTaskQueue(), home, uptime_seconds=12.9, peer_manager=manager)

    assert type(metrics["uptime_seconds"]) is int
    assert metrics["uptime_seconds"] == 12
    assert metrics["workers_reserved"] == 1
    assert metrics["workers_lent"] == 1
    assert len(metrics["loans"]) == 2
    assert {loan["loan_state"] for loan in metrics["loans"]} == {"offered", "committed"}
    assert validate_performance_report_payload(metrics)
    assert not validate_performance_report_payload({**metrics, "loans": [{"loan_state": "committed"}]})


def test_master_performance_metrics_neighbors_use_real_peer_state(tmp_path):
    home = {
        "group_label": "grupo_07", "node_id": "22222222-2222-4222-8222-222222222222",
        "node_label": "grupo_07_master_01", "node_role": "master", "host": "127.0.0.1", "port": 9000,
        "saturation_threshold": 10, "release_threshold": 3, "stable_samples": 3,
        "peers": [{"group_label": "grupo_07", "node_id": "33333333-3333-4333-8333-333333333333", "node_label": "grupo_07_master_02", "node_role": "master", "host": "127.0.0.1", "port": 9001}],
    }
    registry = MasterRegistry()
    manager = PeerLoanManager(registry, master_identity=home, ledger_path=tmp_path / "loan.json")
    manager.record_peer_contact("33333333-3333-4333-8333-333333333333", status="available", last_seen_ms=1700000000000)

    metrics = build_master_performance_metrics(registry, MasterTaskQueue(), home, uptime_seconds=5, peer_manager=manager)

    assert len(metrics["neighbors"]) == 1
    neighbor = metrics["neighbors"][0]
    assert set(neighbor) == {"node", "status", "last_seen_ms"}
    assert set(neighbor["node"]) == {"group_label", "node_id", "node_label", "node_role", "host", "port"}
    assert neighbor["status"] == "available"
    assert neighbor["last_seen_ms"] == 1700000000000

    assert validate_performance_report_payload(metrics)
    neighbor["last_seen_ms"] = True
    assert not validate_performance_report_payload(metrics)
