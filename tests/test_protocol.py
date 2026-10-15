import json
import socket
import sys
import threading
import time
import uuid
from pathlib import Path

import pytest

from master.master import (
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
