import json
import socket
import threading
import uuid

from master.master import build_ack_message, handle_client, load_or_create_config as load_master_config
from protocol import MasterRegistry, extract_complete_messages, send_msg, validate_message
from worker.worker import load_or_create_config as load_worker_config


def test_extract_complete_messages_handles_fragmentation_and_coalescence():
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
            "payload": {"status": "online"},
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
            "payload": {"status": "online"},
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
            "payload": {"status": "online"},
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
    assert received[0]["message_id"] == "m1"
    assert received[1]["message_id"] == "m2"
    assert received[2]["message_id"] == "m3"


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
        "payload": {"status": "online"},
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
        "payload": {"host": "127.0.0.1", "port": 9101},
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
        "payload": {"host": "127.0.0.1", "port": 9101, "blob": "x" * 70000},
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
        "payload": {"host": "127.0.0.1", "port": 9101},
    }

    assert validate_message(message) is True


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
        "payload": {"host": "127.0.0.1", "port": 9105},
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
        "payload": {"host": "127.0.0.1", "port": 9106},
    }

    accepted_reply = register_once(accepted_request)
    assert accepted_reply["payload"]["status"] == "accepted"
    assert len(registry.workers) == 1
    assert registry.workers["44444444-4444-4444-8444-444444444444"]["node_label"] == "worker-good"


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
