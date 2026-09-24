import json
import uuid

from master.master import build_ack_message, load_or_create_config as load_master_config
from protocol import MasterRegistry, extract_complete_messages, validate_message
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


def test_master_registry_updates_existing_uuid():
    registry = MasterRegistry()
    first = {"uuid": "u-9", "label": "worker-a", "host": "127.0.0.1", "port": 9001}
    second = {"uuid": "u-9", "label": "worker-a", "host": "127.0.0.1", "port": 9002}

    registry.upsert_worker(first)
    registry.upsert_worker(second)

    assert len(registry.workers) == 1
    assert registry.workers["u-9"]["port"] == 9002


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

    assert ack["version"] == "sd_2026_2_v1"
    assert ack["request_id"] == request["request_id"]
    assert ack["message_id"] != request["message_id"]
    assert ack["source"]["node_role"] == "master"
    assert ack["destination"]["node_role"] == "worker"
    assert ack["payload"]["status"] == "accepted"
