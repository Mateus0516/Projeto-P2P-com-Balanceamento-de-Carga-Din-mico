import socket
import threading
import time
import uuid

import pytest

from protocol import (
    REQUIRED_NODE_FIELDS,
    build_command_redirect_message,
    build_command_release_message,
    build_task_ack_message,
    build_task_assignment_message,
    build_task_result_message,
    recv_msgs,
    send_msg,
)
from worker.worker import WorkerSessionManager, register_worker


def fixture_node(group, label, role):
    return {"group_label": group, "node_id": str(uuid.uuid4()), "node_label": label, "node_role": role}


def refs():
    home = {**fixture_node("grupo_07", "grupo_07_master_01", "master"), "host": "127.0.0.1", "port": 9000}
    target = {**fixture_node("grupo_08", "grupo_08_master_01", "master"), "host": "127.0.0.1", "port": 9001}
    worker = fixture_node("grupo_07", "grupo_07_master_01_worker_01", "worker")
    loan = {"loan_id": str(uuid.uuid4()), "loan_label": "grupo_07_master_01_loan_000001",
            "home_master": home, "target_master": target, "worker": worker, "assignment_epoch": 1}
    return home, target, worker, loan


def test_registration_uses_official_transfer_and_return_messages():
    home, target, worker, loan = refs()
    config = {**worker, "master": target, "home_master": home, "loan_id": loan["loan_id"],
              "assignment_epoch": 1, "_loan_ref": loan}

    class Session:
        def send_message(self, message):
            self.message = message

        def wait_for_message(self, *, request_id, **_):
            return {
                "version": "sd_2026_2_v1", "type": "registration_ack",
                "message_id": str(uuid.uuid4()), "request_id": request_id, "timestamp_ms": int(time.time() * 1000),
                "source": {key: config["master"][key] for key in REQUIRED_NODE_FIELDS}, "destination": worker,
                "payload": {"accepted": True, "reason": None, "assignment_epoch": 1 if config["master"] == target else 2},
            }

    session = Session()
    assert register_worker(target["host"], target["port"], config, control_session=session)
    assert session.message["type"] == "register_temporary_worker"
    assert session.message["payload"] == {"loan": loan}
    assert session.message["source"] == worker

    config["master"] = home
    assert register_worker(home["host"], home["port"], config, control_session=session)
    assert session.message["type"] == "register_worker"
    assert set(session.message["payload"]) == {"home_master", "assignment_epoch", "loan_id"}
    assert session.message["payload"]["loan_id"] == loan["loan_id"]
    assert session.message["payload"]["assignment_epoch"] == 1
    assert config["assignment_epoch"] == 2
    assert config["loan_id"] is None
    assert "_loan_ref" not in config


def test_redirect_rejects_stale_epoch_and_wrong_origin():
    home, target, worker, loan = refs()
    config = {**worker, "master": home, "assignment_epoch": 1, "loan_id": None}
    manager = WorkerSessionManager(home["host"], home["port"], config)
    source = {key: home[key] for key in REQUIRED_NODE_FIELDS}
    stale = build_command_redirect_message({}, source, worker, loan=loan)
    assert manager.process_inbound_message(stale) is False
    wrong = {**loan, "assignment_epoch": 2, "home_master": target, "target_master": home}
    foreign = build_command_redirect_message({}, {key: target[key] for key in REQUIRED_NODE_FIELDS}, worker, loan=wrong)
    assert manager.process_inbound_message(foreign) is False
    assert config["master"] == home


@pytest.mark.parametrize("disconnect_receiver", [False, True])
def test_transfer_and_return_to_origin_over_tcp(disconnect_receiver):
    home, target, worker, loan = refs()
    with socket.socket() as origin, socket.socket() as receiver:
        for listener, identity in ((origin, home), (receiver, target)):
            listener.bind(("127.0.0.1", 0))
            listener.listen()
            listener.settimeout(5)
            identity["port"] = listener.getsockname()[1]

        def registration(conn):
            buffer = bytearray()
            while True:
                messages = recv_msgs(conn, buffer)
                assert messages is not None, "worker closed before registering"
                for message in messages:
                    if message["type"] in {"register_worker", "register_temporary_worker"}:
                        return message

        def acknowledge(conn, message, assignment_epoch):
            send_msg(conn, {
                "version": "sd_2026_2_v1", "type": "registration_ack",
                "message_id": str(uuid.uuid4()), "request_id": message["request_id"],
                "timestamp_ms": int(time.time() * 1000),
                "source": message["destination"], "destination": message["source"],
                "payload": {"accepted": True, "reason": None, "assignment_epoch": assignment_epoch},
            })

        config = {**worker, "master": dict(home), "assignment_epoch": 0, "loan_id": None, "execution_delay_ms": 150}
        manager = WorkerSessionManager(
            home["host"], home["port"], config, retry_delay_factory=lambda _: 0.05,
        )
        thread = manager.start()
        try:
            origin_conn, _ = origin.accept()
            with origin_conn:
                origin_conn.settimeout(5)
                initial = registration(origin_conn)
                acknowledge(origin_conn, initial, 0)
                redirect = build_command_redirect_message({}, initial["destination"], worker, loan=loan)
                send_msg(origin_conn, redirect)
                while origin_conn.recv(4096):
                    pass

            target_conn, _ = receiver.accept()
            with target_conn:
                target_conn.settimeout(5)
                transfer = registration(target_conn)
                assert transfer["type"] == "register_temporary_worker"
                assert transfer["source"] == worker
                assert transfer["payload"] == {"loan": loan}
                acknowledge(target_conn, transfer, 1)
                buffer = bytearray()
                while True:
                    messages = recv_msgs(target_conn, buffer)
                    assert messages is not None
                    request = next((m for m in messages if m["type"] == "task_request"), None)
                    if request is not None:
                        assert request["source"] == worker
                        assert request["payload"]["loan_id"] == loan["loan_id"]
                        break
                if not disconnect_receiver:
                    task_id, attempt_id = str(uuid.uuid4()), str(uuid.uuid4())
                    assignment = build_task_assignment_message(
                        request, transfer["destination"], worker, task_id=task_id,
                        task_label="ct14_task_000001", attempt_id=attempt_id, task_kind="sum_range",
                        parameters={"start": 1, "end": 1000}, assignment_epoch=1, loan_id=loan["loan_id"],
                    )
                    release = build_command_release_message(
                        {}, transfer["destination"], worker, loan_id=loan["loan_id"],
                        assignment_epoch=1, reason="load_normalized",
                    )
                    send_msg(target_conn, assignment)
                    send_msg(target_conn, release)
                    while True:
                        messages = recv_msgs(target_conn, buffer)
                        assert messages is not None, "worker disconnected before reporting its task"
                        result = next((m for m in messages if m["type"] == "task_result"), None)
                        if result is not None:
                            assert result["source"] == worker
                            assert result["payload"]["result"] == {"sum": 500500}
                            assert result["payload"]["loan_id"] == loan["loan_id"]
                            send_msg(target_conn, build_task_ack_message(
                                result, transfer["destination"], worker, task_id=task_id,
                                attempt_id=attempt_id, accepted=True, reason="completed",
                            ))
                            break
                    while target_conn.recv(4096):
                        pass
                # The other case closes the target socket to simulate a receiver failure.

            return_conn, _ = origin.accept()
            with return_conn:
                return_conn.settimeout(5)
                returned = registration(return_conn)
                assert returned["type"] == "register_worker"
                assert returned["source"] == worker
                assert returned["payload"]["loan_id"] == loan["loan_id"]
                assert returned["payload"]["assignment_epoch"] == 1
                acknowledge(return_conn, returned, 2)
                buffer = bytearray()
                while True:
                    messages = recv_msgs(return_conn, buffer)
                    assert messages is not None
                    assert all(m["type"] != "notify_worker_returned" for m in messages)
                    request = next((m for m in messages if m["type"] == "task_request"), None)
                    if request is not None:
                        assert request["payload"]["loan_id"] is None
                        assert request["payload"]["assignment_epoch"] == 2
                        break
        finally:
            manager.stop()
            thread.join(timeout=2)


def test_pending_result_reconciles_on_socket_to_original_master():
    home, target, worker, loan = refs()
    with socket.socket() as receiver:
        receiver.bind(("127.0.0.1", 0))
        receiver.listen()
        receiver.settimeout(3)
        target["port"] = receiver.getsockname()[1]
        config = {**worker, "master": target, "assignment_epoch": 1, "loan_id": loan["loan_id"]}
        manager = WorkerSessionManager(target["host"], target["port"], config)
        task_id, attempt_id = str(uuid.uuid4()), str(uuid.uuid4())
        target_identity = {key: target[key] for key in REQUIRED_NODE_FIELDS}
        result = build_task_result_message(
            {"request_id": str(uuid.uuid4())}, source=worker, destination=target_identity,
            task_id=task_id, task_label="ct14_task_000001", attempt_id=attempt_id,
            assignment_epoch=1, loan_id=loan["loan_id"], status="ok",
            result={"sum": 500500}, error_code=None,
        )
        manager._remember_pending_result(result)
        # The execution session is already at home and its request stays pending.
        config["master"] = home
        manager.task_request_pending = True
        reconciler = threading.Thread(target=manager._reconcile_foreign_results, daemon=True)
        reconciler.start()
        peer, _ = receiver.accept()
        with peer:
            peer.settimeout(3)
            buffer = bytearray()
            messages = []
            while not messages:
                messages = recv_msgs(peer, buffer)
                assert messages is not None
            assert messages == [result]
            ack = build_task_ack_message(
                result, target_identity, worker, task_id=task_id, attempt_id=attempt_id,
                accepted=False, reason="stale_attempt",
            )
            # A mismatched peer identity cannot settle the pending result.
            impostor = {**ack, "source": {**ack["source"], "node_id": str(uuid.uuid4())}}
            send_msg(peer, impostor)
            send_msg(peer, ack)
            reconciler.join(timeout=3)
            assert not reconciler.is_alive()
            assert manager.pending_results == {}
            assert manager.task_request_pending is True
