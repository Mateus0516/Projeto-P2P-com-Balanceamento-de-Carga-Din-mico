import socket
import socketserver
import threading
import time
import uuid
from contextlib import contextmanager

from master import master as farm
from protocol import (
    MasterRegistry, build_confirm_loan_message, build_request_help_message,
    REQUIRED_NODE_FIELDS, build_get_loan_status_message,
    build_notify_worker_returned_message, build_register_temporary_worker_message,
    recv_msgs, send_msg, validate_message,
)

from test_sprint3_dynamic_load import _identity, _loan_ref


def test_ct11_status_request_uses_only_official_loan_id():
    requester = _identity("master", "grupo_07_master_01")
    home = _identity("master", "grupo_07_master_02")
    worker = _identity("worker", "grupo_07_master_02_worker_01")
    loan = _loan_ref(home, requester, worker)
    request = build_get_loan_status_message(requester, home, loan_id=loan["loan_id"])
    assert request["payload"] == {"loan_id": loan["loan_id"]}
    assert validate_message(request)


def test_ct12_temporary_registration_is_from_the_original_worker():
    requester = _identity("master", "grupo_07_master_01")
    home = _identity("master", "grupo_07_master_02")
    worker = _identity("worker", "grupo_07_master_02_worker_01")
    loan = _loan_ref(home, requester, worker)
    loan["worker"] = {key: worker[key] for key in REQUIRED_NODE_FIELDS}
    message = build_register_temporary_worker_message({}, loan["worker"], requester, loan=loan)
    assert message["source"] == loan["worker"]
    assert message["payload"] == {"loan": loan}
    assert validate_message(message)
    assert not validate_message({**message, "source": {**message["source"], "node_id": home["node_id"]}})


class FarmHandler(socketserver.BaseRequestHandler):
    def handle(self):
        farm.handle_client(self.request, self.client_address, *self.server.args)


@contextmanager
def local_masters(tmp_path, *, reservation_timeout_ms=15000):
    with socketserver.ThreadingTCPServer(("127.0.0.1", 0), FarmHandler) as origin, \
         socketserver.ThreadingTCPServer(("127.0.0.1", 0), FarmHandler) as target:
        servers = (origin, target)
        nodes = [
            _identity("master", f"grupo_07_master_{index:02d}", port=server.server_address[1])
            for index, server in enumerate(servers, 1)
        ]
        managers = []
        for index, server in enumerate(servers):
            node = {**nodes[index], "peers": [nodes[1 - index]], "min_home_workers": 0,
                    "response_timeout_ms": 150, "reservation_timeout_ms": reservation_timeout_ms}
            registry = MasterRegistry()
            queue = farm.MasterTaskQueue()
            manager = farm.PeerLoanManager(registry, node, ledger_path=tmp_path / f"ledger-{index}.json", task_queue=queue)
            server.args = (registry, node, queue, manager)
            server.daemon_threads = True
            threading.Thread(target=server.serve_forever, kwargs={"poll_interval": .01}, daemon=True).start()
            managers.append(manager)
        try:
            yield nodes, managers
        finally:
            for server in servers:
                server.shutdown()


def ask_for_worker(home, borrower):
    request = build_request_help_message(
        farm.node_identity(borrower), farm.node_identity(home),
        requester_master=borrower, workers_needed=1, tasks_pending=11,
    )
    return farm.exchange_peer(home, request, {"response_accepted"}, timeout=1)


def confirm_offer(home, borrower, offer):
    loan = offer["payload"]["loan"]
    return build_confirm_loan_message(
        offer, farm.node_identity(borrower), farm.node_identity(home),
        loan_id=loan["loan_id"], assignment_epoch=loan["assignment_epoch"],
    )


def add_idle_worker(manager):
    worker = _identity("worker", "grupo_07_master_01_worker_01")
    manager.registry.upsert_worker(worker)
    return worker


def test_ct10_expired_offer_rejects_confirmation_without_redirect(tmp_path):
    with local_masters(tmp_path, reservation_timeout_ms=25) as ((home, borrower), (origin, _)):
        worker = add_idle_worker(origin)
        offer = ask_for_worker(home, borrower)
        assert offer["payload"]["loan"]["assignment_epoch"] == 1
        time.sleep(.05)
        confirm = confirm_offer(home, borrower, offer)
        with socket.create_connection((home["host"], home["port"]), timeout=1) as conn:
            send_msg(conn, confirm)
            error, = recv_msgs(conn, bytearray())
        assert validate_message(error)
        assert error["payload"]["error_code"] == "reservation_expired"
        assert origin.registry.get_worker(worker["node_id"])["status"] == "available"
        assert not origin.list_committed_loans()


def test_ct11_lost_commit_is_resolved_using_the_origin_ledger(tmp_path, monkeypatch):
    with local_masters(tmp_path) as ((home, borrower), (origin, target)):
        add_idle_worker(origin)
        real_exchange = farm.exchange_peer
        seen = []

        def drop_commit_reply(*args, **kwargs):
            request = args[1]
            seen.append(request["type"])
            response = real_exchange(*args, **kwargs)
            if request["type"] == "confirm_loan":
                raise TimeoutError("loan_committed dropped after committing")
            return response

        monkeypatch.setattr(farm, "exchange_peer", drop_commit_reply)
        farm.negotiate_peer_loans(target.registry, target.task_queue, target.master_identity, target)
        assert seen == ["request_help", "confirm_loan", "get_loan_status"]
        loan, = origin.list_committed_loans()
        mirrored, = target.list_committed_loans()
        assert farm.loan_reference(mirrored) == farm.loan_reference(loan)
        assert origin.registry.get_worker(loan["worker"]["node_id"])["status"] == "committed"


def test_ct12_delayed_registration_and_ct13_duplicate_close(tmp_path):
    with local_masters(tmp_path) as ((home, borrower), (origin, target)):
        worker = farm.node_identity(add_idle_worker(origin))
        offer = ask_for_worker(home, borrower)
        loan = offer["payload"]["loan"]

        def temporary():
            return build_register_temporary_worker_message({}, worker, farm.node_identity(borrower), loan=loan)

        with socket.create_connection((borrower["host"], borrower["port"]), timeout=1) as conn:
            send_msg(conn, temporary())
            error, = recv_msgs(conn, bytearray())
            assert error["type"] == "protocol_error"
            assert not target.registry.active_sessions
            confirm = confirm_offer(home, borrower, offer)
            first = farm.exchange_peer(home, confirm, {"loan_committed"}, timeout=1)
            second = farm.exchange_peer(home, confirm, {"loan_committed"}, timeout=1)
            assert first == second
            send_msg(conn, temporary())
            ack, = recv_msgs(conn, bytearray())
            assert ack["payload"]["accepted"] is True
            assert ack["payload"]["assignment_epoch"] == 1

        returned = {
            "version": "sd_2026_2_v1", "type": "register_worker",
            "message_id": str(uuid.uuid4()), "request_id": str(uuid.uuid4()),
            "timestamp_ms": time.time_ns() // 1_000_000,
            "source": worker, "destination": farm.node_identity(home),
            "payload": {"home_master": home, "loan_id": loan["loan_id"], "assignment_epoch": 1},
        }
        for _ in range(2):
            with socket.create_connection((home["host"], home["port"]), timeout=1) as conn:
                send_msg(conn, returned)
                ack, = recv_msgs(conn, bytearray())
                assert ack["payload"]["accepted"] is True
                assert ack["payload"]["assignment_epoch"] == 2

        notice = build_notify_worker_returned_message(
            {}, farm.node_identity(home), farm.node_identity(borrower),
            loan_id=loan["loan_id"], loan_label=loan["loan_label"], worker=worker,
            assignment_epoch=2,
        )
        for _ in range(2):
            ack = farm.exchange_peer(borrower, notice, {"return_ack"}, timeout=1)
            assert ack["payload"] == {"loan_id": loan["loan_id"], "loan_state": "closed"}
        assert origin.get_loan_status(loan["loan_id"])["assignment_epoch"] == 2
        assert target.get_loan_status(loan["loan_id"])["assignment_epoch"] == 2



def test_ct11_redirect_is_sent_even_when_origin_cannot_send_commit_reply(tmp_path, monkeypatch):
    with local_masters(tmp_path) as ((home, borrower), (origin, _)):
        worker = farm.node_identity(add_idle_worker(origin))
        registration = {
            "version": "sd_2026_2_v1", "type": "register_worker",
            "message_id": str(uuid.uuid4()), "request_id": str(uuid.uuid4()),
            "timestamp_ms": time.time_ns() // 1_000_000,
            "source": worker, "destination": farm.node_identity(home),
            "payload": {"home_master": home, "loan_id": None, "assignment_epoch": 0},
        }
        with socket.create_connection((home["host"], home["port"]), timeout=1) as worker_conn:
            send_msg(worker_conn, registration)
            ack, = recv_msgs(worker_conn, bytearray())
            assert ack["payload"]["accepted"]

            def fail_commit_reply(conn, message):
                if message["type"] == "loan_committed":
                    raise ConnectionError("commit reply was lost")
                send_msg(conn, message)

            monkeypatch.setattr(farm, "send_msg", fail_commit_reply)
            offer = ask_for_worker(home, borrower)
            confirm = confirm_offer(home, borrower, offer)
            with socket.create_connection((home["host"], home["port"]), timeout=1) as peer_conn:
                send_msg(peer_conn, confirm)
                assert recv_msgs(peer_conn, bytearray()) is None

            redirect, = recv_msgs(worker_conn, bytearray())
            assert redirect["type"] == "command_redirect"
            assert redirect["payload"]["loan"] == offer["payload"]["loan"]
