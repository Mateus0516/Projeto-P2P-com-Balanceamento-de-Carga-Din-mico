"""Capture a loopback run with the professor's Collector and four real workers."""
import json
import runpy
import socket
import socketserver
import sys
import threading
import time
from contextlib import ExitStack
from datetime import datetime
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT))

from master.master import (
    MasterTaskQueue, PeerLoanManager, handle_client,
    start_master_performance_report_loop,
)
from protocol import (
    REQUIRED_NODE_FIELDS, MasterRegistry, build_confirm_loan_message,
    build_request_help_message, recv_msgs, send_msg, validate_message,
)
from worker.worker import run_worker


def start_server(stack, server):
    stack.callback(server.server_close)
    threading.Thread(target=server.serve_forever, daemon=True).start()
    stack.callback(server.shutdown)
    return server


def wait_until(condition, timeout=20):
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        result = condition()
        if result:
            return result
        time.sleep(0.05)
    raise TimeoutError(condition.__name__)


def identity(node):
    return {key: node[key] for key in REQUIRED_NODE_FIELDS}


def load(relative):
    return json.loads((ROOT / relative).read_text(encoding="utf-8"))


class MasterHandler(socketserver.BaseRequestHandler):
    def handle(self):
        handle_client(self.request, self.client_address, *self.server.master_args)


class CaptureHandler(socketserver.BaseRequestHandler):
    def handle(self):
        with socket.create_connection(self.server.upstream, timeout=3) as collector:
            with self.request.makefile("rb") as incoming:
                for line in incoming:
                    with self.server.capture_lock:
                        with self.server.capture_file.open("ab") as capture:
                            capture.write(line)
                    collector.sendall(line)


def exchange(conn, message, expected):
    send_msg(conn, message)
    buffer = bytearray()
    while True:
        responses = recv_msgs(conn, buffer)
        if responses is None:
            raise EOFError(expected)
        for item in responses:
            if item["request_id"] == message["request_id"]:
                assert validate_message(item) and item["type"] == expected, item
                return item


def main():
    output = Path(__file__).resolve().parent / datetime.now().strftime("loopback-%Y%m%d-%H%M%S")
    output.mkdir()
    print(f"Evidence directory {output}", flush=True)
    reference = runpy.run_path(str(ROOT / "docs/referencias-professor/collector.py"))
    stop = threading.Event()
    with ExitStack() as stack:
        collector = start_server(stack, reference["make_server"]("127.0.0.1", 0, output, ["grupo_07"]))
        relay = socketserver.ThreadingTCPServer(("127.0.0.1", 0), CaptureHandler)
        relay.upstream = collector.server_address
        relay.capture_file = output / "wire.ndjson"
        relay.capture_lock = threading.Lock()
        start_server(stack, relay)

        masters = [load(f"master/master-{suffix}.json") for suffix in ("a", "b")]
        servers = [socketserver.ThreadingTCPServer(("127.0.0.1", 0), MasterHandler) for _ in masters]
        for node, server in zip(masters, servers):
            node.update(host="127.0.0.1", port=server.server_address[1],
                        collector_host="127.0.0.1", collector_port=relay.server_address[1])
        managers = []
        for index, (node, server) in enumerate(zip(masters, servers)):
            peer = masters[1-index]
            node["peers"] = [{**identity(peer), "host": peer["host"], "port": peer["port"]}]
            registry, queue = MasterRegistry(), MasterTaskQueue()
            manager = PeerLoanManager(registry, node, ledger_path=output / (node["node_label"] + ".json"), task_queue=queue)
            managers.append(manager)
            server.master_args = registry, node, queue, manager
            server.daemon_threads = True
            start_server(stack, server)
            start_master_performance_report_loop(registry, queue, node, interval_seconds=10, stop_event=stop, peer_manager=manager)
        stack.callback(stop.set)

        for index, filename in enumerate(("worker-a.json", "worker-a2.json", "worker-b.json", "worker-b2.json")):
            config = load(f"worker/{filename}")
            node = masters[index // 2]
            config.update(master={**identity(node), "host": node["host"], "port": node["port"]},
                          assignment_epoch=0, loan_id=None)
            threading.Thread(target=run_worker, args=(node["host"], node["port"], config, stop),
                             name=config["node_label"], daemon=True).start()

        def all_registered():
            return all(len(manager.registry.active_sessions) == 2 for manager in managers)

        def reports():
            with collector.lock:
                path = output / "reports.jsonl"
                return [json.loads(line)["message"] for line in path.read_text(encoding="utf-8").splitlines()] if path.exists() else []

        def matching(node, condition):
            return next((report for report in reports()
                         if report["source"]["node_id"] == node["node_id"] and condition(report["payload"])), None)

        wait_until(all_registered)
        home, target = masters
        home_manager, target_manager = managers
        requester = {**identity(target), "host": target["host"], "port": target["port"]}
        with socket.create_connection((home["host"], home["port"]), timeout=14) as conn:
            request = build_request_help_message(
                identity(target), identity(home), requester_master=requester, workers_needed=1, tasks_pending=10,
            )
            offered = exchange(conn, request, "response_accepted")
            target_manager.observe_peer_message(offered)
            loan = offered["payload"]["loan"]
            loan_id = loan["loan_id"]
            print(f"offer loan_id={loan_id} request_id={request['request_id']}")
            reserved = wait_until(lambda: matching(home, lambda p: p["workers_reserved"] > 0), timeout=12)
            confirm = build_confirm_loan_message(
                offered, identity(target), identity(home), loan_id=loan_id,
                requester_master=requester, target_master=requester,
                home_master=loan["home_master"], worker=loan["worker"],
                assignment_epoch=loan["assignment_epoch"],
            )
            committed_message = exchange(conn, confirm, "loan_committed")
            target_manager.observe_peer_message(committed_message)
            loan = committed_message["payload"]["loan"]
            result = target_manager.mirror_committed_loan(
                loan_id=loan_id, loan_label=loan["loan_label"], requester_master=requester,
                target_master=loan["target_master"], home_master=loan["home_master"],
                worker=loan["worker"], assignment_epoch=loan["assignment_epoch"],
            )
            assert result["status"] == "accepted", result
            print(f"commit loan_id={loan_id} request_id={confirm['request_id']}")

        lent = wait_until(lambda: matching(home, lambda p: p["workers_lent"] > 0))
        received = wait_until(lambda: matching(target, lambda p: p["workers_received"] > 0))
        result = target_manager.issue_release_command(loan_id)
        assert result["status"] == "accepted", result
        print(f"release loan_id={loan_id} request_id={result['message']['request_id']}")

        def closed():
            return all(manager.get_loan_status(loan_id)["loan_state"] == "closed" for manager in managers)

        wait_until(closed)
        wait_until(lambda: all(sum(report["source"]["node_id"] == node["node_id"] for report in reports()) >= 3
                               for node in masters))
        stop.set()
        messages = reports()
        assert not (output / "rejected.jsonl").exists()
        originals = [json.loads(line) for line in (output / "wire.ndjson").read_bytes().splitlines()]
        assert {report["message_id"]: report for report in originals} == {report["message_id"]: report for report in messages}
        for node in masters:
            own = sorted((message for message in messages if message["source"]["node_id"] == node["node_id"]),
                         key=lambda message: message["timestamp_ms"])
            assert len(own) >= 3
            assert all(9500 <= b["timestamp_ms"] - a["timestamp_ms"] <= 11000 for a, b in zip(own, own[1:]))
            assert own[-1]["payload"]["loans"] == []
            assert own[-1]["payload"]["neighbors"][0]["status"] == "available"
        for report, state in ((reserved, "offered"), (lent, "committed"), (received, "active")):
            assert [(entry["loan_id"], entry["loan_state"]) for entry in report["payload"]["loans"]] == [(loan_id, state)]
            print(f"{state} loan_id={loan_id} message_id={report['message_id']}")
        print(f"Accepted {len(messages)} original reports; dual ledger closure confirmed.", flush=True)


if __name__ == "__main__":
    main()
