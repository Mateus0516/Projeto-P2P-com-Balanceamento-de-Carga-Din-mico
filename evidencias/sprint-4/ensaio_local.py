"""Ensaios locais do perfil CT_14, com processos independentes do grupo 07."""
import argparse
from contextlib import ExitStack
import hashlib
import json
from datetime import datetime
from pathlib import Path
import re
import shutil
import socket
import subprocess
import sys
import tempfile
import time
import uuid

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT))

from client.client import TaskClient
from client.load_generator import build_load_plan
from protocol import (
    REQUIRED_NODE_FIELDS,
    build_get_loan_status_message,
    build_request_help_message,
    recv_msgs,
    send_msg,
    validate_message,
)

LOOPBACK = "127.0.0.1"
WORKERS = ("a", "a2", "b", "b2")
SNAPSHOT_PATTERN = re.compile(r"MASTER SNAPSHOT total=(\d+) completed=(\d+) pending=(\d+) running=(\d+) failed=(\d+)")
SNAPSHOT_FIELDS = ("total", "completed", "pending", "running", "failed")


def identity(node):
    return {key: node[key] for key in REQUIRED_NODE_FIELDS}


def address(node):
    return {**identity(node), "host": node["host"], "port": node["port"]}


def read_text(path):
    if path.exists():
        return path.read_text(encoding="utf-8", errors="replace")
    return ""


def write_json(path, value):
    path.write_text(json.dumps(value, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")


def read_json_lines(path):
    content = read_text(path)
    return [json.loads(line) for line in content.splitlines(keepends=True) if line.endswith("\n")]


def load_config(relative):
    return json.loads((ROOT / relative).read_text(encoding="utf-8-sig"))


def reserve_ports(count):
    with ExitStack() as stack:
        listeners = [stack.enter_context(socket.socket()) for _ in range(count)]
        for listener in listeners:
            listener.bind((LOOPBACK, 0))
        return [listener.getsockname()[1] for listener in listeners]


def stop_process(process):
    if process.poll() is None:
        process.terminate()
        try:
            process.wait(timeout=5)
        except subprocess.TimeoutExpired:
            process.kill()
            process.wait(timeout=5)


def launch(stack, cwd, log, *arguments):
    with log.open("xb") as handle:
        process = subprocess.Popen(
            [sys.executable, "-B", "-u", *map(str, arguments)],
            cwd=cwd,
            stdout=handle,
            stderr=subprocess.STDOUT,
        )
    stack.callback(stop_process, process)
    return process


def wait_for(check, processes, timeout, description):
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        for label, process in processes.items():
            if process.poll() is not None:
                raise RuntimeError(f"{label} encerrou com código {process.returncode}")
        result = check()
        if result:
            return result
        time.sleep(0.1)
    raise TimeoutError(description)


def can_connect(port):
    try:
        with socket.create_connection((LOOPBACK, port), timeout=0.25):
            return True
    except OSError:
        return False


def read_snapshots(log):
    return [dict(zip(SNAPSHOT_FIELDS, map(int, match.groups())))
            for match in SNAPSHOT_PATTERN.finditer(read_text(log))]


def ledger_states(output, label):
    path = output / "ledgers" / (label + ".jsonl")
    return read_json_lines(path)


class TrialClient(TaskClient):
    def __init__(self, master):
        super().__init__(host=master["host"], port=master["port"],
                         group_label=master["group_label"], node_label="load_generator")
        self.master = master

    def _master_identity(self):
        return identity(self.master)


def submit_tasks(master, plan, evidence):
    client = TrialClient(master)
    started = time.monotonic()
    with evidence.open("x", encoding="utf-8") as output:
        for task in plan:
            due = started + task["scheduled_after_ms"] / 1000
            time.sleep(max(0, due - time.monotonic()))
            response = client.submit_task(task["task_id"], **task["parameters"])
            output.write(json.dumps(response, ensure_ascii=False) + "\n")
            if response["payload"].get("accepted") is not True:
                raise RuntimeError(f"Submissão rejeitada: {response!r}")
    return started


def evidence_report(output, masters, active, mode, elapsed, plan, unavailable):
    accepted = read_json_lines(output / "collector" / "reports.jsonl")
    rejected = read_json_lines(output / "collector" / "rejected.jsonl")
    metrics = []
    for item in accepted:
        message = item["message"]
        payload = message["payload"]
        metrics.append({
            "master": message["source"]["node_label"],
            "timestamp_ms": message["timestamp_ms"],
            "uptime_seconds": payload["uptime_seconds"],
            **{key: value for key, value in payload.items() if key.startswith(("tasks_", "workers_"))},
            "loans": payload["loans"],
            "neighbors": payload["neighbors"],
        })
    donor = masters["b" if active == "a" else "a"]
    donor_suffixes = ("b", "b2") if active == "a" else ("a", "a2")
    active_label = masters[active]["node_label"]
    main_log = read_text(output / f"master-{active}.log")
    foreign_task_acks = {}
    for suffix in donor_suffixes:
        worker_label = donor["node_label"] + ("_worker_02" if suffix.endswith("2") else "_worker_01")
        worker_log = read_text(output / f"worker-{suffix}.log")
        from_active = set(re.findall(
            rf"\| {re.escape(active_label)} \| {re.escape(worker_label)} \| task_ack \| ([0-9a-f-]+) \| received",
            worker_log,
        ))
        worker_completed = set(re.findall(
            rf"\| {re.escape(worker_label)} \| master \| task_ack \| ([0-9a-f-]+) \| completed",
            worker_log,
        ))
        sent_by_active = set(re.findall(
            rf"\| master \| {re.escape(worker_label)} \| task_ack \| ([0-9a-f-]+) \| sent",
            main_log,
        ))
        foreign_task_acks[worker_label] = sorted(from_active & worker_completed & sent_by_active)
    snapshots = read_snapshots(output / f"master-{active}.log")
    return {
        "mode": mode,
        "active_master": masters[active]["node_label"],
        "tasks": len(plan),
        "elapsed_seconds": round(elapsed, 3),
        "peak_pending_snapshot": max(item["pending"] for item in snapshots),
        "collector_accepted": len(accepted),
        "collector_rejected": len(rejected),
        "foreign_result_accepted_log_count": sum(map(len, foreign_task_acks.values())),
        "foreign_task_ack_request_ids": foreign_task_acks,
        "ledgers": {name: ledger_states(output, node["node_label"]) for name, node in masters.items()},
        "samples": metrics,
        "unavailable_probe": unavailable,
    }


def peer_exchange(request, peer):
    with socket.create_connection((peer["host"], peer["port"]), timeout=5) as sock:
        send_msg(sock, request)
        buffer = bytearray()
        while True:
            messages = recv_msgs(sock, buffer)
            if messages is None:
                raise RuntimeError("Peer encerrou a conexão antes da resposta")
            for reply in messages:
                if reply.get("request_id") != request["request_id"]:
                    continue
                if (
                    not validate_message(reply)
                    or reply["source"] != request["destination"]
                    or reply["destination"] != request["source"]
                ):
                    raise RuntimeError(f"Resposta incompatível na sonda: {reply!r}")
                return reply


def preflight(masters, output):
    checks = []
    for sender, receiver in (("a", "b"), ("b", "a")):
        node, peer = masters[sender], masters[receiver]
        loan_id = str(uuid.uuid4())
        request = build_get_loan_status_message(identity(node), identity(peer), loan_id=loan_id)
        reply = peer_exchange(request, peer)
        checks.append({"request": request, "response": reply})
        if reply["type"] != "loan_status" or reply["payload"] != {
            "loan_id": loan_id, "loan_state": "unknown", "loan": None,
        }:
            raise RuntimeError(f"Peer respondeu com status inesperado na verificação inicial: {reply!r}")
    write_json(output / "preflight.json", checks)


def low_level_request_help(borrower, donor, output):
    request = build_request_help_message(
        identity(borrower), identity(donor), requester_master=address(borrower),
        workers_needed=1, tasks_pending=10,
    )
    reply = peer_exchange(request, donor)
    write_json(output / "sonda-recusa.json", {"request": request, "response": reply})
    return reply


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("mode", choices=["sem-ajuda", "com-ajuda"])
    parser.add_argument("--ativo", choices=["a", "b"], default="a")
    parser.add_argument("--seed", type=int, default=44)
    parser.add_argument("--sondas", action="store_true",
                        help="sondar recusa durante saturação e interromper o peer depois do ciclo")
    args = parser.parse_args()
    if args.sondas and args.mode != "com-ajuda":
        parser.error("--sondas requer com-ajuda")

    stamp = datetime.now().astimezone().strftime("%Y%m%d-%H%M%S-%f")
    output = Path(__file__).resolve().parent / f"local-{args.mode}-{args.ativo}-{stamp}"
    output.mkdir()
    (output / "configs").mkdir()
    (output / "ledgers").mkdir()
    print(f"Evidência em {output}", flush=True)
    port_a, port_b, collector_port = reserve_ports(3)
    master_ports = {"a": port_a, "b": port_b}
    masters = {name: load_config(f"master/master-{name}.json") for name in "ab"}
    for name, node in masters.items():
        node.update(host=LOOPBACK, port=master_ports[name], collector_host=LOOPBACK,
                    collector_port=collector_port, min_home_workers=1, saturation_threshold=10,
                    release_threshold=3, stable_samples=3, monitor_interval_ms=1000,
                    negotiation_cooldown_ms=5000, reservation_timeout_ms=15000,
                    response_timeout_ms=5000, performance_interval_seconds=10,
                    loan_ledger_path=str(output / "ledgers" / (node["node_label"] + ".json")))
    for name, node in masters.items():
        other = "b" if name == "a" else "a"
        node["peers"] = [address(masters[other])] if args.mode == "com-ajuda" else []
        write_json(output / "configs" / f"master-{name}.json", node)

    plan = build_load_plan(task_count=200, start=1, end=1000, interval_ms=20, seed=args.seed)
    write_json(output / "carga.json", plan)
    source_paths = ("master/master.py", "worker/worker.py", "protocol.py")
    sources = {path: (ROOT / path).read_bytes() for path in source_paths}
    write_json(output / "parametros.json", {
        "base_git": subprocess.check_output(["git", "rev-parse", "HEAD"], cwd=ROOT, text=True).strip(),
        "sources_sha256": {path: hashlib.sha256(content).hexdigest() for path, content in sources.items()},
        "same_group_loopback": True,
        "mode": args.mode,
        "seed": args.seed,
        "active_master": args.ativo,
        "collector_port": collector_port,
    })

    # Snapshot isolado do código, inclusive para versões anteriores a loan_ledger_path.
    with tempfile.TemporaryDirectory(prefix="ct14-") as temporary:
        source = Path(temporary)
        for relative, content in sources.items():
            copy = source / relative
            copy.parent.mkdir(parents=True, exist_ok=True)
            copy.write_bytes(content)
        with ExitStack() as stack:
            processes = {}
            collector_script = ROOT / "docs" / "referencias-professor" / "collector.py"
            processes["collector"] = launch(
                stack, source, output / "collector.log", collector_script,
                "--host", LOOPBACK, "--port", collector_port, "--grupo", "07",
                "--data-dir", output / "collector",
            )
            wait_for(lambda: can_connect(collector_port), processes, 10, "Collector indisponível")

            for name in masters:
                processes[f"master-{name}"] = launch(
                    stack, source, output / f"master-{name}.log", "master/master.py",
                    "--config", output / "configs" / f"master-{name}.json",
                    "--snapshot-interval-seconds", "1",
                )
            wait_for(lambda: all(can_connect(p) for p in master_ports.values()),
                     processes, 10, "Masters indisponíveis")

            if args.mode == "com-ajuda":
                preflight(masters, output)

            for suffix in WORKERS:
                config = load_config(f"worker/worker-{suffix}.json")
                node = masters[suffix[0]]
                config.update(master=address(node), assignment_epoch=0, loan_id=None, execution_delay_ms=1000)
                config.pop("home_master", None)
                path = output / "configs" / f"worker-{suffix}.json"
                write_json(path, config)
                processes[f"worker-{suffix}"] = launch(
                    stack, source, output / f"worker-{suffix}.log",
                    "worker/worker.py", "--config", path,
                )
            for suffix in WORKERS:
                wait_for(lambda: "| registration_ack |" in read_text(output / f"worker-{suffix}.log"),
                         processes, 20, f"Worker {suffix} não levantou")

            reports_path = output / "collector" / "reports.jsonl"
            wait_for(lambda: len({item["message"]["source"]["node_id"] for item in read_json_lines(reports_path)
                                  if item["message"]["payload"]["workers_active_here"] == 2}) == 2,
                     processes, 20, "Faltou amostra inicial com os dois workers por Master")
            active_log = output / f"master-{args.ativo}.log"
            active = masters[args.ativo]
            other = "b" if args.ativo == "a" else "a"
            started = submit_tasks(active, plan, output / "task_submit_ack.jsonl")
            if args.sondas:
                reply = low_level_request_help(masters[other], active, output)
                if reply["type"] != "response_rejected" or reply["payload"]["reason"] not in {"high_load", "no_workers_available"}:
                    raise RuntimeError(f"Recusa de saturação não observada: {reply!r}")

            wait_for(lambda: bool(read_snapshots(active_log)) and read_snapshots(active_log)[-1]["completed"] == 200,
                     processes, 300, "As 200 tarefas não foram concluídas")
            elapsed = time.monotonic() - started

            for path in (source / "master").glob("loan_ledger_*.json*"):
                suffix = path.name.removeprefix("loan_ledger_")
                target = output / "ledgers" / suffix
                if not target.exists():
                    shutil.copy2(path, target)

            if args.mode == "com-ajuda":
                def closed_in_both():
                    ids = [{item["loan_id"] for item in ledger_states(output, node["node_label"])
                            if item["loan_state"] == "closed"} for node in masters.values()]
                    return ids[0] & ids[1]
                wait_for(closed_in_both, processes, 35, "Retorno ausente nos dois ledgers")

            initial_count = len(read_json_lines(reports_path))
            wait_for(lambda: len(read_json_lines(reports_path)) >= initial_count + 2,
                     processes, 25, "Faltaram amostras finais")
            report = evidence_report(output, masters, args.ativo, args.mode, elapsed, plan, None)
            write_json(output / "resultado.json", report)
            print(f"Concluídas 200 tarefas em {elapsed:.3f} s; Collector aceitou {report['collector_accepted']}.",
                  flush=True)
            if args.mode == "com-ajuda" and report["foreign_result_accepted_log_count"] == 0:
                raise RuntimeError("Nenhum task_ack aceito para worker cedido foi correlacionado")
            if report["collector_rejected"]:
                raise RuntimeError("Collector rejeitou relatórios; consulte o log e rejected.jsonl")

            if args.sondas:
                outage_started_ms = time.time_ns() // 1_000_000
                error_count_before = read_text(active_log).count("peer_error")
                stop_process(processes.pop(f"master-{other}"))
                for suffix in WORKERS:
                    if suffix.startswith(other):
                        stop_process(processes.pop(f"worker-{suffix}"))
                failure_plan = build_load_plan(task_count=30, start=1, end=1000,
                                               interval_ms=20, seed=args.seed + 1)
                submit_tasks(active, failure_plan, output / "task_submit_ack-peer-indisponivel.jsonl")
                wait_for(lambda: read_text(active_log).count("peer_error") > error_count_before,
                         processes, 25,
                         "Falha de conexão ao peer não observada")
                wait_for(lambda: bool(read_snapshots(active_log)) and read_snapshots(active_log)[-1]["completed"] == 200 + len(failure_plan),
                         processes, 35, "Farm não concluiu carga com peer indisponível")
                def unavailable_sample():
                    for record in reversed(read_json_lines(reports_path)):
                        message = record["message"]
                        if message["source"]["node_id"] != active["node_id"] or message["timestamp_ms"] < outage_started_ms:
                            continue
                        for neighbor in message["payload"]["neighbors"]:
                            if neighbor["node"]["node_id"] == masters[other]["node_id"] and neighbor["status"] == "unavailable":
                                return message["message_id"]
                    return None

                sample_id = wait_for(unavailable_sample, processes, 25,
                                     "Coletor não observou peer indisponível")
                unavailable = {
                    **read_snapshots(active_log)[-1],
                    "outage_started_ms": outage_started_ms,
                    "message_id": sample_id,
                }
                if read_json_lines(output / "collector" / "rejected.jsonl"):
                    raise RuntimeError("Collector rejeitou relatório durante a sonda")
                report["unavailable_probe"] = unavailable
                write_json(output / "resultado.json", report)
                print(f"Sonda de indisponibilidade concluiu {len(failure_plan)} tarefas adicionais.", flush=True)


if __name__ == "__main__":
    main()
