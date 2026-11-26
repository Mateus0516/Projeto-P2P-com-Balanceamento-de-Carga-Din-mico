"""Validar a configuração e o contrato TCP dos peers cadastrados."""
import argparse
import json
import uuid
from pathlib import Path

from master.master import exchange_peer, node_identity
from protocol import build_get_loan_status_message, validate_official_node


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--config", type=Path, required=True)
    args = parser.parse_args()
    master = json.loads(args.config.read_text(encoding="utf-8-sig"))
    if not validate_official_node(master, "master", address=True):
        parser.error("identidade ou endereço do Master inválido")
    peers = master.get("peers")
    if not isinstance(peers, list) or not peers:
        parser.error("configure ao menos um peer em peers")
    timeout = master.get("response_timeout_ms", 5000) / 1000
    failed = False
    seen = {master["node_id"]}
    for peer in peers:
        if not validate_official_node(peer, "master", address=True) or peer["node_id"] in seen:
            print("ERRO: identidade, endereço ou UUID duplicado em peers")
            failed = True
            continue
        seen.add(peer["node_id"])
        loan_id = str(uuid.uuid4())
        request = build_get_loan_status_message(node_identity(master), node_identity(peer), loan_id=loan_id)
        try:
            response = exchange_peer(peer, request, {"loan_status"}, timeout=timeout, attempts=1)
            if response["payload"] != {"loan_id": loan_id, "loan_state": "unknown", "loan": None}:
                raise ValueError("loan_status não confirma a consulta de teste")
            print(f'OK {peer["node_label"]} {peer["host"]}:{peer["port"]} {response["version"]}')
        except (OSError, ValueError) as exc:
            print(f'ERRO {peer["node_label"]} {peer["host"]}:{peer["port"]}: {exc}')
            failed = True
    if failed:
        raise SystemExit(1)


if __name__ == "__main__":
    main()
