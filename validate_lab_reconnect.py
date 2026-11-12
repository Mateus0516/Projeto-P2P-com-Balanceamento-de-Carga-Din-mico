import json
import socket
import time
import uuid

from protocol import validate_message

MASTER_HOST = "127.0.0.1"
MASTER_PORT = 9000
MASTER_NODE_ID = "cf662f72-9291-48fd-bc0e-591eea76e6b1"
GROUP_LABEL = "p2p-cluster"


def register(node_id: str, label: str, port: int):
    msg = {
        "version": "sd_2026_2_v1",
        "type": "register_worker",
        "message_id": str(uuid.uuid4()),
        "request_id": str(uuid.uuid4()),
        "timestamp_ms": int(time.time() * 1000),
        "source": {
            "group_label": GROUP_LABEL,
            "node_id": node_id,
            "node_label": label,
            "node_role": "worker",
        },
        "destination": {
            "group_label": GROUP_LABEL,
            "node_id": MASTER_NODE_ID,
            "node_label": "master",
            "node_role": "master",
        },
        "payload": {
            "host": "127.0.0.1",
            "port": port,
            "home_master": {
                "group_label": GROUP_LABEL,
                "node_id": MASTER_NODE_ID,
                "node_label": "master",
                "node_role": "master",
                "host": MASTER_HOST,
                "port": MASTER_PORT,
            },
            "assignment_epoch": 0,
            "loan_id": None,
        },
    }
    if not validate_message(msg):
        raise ValueError("Mensagem de registro incompatível com o protocolo")
    encoded = (json.dumps(msg, separators=(",", ":")) + "\n").encode("utf-8")
    with socket.create_connection((MASTER_HOST, MASTER_PORT), timeout=5) as sock:
        sock.settimeout(5)
        sock.sendall(encoded)
        try:
            reply = sock.recv(4096)
            if reply:
                print("ACK", reply.decode("utf-8"))
            else:
                print("CONNECTION_CLOSED")
        except socket.timeout:
            print("ACK_TIMEOUT")


if __name__ == "__main__":
    bad = b'{"broken":\n'
    with socket.create_connection((MASTER_HOST, MASTER_PORT), timeout=5) as sock:
        sock.sendall(bad)
        time.sleep(0.2)
    print("INVALID_JSON_SENT")

    shared_node_id = str(uuid.uuid4())
    register(shared_node_id, "worker-a", 9102)
    register(shared_node_id, "worker-a", 9103)
