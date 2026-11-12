import json
import socket
import threading
import time
import uuid

from protocol import extract_complete_messages, validate_message

MASTER_ID = str(uuid.uuid4())
WORKER_ID = str(uuid.uuid4())


def make_message(msg_id, payload):
    master = {
        "group_label": "p2p-cluster",
        "node_id": MASTER_ID,
        "node_label": "master",
        "node_role": "master",
    }
    worker = {
        "group_label": "p2p-cluster",
        "node_id": WORKER_ID,
        "node_label": "worker-demo",
        "node_role": "worker",
    }
    return {
        "version": "sd_2026_2_v1",
        "type": "register_worker",
        "message_id": str(uuid.uuid4()),
        "request_id": str(uuid.uuid4()),
        "timestamp_ms": int(time.time() * 1000),
        "source": worker,
        "destination": master,
        "payload": {
            "home_master": master,
            "assignment_epoch": 0,
            "loan_id": None,
            "host": "127.0.0.1",
            "port": 9101,
            "demo_label": msg_id,
            **payload,
        },
    }


def run_receiver(port):
    server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    server.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    server.bind(("127.0.0.1", port))
    server.listen(1)

    result = {"messages": []}

    def server_main():
        conn, _ = server.accept()
        try:
            buffer = bytearray()
            conn.settimeout(5)
            deadline = time.time() + 5
            while time.time() < deadline and len(result["messages"]) < 4:
                try:
                    chunk = conn.recv(4096)
                except socket.timeout:
                    break
                if not chunk:
                    break
                buffer.extend(chunk)
                packets = extract_complete_messages(buffer)
                for pkt in packets:
                    if validate_message(pkt):
                        result["messages"].append(pkt)
            print("RECEIVED_MESSAGES", len(result["messages"]))
            for item in result["messages"]:
                print(item["payload"]["demo_label"])
        finally:
            conn.close()
            server.close()

    thread = threading.Thread(target=server_main, daemon=True)
    thread.start()
    return result, thread


def main():
    port = 19200
    result, thread = run_receiver(port)
    time.sleep(0.2)

    with socket.create_connection(("127.0.0.1", port), timeout=5) as client:
        large = make_message("m-frag", {"status": "online", "demo_data": "x" * 2000})
        encoded = (json.dumps(large, separators=(",", ":")) + "\n").encode("utf-8")
        client.sendall(encoded[:120])
        time.sleep(0.1)
        client.sendall(encoded[120:])

        messages = [
            make_message("m1", {"status": "online"}),
            make_message("m2", {"status": "ready"}),
            make_message("m3", {"status": "busy"}),
        ]
        batched = b"".join((json.dumps(msg, separators=(",", ":")) + "\n").encode("utf-8") for msg in messages)
        client.sendall(batched)
        client.shutdown(socket.SHUT_WR)

    thread.join(timeout=6)
    print("FINAL_COUNT", len(result["messages"]))
    print("EXPECTED", 4)
    if len(result["messages"]) != 4:
        raise SystemExit("Demo failed: expected four valid messages")


if __name__ == "__main__":
    main()
