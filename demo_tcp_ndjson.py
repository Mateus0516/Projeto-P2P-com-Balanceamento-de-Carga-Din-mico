import json
import socket
import threading
import time

from protocol import extract_complete_messages, validate_message


def make_message(msg_id, payload):
    return {
        "type": "register_worker",
        "msg_id": msg_id,
        "request_id": f"req-{msg_id}",
        "origin": {"uuid": f"uuid-{msg_id}", "label": f"worker-{msg_id}"},
        "timestamp": "2026-09-17T10:00:00Z",
        "payload": payload,
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
            deadline = time.time() + 5
            while time.time() < deadline and len(result["messages"]) < 4:
                chunk = conn.recv(4096)
                if not chunk:
                    break
                buffer.extend(chunk)
                packets = extract_complete_messages(buffer)
                if packets:
                    for pkt in packets:
                        if validate_message(pkt):
                            result["messages"].append(pkt)
            print("RECEIVED_MESSAGES", len(result["messages"]))
            for item in result["messages"]:
                print(item["msg_id"])
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

    client = socket.create_connection(("127.0.0.1", port), timeout=5)
    large = make_message("m-frag", {"status": "online", "payload": "x" * 2000})
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
    client.close()

    thread.join(timeout=5)
    print("FINAL_COUNT", len(result["messages"]))
    print("EXPECTED", 4)


if __name__ == "__main__":
    main()
