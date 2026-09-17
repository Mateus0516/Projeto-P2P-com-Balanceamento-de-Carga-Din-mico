import json
import socket
import time


def register(uuid_value: str, label: str, port: int):
    msg = {
        "type": "register_worker",
        "msg_id": f"dup-{uuid_value}-{port}",
        "request_id": f"req-{uuid_value}-{port}",
        "origin": {"uuid": uuid_value, "label": label},
        "timestamp": "2026-09-17T10:00:00Z",
        "payload": {"host": "127.0.0.1", "port": port},
    }
    encoded = (json.dumps(msg, separators=(",", ":")) + "\n").encode("utf-8")
    s = socket.create_connection(("127.0.0.1", 9000), timeout=5)
    s.sendall(encoded)
    time.sleep(0.3)
    try:
        reply = s.recv(4096)
        if reply:
            print("ACK", reply.decode("utf-8"))
    except socket.timeout:
        print("ACK_TIMEOUT")
    finally:
        s.close()


if __name__ == "__main__":
    bad = ('{"broken":' + chr(10)).encode("utf-8")
    s = socket.create_connection(("127.0.0.1", 9000), timeout=5)
    s.sendall(bad)
    time.sleep(0.2)
    s.close()
    print("INVALID_JSON_SENT")

    register("worker-a", "worker-a", 9102)
    register("worker-a", "worker-a", 9103)
