import socket
import time
import threading

def send_recv(s, data):
    # print(f"> {data.strip()}")
    s.sendall(data)
    response = s.recv(4096)
    # print(f"< {response.decode().strip()}")
    return response.decode()

def test_basic_transaction():
    print("Testing Basic Transaction (MULTI/EXEC)...")
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    s.connect(("127.0.0.1", 6379))
    
    send_recv(s, b"DEL txn_key\r\n")
    assert "OK" in send_recv(s, b"MULTI\r\n")
    assert "QUEUED" in send_recv(s, b"SET txn_key txn_val\r\n")
    assert "QUEUED" in send_recv(s, b"GET txn_key\r\n")
    
    resp = send_recv(s, b"EXEC\r\n")
    # Expect Array with OK and "txn_val"
    if "*2" in resp and "OK" in resp and "txn_val" in resp:
        print("PASS: Basic Transaction Executed")
    else:
        print(f"FAIL: EXEC response: {resp}")
    s.close()

def test_discard():
    print("Testing DISCARD...")
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    s.connect(("127.0.0.1", 6379))
    
    send_recv(s, b"MULTI\r\n")
    send_recv(s, b"SET discard_key val\r\n")
    assert "OK" in send_recv(s, b"DISCARD\r\n")
    # Verify key not set
    assert "nil" in send_recv(s, b"GET discard_key\r\n") or "-1" in send_recv(s, b"GET discard_key\r\n") 
    print("PASS: DISCARD worked")
    s.close()

def test_watch_abort():
    print("Testing WATCH Abort...")
    # Client 1: WATCH k
    s1 = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    s1.connect(("127.0.0.1", 6379))
    send_recv(s1, b"SET watch_key init\r\n")
    assert "OK" in send_recv(s1, b"WATCH watch_key\r\n")
    assert "OK" in send_recv(s1, b"MULTI\r\n")
    assert "QUEUED" in send_recv(s1, b"SET watch_key final\r\n")
    
    # Client 2: Modify k
    s2 = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    s2.connect(("127.0.0.1", 6379))
    send_recv(s2, b"SET watch_key modified\r\n")
    s2.close()
    
    # Client 1: EXEC -> Should fail (nil)
    resp = send_recv(s1, b"EXEC\r\n")
    if "nil" in resp or "*-1" in resp: # Null array or nil bulk
        print("PASS: EXEC Aborted due to WATCH")
    else:
        print(f"FAIL: EXEC should have aborted but got: {resp}")
    s1.close()

def test_watch_success():
    print("Testing WATCH Success...")
    s1 = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    s1.connect(("127.0.0.1", 6379))
    
    send_recv(s1, b"SET watch_key2 init\r\n")
    send_recv(s1, b"WATCH watch_key2\r\n")
    send_recv(s1, b"MULTI\r\n")
    send_recv(s1, b"SET watch_key2 final\r\n")
    
    resp = send_recv(s1, b"EXEC\r\n")
    if "OK" in resp:
        print("PASS: EXEC Succeeded (Touched untouched key)")
    else:
        print(f"FAIL: EXEC failed: {resp}")
    s1.close()

if __name__ == "__main__":
    try:
        test_basic_transaction()
        test_discard()
        test_watch_abort()
        test_watch_success()
    except Exception as e:
        print(f"ERROR: {e}")
