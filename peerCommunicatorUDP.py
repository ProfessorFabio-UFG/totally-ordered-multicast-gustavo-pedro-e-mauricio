from socket import *
from constMP import *
import threading
import pickle
from requests import get
import heapq
from collections import defaultdict
import time
import sys

# Variáveis globais
handShakeCount = 0
PEERS = []
lamport_clock = 0
myself = -1

# Estruturas para ordenação
hold_back_queue = defaultdict(list)
expected_seq = defaultdict(int)

# Sockets
send_socket = socket(AF_INET, SOCK_DGRAM)
recv_socket = socket(AF_INET, SOCK_DGRAM)
recv_socket.settimeout(5.0)
recv_socket.bind(('0.0.0.0', PEER_UDP_PORT))

# Socket TCP para comunicação com servidor
server_sock = socket(AF_INET, SOCK_STREAM)
server_sock.bind(('0.0.0.0', PEER_TCP_PORT))
server_sock.listen(1)

def get_public_ip():
    try:
        ip = get('https://api.ipify.org', timeout=5).content.decode('utf8')
        print(f'My public IP: {ip}')
        return ip
    except:
        return '127.0.0.1'

def register_with_manager():
    try:
        sock = socket(AF_INET, SOCK_STREAM)
        sock.connect((GROUPMNGR_ADDR, GROUPMNGR_TCP_PORT))
        req = {"op": "register", "ipaddr": get_public_ip(), "port": PEER_UDP_PORT}
        sock.send(pickle.dumps(req))
        sock.close()
    except Exception as e:
        print(f"Failed to register: {e}")

def get_peer_list():
    try:
        sock = socket(AF_INET, SOCK_STREAM)
        sock.connect((GROUPMNGR_ADDR, GROUPMNGR_TCP_PORT))
        sock.send(pickle.dumps({"op": "list"}))
        peers = pickle.loads(sock.recv(2048))
        sock.close()
        return peers
    except Exception as e:
        print(f"Failed to get peers: {e}")
        return []

def get_unique_peers(peers_list):
    seen = set()
    return [p for p in peers_list if not (p in seen or seen.add(p))]

def create_message(msg_type, msg_id=0, ack_for=0):
    global lamport_clock
    lamport_clock += 1
    return {
        'type': msg_type,
        'sender': myself,
        'id': msg_id,
        'ack_for': ack_for,
        'timestamp': lamport_clock
    }

class PeerSystem:
    def __init__(self):
        self.running = True
        self.handshake_received = set()
        self.peer_lock = threading.Lock()
        
    def reset(self):
        with self.peer_lock:
            global handShakeCount, lamport_clock
            handShakeCount = 0
            lamport_clock = 0
            self.handshake_received.clear()
            hold_back_queue.clear()
            expected_seq.clear()

system = PeerSystem()

def message_handler():
    global lamport_clock
    print(f"Peer {myself} message handler started")
    
    while system.running:
        try:
            data, addr = recv_socket.recvfrom(2048)
            msg = pickle.loads(data)
            
            with system.peer_lock:
                # Atualiza relógio lógico
                lamport_clock = max(lamport_clock, msg['timestamp']) + 1
                
                # Processa mensagem
                if msg['type'] == 'READY':
                    if msg['sender'] not in system.handshake_received:
                        system.handshake_received.add(msg['sender'])
                        ack_msg = create_message('ACK', ack_for=msg['id'])
                        send_socket.sendto(pickle.dumps(ack_msg), (addr[0], PEER_UDP_PORT))
                        
                elif msg['type'] == 'DATA':
                    heapq.heappush(hold_back_queue[msg['sender']], (msg['timestamp'], msg))
                    ack_msg = create_message('ACK', ack_for=msg['id'])
                    send_socket.sendto(pickle.dumps(ack_msg), (addr[0], PEER_UDP_PORT))
                    
                elif msg['type'] == 'END':
                    system.running = False
                
                # Entrega mensagens ordenadas
                deliver_messages()
                
        except timeout:
            continue
        except Exception as e:
            print(f"Handler error: {e}")
            continue

def deliver_messages():
    for sender in list(hold_back_queue.keys()):
        while hold_back_queue[sender] and hold_back_queue[sender][0][1]['id'] == expected_seq[sender]:
            _, msg = heapq.heappop(hold_back_queue[sender])
            print(f"Peer {myself} delivered message {msg['id']} from {msg['sender']} (ts: {msg['timestamp']})")
            expected_seq[sender] += 1

def wait_for_start():
    try:
        conn, addr = server_sock.accept()
        data = conn.recv(1024)
        msg = pickle.loads(data)
        global myself
        myself = msg[0]
        conn.send(pickle.dumps(f"Peer {myself} started"))
        conn.close()
        return msg[1]
    except Exception as e:
        print(f"Start error: {e}")
        return 0

def main():
    global myself, PEERS
    
    register_with_manager()
    n_msgs = wait_for_start()
    
    if n_msgs <= 0:
        return
    
    peers = get_unique_peers(get_peer_list())
    if not peers:
        print("No valid peers found!")
        return
    
    PEERS = [p for p in peers if p != get_public_ip()]
    N = len(PEERS) + 1
    
    print(f"Peer {myself} started with {len(PEERS)} peers")
    
    handler_thread = threading.Thread(target=message_handler)
    handler_thread.daemon = True
    handler_thread.start()
    
    # Handshake
    ready_msg = create_message('READY')
    for peer in PEERS:
        send_socket.sendto(pickle.dumps(ready_msg), (peer, PEER_UDP_PORT))
    
    # Wait for handshakes
    while len(system.handshake_received) < N - 1 and system.running:
        time.sleep(0.1)
    
    print(f"Peer {myself} ready with {len(system.handshake_received)} peers")
    
    # Send messages
    for i in range(n_msgs):
        data_msg = create_message('DATA', msg_id=i)
        for peer in PEERS:
            send_socket.sendto(pickle.dumps(data_msg), (peer, PEER_UDP_PORT))
        time.sleep(0.5)
    
    # Cleanup
    end_msg = create_message('END')
    for peer in PEERS:
        send_socket.sendto(pickle.dumps(end_msg), (peer, PEER_UDP_PORT))
    
    handler_thread.join(timeout=10)
    print(f"Peer {myself} finished")

if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print("\nShutting down...")
        system.running = False
        sys.exit(0)
