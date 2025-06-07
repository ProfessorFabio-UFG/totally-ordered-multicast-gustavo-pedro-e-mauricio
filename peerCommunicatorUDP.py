from socket import *
from constMP import *
import threading
import pickle
from requests import get
import heapq
from collections import defaultdict
import time

# Variáveis globais
handShakeCount = 0
PEERS = []
lamport_clock = 0
myself = 0

# Estruturas para ordenação
hold_back_queue = defaultdict(list)
expected_seq = defaultdict(int)

# Sockets
send_socket = socket(AF_INET, SOCK_DGRAM)
recv_socket = socket(AF_INET, SOCK_DGRAM)
recv_socket.bind(('0.0.0.0', PEER_UDP_PORT))

# Socket TCP
server_sock = socket(AF_INET, SOCK_STREAM)
server_sock.bind(('0.0.0.0', PEER_TCP_PORT))
server_sock.listen(1)

# Constantes para tipos de mensagem
MSG_READY = 'READY'
MSG_DATA = 'DATA'
MSG_ACK = 'ACK'
MSG_END = 'END'

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
        print(f"Registration error: {e}")

def get_peer_list():
    try:
        sock = socket(AF_INET, SOCK_STREAM)
        sock.connect((GROUPMNGR_ADDR, GROUPMNGR_TCP_PORT))
        sock.send(pickle.dumps({"op": "list"}))
        peers = pickle.loads(sock.recv(2048))
        sock.close()
        return peers
    except Exception as e:
        print(f"Peer list error: {e}")
        return []

def update_clock(received_clock):
    global lamport_clock
    lamport_clock = max(lamport_clock, received_clock) + 1

def create_message(msg_type, msg_id=None, ack_for=None):
    global lamport_clock
    lamport_clock += 1
    
    if msg_type == MSG_READY:
        return (MSG_READY, myself, 0, lamport_clock)
    elif msg_type == MSG_DATA:
        return (MSG_DATA, myself, msg_id, lamport_clock)
    elif msg_type == MSG_ACK:
        return (MSG_ACK, myself, ack_for, lamport_clock)
    elif msg_type == MSG_END:
        return (MSG_END, myself, 0, lamport_clock)

def send_message(dest, msg):
    try:
        send_socket.sendto(pickle.dumps(msg), (dest, PEER_UDP_PORT))
    except Exception as e:
        print(f"Send error: {e}")

def validate_message(msg):
    if not isinstance(msg, tuple) or len(msg) < 4:
        return False
    
    msg_type = msg[0]
    if msg_type == MSG_READY:
        return len(msg) == 4
    elif msg_type in (MSG_DATA, MSG_ACK):
        return len(msg) == 4
    elif msg_type == MSG_END:
        return len(msg) == 4
    return False

def can_deliver(sender, msg_id):
    return msg_id == expected_seq[sender]

def deliver_message(msg):
    try:
        print(f'Delivered message {msg[2]} from {msg[1]} (ts: {msg[3]})')
        expected_seq[msg[1]] += 1
    except:
        print("Delivery error")

def check_pending_messages():
    for sender in list(hold_back_queue.keys()):
        while hold_back_queue[sender]:
            msg = hold_back_queue[sender][0][1]
            if can_deliver(sender, msg[2]):
                _, msg = heapq.heappop(hold_back_queue[sender])
                deliver_message(msg)
            else:
                break

class MessageHandler(threading.Thread):
    def __init__(self):
        threading.Thread.__init__(self)
        self.running = True
    
    def run(self):
        global handShakeCount, lamport_clock
        
        print("Message handler started")
        
        # Handshake phase
        while handShakeCount < N and self.running:
            try:
                data, addr = recv_socket.recvfrom(1024)
                msg = pickle.loads(data)
                
                if not validate_message(msg):
                    print("Invalid handshake format")
                    continue
                
                if msg[0] == MSG_READY:
                    update_clock(msg[3])
                    handShakeCount += 1
                    print(f"Handshake from {msg[1]}")
                    ack = create_message(MSG_ACK, ack_for=msg[2])
                    send_message(addr[0], ack)
            except Exception as e:
                print(f"Handshake error: {e}")

        print("All handshakes received")
        
        # Message processing
        while self.running:
            try:
                data, addr = recv_socket.recvfrom(1024)
                msg = pickle.loads(data)
                
                if not validate_message(msg):
                    print("Invalid message format")
                    continue
                
                # Update clock
                update_clock(msg[3])
                
                if msg[0] == MSG_DATA:
                    # Add to queue and send ACK
                    heapq.heappush(hold_back_queue[msg[1]], (msg[3], msg))
                    ack = create_message(MSG_ACK, ack_for=msg[2])
                    send_message(addr[0], ack)
                
                elif msg[0] == MSG_ACK:
                    pass  # ACKs are handled by clock update
                
                elif msg[0] == MSG_END:
                    self.running = False
                
                # Check for deliverable messages
                check_pending_messages()
                
            except Exception as e:
                print(f"Processing error: {e}")

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
    register_with_manager()
    while True:
        print("Waiting for start signal...")
        n_msgs = wait_for_start()
        
        if n_msgs <= 0:
            break

        print(f"I'm peer {myself} with {n_msgs} messages")
        
        global PEERS
        PEERS = get_peer_list()
        if not PEERS:
            print("No peers available")
            break
        
        handler = MessageHandler()
        handler.daemon = True
        handler.start()
        
        # Send handshakes
        ready_msg = create_message(MSG_READY)
        for peer in PEERS:
            send_message(peer, ready_msg)
        
        # Wait for handshakes
        while handShakeCount < N:
            time.sleep(0.1)
        
        # Send messages
        for i in range(n_msgs):
            data_msg = create_message(MSG_DATA, msg_id=i)
            for peer in PEERS:
                send_message(peer, data_msg)
        
        # Send end signal
        end_msg = create_message(MSG_END)
        for peer in PEERS:
            send_message(peer, end_msg)
        
        # Wait for completion
        while handler.running:
            time.sleep(0.1)

if __name__ == "__main__":
    main()