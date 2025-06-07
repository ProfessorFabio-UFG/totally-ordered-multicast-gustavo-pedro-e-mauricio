from socket import *
from constMP import *
import threading
import pickle
from requests import get
import heapq
from collections import defaultdict

# Variáveis globais
handShakeCount = 0
PEERS = []
lamport_clock = 0
myself = 0

# Estruturas para ordenação de mensagens
message_queue = []
hold_back_queue = defaultdict(list)
ack_received = defaultdict(int)
expected_seq = defaultdict(int)

# Sockets
send_socket = socket(AF_INET, SOCK_DGRAM)
recv_socket = socket(AF_INET, SOCK_DGRAM)
recv_socket.bind(('0.0.0.0', PEER_UDP_PORT))

# Socket TCP para comunicação com servidor
server_sock = socket(AF_INET, SOCK_STREAM)
server_sock.bind(('0.0.0.0', PEER_TCP_PORT))
server_sock.listen(1)

def get_public_ip():
    ip = get('https://api.ipify.org').content.decode('utf8')
    print(f'My public IP: {ip}')
    return ip

def register_with_manager():
    sock = socket(AF_INET, SOCK_STREAM)
    sock.connect((GROUPMNGR_ADDR, GROUPMNGR_TCP_PORT))
    req = {"op": "register", "ipaddr": get_public_ip(), "port": PEER_UDP_PORT}
    sock.send(pickle.dumps(req))
    sock.close()

def get_peer_list():
    sock = socket(AF_INET, SOCK_STREAM)
    sock.connect((GROUPMNGR_ADDR, GROUPMNGR_TCP_PORT))
    sock.send(pickle.dumps({"op": "list"}))
    peers = pickle.loads(sock.recv(2048))
    sock.close()
    return peers

def update_clock(received_clock):
    global lamport_clock
    lamport_clock = max(lamport_clock, received_clock) + 1

def send_ack(dest, msg_id, timestamp):
    ack_msg = ('ACK', myself, msg_id, timestamp, lamport_clock)
    send_socket.sendto(pickle.dumps(ack_msg), (dest, PEER_UDP_PORT))

def send_message(dest, msg_id):
    global lamport_clock
    lamport_clock += 1
    msg = ('DATA', myself, msg_id, lamport_clock)
    send_socket.sendto(pickle.dumps(msg), (dest, PEER_UDP_PORT))

def can_deliver(sender, msg_id):
    return msg_id == expected_seq[sender]

def deliver_message(msg):
    # Processa a mensagem na aplicação
    print(f'Delivered message {msg[2]} from {msg[1]} (ts: {msg[3]})')
    expected_seq[msg[1]] += 1

def check_pending_messages():
    for sender in list(hold_back_queue.keys()):
        while hold_back_queue[sender] and can_deliver(sender, hold_back_queue[sender][0][2]):
            msg = heapq.heappop(hold_back_queue[sender])[1]
            deliver_message(msg)

class MessageHandler(threading.Thread):
    def __init__(self):
        threading.Thread.__init__(self)
    
    def run(self):
        global handShakeCount, lamport_clock
        
        print("Message handler started")
        
        # Fase de handshake
        while handShakeCount < N:
            data, addr = recv_socket.recvfrom(1024)
            msg = pickle.loads(data)
            
            if msg[0] == 'READY':
                update_clock(msg[3])
                handShakeCount += 1
                print(f"Handshake from {msg[1]}")
                send_ack(addr[0], msg[2], msg[3])

        print("All handshakes received. Starting message processing.")
        
        while True:
            data, addr = recv_socket.recvfrom(1024)
            msg = pickle.loads(data)
            
            # 1. Atualiza relógio lógico
            update_clock(msg[4] if msg[0] == 'ACK' else msg[3])
            
            if msg[0] == 'DATA':
                # 2. Coloca na fila
                heapq.heappush(hold_back_queue[msg[1]], (msg[3], msg))
                
                # 3. Envia ACK
                send_ack(addr[0], msg[2], msg[3])
                
            elif msg[0] == 'ACK':
                ack_received[(msg[1], msg[2])] += 1
            
            # 4. Verifica se pode entregar mensagens
            check_pending_messages()

def wait_for_start():
    conn, addr = server_sock.accept()
    data = conn.recv(1024)
    msg = pickle.loads(data)
    global myself
    myself = msg[0]
    conn.send(pickle.dumps(f"Peer {myself} started"))
    conn.close()
    return msg[1]  # Retorna número de mensagens

def main():
    register_with_manager()
    while True:
        print("Waiting for start signal...")
        n_msgs = wait_for_start()
        print(f"I'm peer {myself} with {n_msgs} messages")
        
        if n_msgs == 0:
            break

        global PEERS
        PEERS = get_peer_list()
        
        handler = MessageHandler()
        handler.start()
        
        # Envia handshakes
        for peer in PEERS:
            msg = ('READY', myself, 0, lamport_clock)
            send_socket.sendto(pickle.dumps(msg), (peer, PEER_UDP_PORT))
        
        # Espera handshakes completos
        while handShakeCount < N:
            pass
        
        # Envia mensagens
        for i in range(n_msgs):
            for peer in PEERS:
                send_message(peer, i)
        
        # Espera todas as mensagens serem entregues
        while any(hold_back_queue.values()):
            pass

if __name__ == "__main__":
    main()