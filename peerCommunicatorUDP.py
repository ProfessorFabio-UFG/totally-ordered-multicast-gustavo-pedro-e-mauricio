from socket import *
from constMP import *
import threading
import pickle
from requests import get
import heapq
from collections import defaultdict
import time
import sys

# Configuração inicial
myself = int(sys.argv[1]) if len(sys.argv) > 1 else 0
PEER_TCP_PORT += myself  # Porta única para cada peer
PEER_UDP_PORT += myself

# Variáveis globais
handShakeCount = 0
PEERS = []
logical_clock = 0
message_queue = []
received_acks = defaultdict(set)
my_ip = ""

# Sockets
sendSocket = socket(AF_INET, SOCK_DGRAM)
recvSocket = socket(AF_INET, SOCK_DGRAM)
recvSocket.bind(('0.0.0.0', PEER_UDP_PORT))
recvSocket.settimeout(1.0)

serverSock = socket(AF_INET, SOCK_STREAM)
serverSock.setsockopt(SOL_SOCKET, SO_REUSEADDR, 1)
serverSock.bind(('0.0.0.0', PEER_TCP_PORT))
serverSock.listen(5)

def get_public_ip():
    global my_ip
    my_ip = get('https://api.ipify.org').content.decode('utf8')
    print(f'Peer {myself} - IP: {my_ip}')
    return my_ip

def registerWithGroupManager():
    try:
        clientSock = socket(AF_INET, SOCK_STREAM)
        clientSock.connect((GROUPMNGR_ADDR, GROUPMNGR_TCP_PORT))
        req = {"op": "register", "ipaddr": my_ip, "port": PEER_UDP_PORT}
        clientSock.send(pickle.dumps(req))
        clientSock.close()
    except Exception as e:
        print(f"Erro no registro: {e}")

def getListOfPeers():
    try:
        clientSock = socket(AF_INET, SOCK_STREAM)
        clientSock.connect((GROUPMNGR_ADDR, GROUPMNGR_TCP_PORT))
        req = {"op": "list"}
        clientSock.send(pickle.dumps(req))
        peers = pickle.loads(clientSock.recv(2048))
        clientSock.close()
        return [p for p in peers if p != my_ip]  # Remove próprio IP
    except Exception as e:
        print(f"Erro ao obter peers: {e}")
        return []

def update_logical_clock(received_clock=0):
    global logical_clock
    logical_clock = max(logical_clock, received_clock) + 1
    return logical_clock

def send_ack(dest_ip, sender_id, content):
    ack_msg = {
        'type': 'ACK_DATA',
        'sender': myself,
        'timestamp': logical_clock,
        'original_sender': sender_id,
        'content': content,
        'ipaddr': my_ip
    }
    sendSocket.sendto(pickle.dumps(ack_msg), (dest_ip, PEER_UDP_PORT))

class MsgHandler(threading.Thread):
    def __init__(self):
        threading.Thread.__init__(self)
        self.logList = []
        self.running = True

    def run(self):
        global handShakeCount
        
        while self.running:
            try:
                msgPack, addr = recvSocket.recvfrom(4096)
                msg = pickle.loads(msgPack)
                
                if msg['type'] == 'READY':
                    update_logical_clock(msg['timestamp'])
                    handShakeCount += 1
                    print(f"Handshake de {msg['sender']}")
                    send_ack(msg['ipaddr'], msg['sender'], "READY")
                
                elif msg['type'] == 'ACK_DATA':
                    update_logical_clock(msg['timestamp'])
                    key = (msg['original_sender'], msg['content'])
                    received_acks[key].add(msg['sender'])
                    print(f"ACK de {msg['sender']} para {msg['content']}")
                
                elif msg['type'] == 'DATA':
                    update_logical_clock(msg['timestamp'])
                    heapq.heappush(message_queue, (msg['timestamp'], msg['sender'], msg['content']))
                    send_ack(msg['ipaddr'], msg['sender'], msg['content'])
                    print(f"Dados de {msg['sender']}: {msg['content']}")
                
                elif msg['type'] == 'STOP':
                    self.running = False
            
            except timeout:
                continue
            except Exception as e:
                print(f"Erro no handler: {e}")

        self.deliver_all()
        self.send_logs()

    def deliver_all(self):
        while message_queue:
            timestamp, sender, content = heapq.heappop(message_queue)
            key = (sender, content)
            if len(received_acks.get(key, set())) >= N - 1:
                self.logList.append((sender, content, timestamp))
                print(f"Entregue: {content} de {sender}")
            else:
                print(f"Faltando ACKs para: {content}")

    def send_logs(self):
        try:
            clientSock = socket(AF_INET, SOCK_STREAM)
            clientSock.connect((SERVER_ADDR, SERVER_PORT))
            clientSock.send(pickle.dumps(self.logList))
            clientSock.close()
        except Exception as e:
            print(f"Erro ao enviar logs: {e}")

def waitToStart():
    conn, addr = serverSock.accept()
    msg = pickle.loads(conn.recv(1024))
    global myself, nMsgs
    myself = msg[0]
    nMsgs = msg[1]
    conn.send(pickle.dumps(f"Peer {myself} pronto"))
    conn.close()
    return (myself, nMsgs)

def main():
    global PEERS
    
    get_public_ip()
    registerWithGroupManager()
    myself, nMsgs = waitToStart()
    print(f"Peer {myself} iniciado com {nMsgs} mensagens")
    
    PEERS = getListOfPeers()
    print(f"Peers conectados: {PEERS}")
    
    handler = MsgHandler()
    handler.start()
    
    # Handshake
    for peer in PEERS:
        msg = {
            'type': 'READY',
            'sender': myself,
            'timestamp': logical_clock,
            'ipaddr': my_ip
        }
        sendSocket.sendto(pickle.dumps(msg), (peer, PEER_UDP_PORT))
    
    while handShakeCount < len(PEERS):
        time.sleep(0.5)
    
    # Envio de mensagens
    for i in range(nMsgs):
        update_logical_clock()
        msg = {
            'type': 'DATA',
            'sender': myself,
            'timestamp': logical_clock,
            'content': f"msg{i}",
            'ipaddr': my_ip
        }
        for peer in PEERS:
            sendSocket.sendto(pickle.dumps(msg), (peer, PEER_UDP_PORT))
        time.sleep(0.5)
    
    # Sinal de parada
    for peer in PEERS:
        msg = {
            'type': 'STOP',
            'sender': myself,
            'timestamp': logical_clock,
            'ipaddr': my_ip
        }
        sendSocket.sendto(pickle.dumps(msg), (peer, PEER_UDP_PORT))
    
    handler.join()

if __name__ == "__main__":
    main()