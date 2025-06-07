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
logical_clock = 0
message_queue = []
expected_acks = defaultdict(set)
received_acks = defaultdict(set)
myself = -1
nMsgs = -1
my_ip = ""

# Sockets
sendSocket = socket(AF_INET, SOCK_DGRAM)
recvSocket = socket(AF_INET, SOCK_DGRAM)
recvSocket.bind(('0.0.0.0', PEER_UDP_PORT))

# TCP socket para receber sinal de início
serverSock = socket(AF_INET, SOCK_STREAM)
serverSock.bind(('0.0.0.0', PEER_TCP_PORT))
serverSock.listen(1)

def get_public_ip():
    global my_ip
    my_ip = get('https://api.ipify.org').content.decode('utf8')
    print(f'Meu IP público: {my_ip}')
    return my_ip

def registerWithGroupManager():
    try:
        clientSock = socket(AF_INET, SOCK_STREAM)
        clientSock.connect((GROUPMNGR_ADDR, GROUPMNGR_TCP_PORT))
        req = {"op": "register", "ipaddr": get_public_ip(), "port": PEER_UDP_PORT}
        clientSock.send(pickle.dumps(req))
        clientSock.close()
    except Exception as e:
        print(f"Erro ao registrar: {e}")

def getListOfPeers():
    try:
        clientSock = socket(AF_INET, SOCK_STREAM)
        clientSock.connect((GROUPMNGR_ADDR, GROUPMNGR_TCP_PORT))
        req = {"op": "list"}
        clientSock.send(pickle.dumps(req))
        peers = pickle.loads(clientSock.recv(2048))
        clientSock.close()
        return list(set(peers))  # Remove duplicatas
    except Exception as e:
        print(f"Erro ao obter peers: {e}")
        return []

def update_logical_clock(received_clock=0):
    global logical_clock
    logical_clock = max(logical_clock, received_clock) + 1
    return logical_clock

class MsgHandler(threading.Thread):
    def __init__(self):
        threading.Thread.__init__(self)
        self.running = True
        self.logList = []

    def run(self):
        global handShakeCount, logical_clock
        
        while self.running:
            try:
                msgPack, _ = recvSocket.recvfrom(4096)
                msg = pickle.loads(msgPack)
                
                if msg['type'] == 'READY':
                    update_logical_clock(msg['timestamp'])
                    handShakeCount += 1
                    # Envia ACK
                    ack = {
                        'type': 'ACK_READY',
                        'sender': myself,
                        'timestamp': logical_clock,
                        'original_sender': msg['sender']
                    }
                    sendSocket.sendto(pickle.dumps(ack), (msg['ipaddr'], PEER_UDP_PORT))
                
                elif msg['type'] == 'ACK_READY':
                    update_logical_clock(msg['timestamp'])
                
                elif msg['type'] == 'DATA':
                    update_logical_clock(msg['timestamp'])
                    heapq.heappush(message_queue, (msg['timestamp'], msg['sender'], msg['content']))
                    # Envia ACK
                    ack = {
                        'type': 'ACK_DATA',
                        'sender': myself,
                        'timestamp': logical_clock,
                        'original_sender': msg['sender'],
                        'content': msg['content']
                    }
                    sendSocket.sendto(pickle.dumps(ack), (msg['ipaddr'], PEER_UDP_PORT))
                
                elif msg['type'] == 'ACK_DATA':
                    update_logical_clock(msg['timestamp'])
                    key = (msg['original_sender'], msg['content'])
                    received_acks[key].add(msg['sender'])
                
                elif msg['type'] == 'STOP':
                    self.running = False
                    
            except Exception as e:
                print(f"Erro no handler: {e}")

        # Processa mensagens restantes
        self.deliver_messages()
        
        # Envia logs para o servidor
        self.send_logs()

    def deliver_messages(self):
        while message_queue:
            timestamp, sender, content = heapq.heappop(message_queue)
            key = (sender, content)
            
            # Verifica se recebeu ACKs de todos os peers
            if len(received_acks.get(key, set())) >= N - 1:
                self.logList.append((sender, content, timestamp))
                print(f"Entregue: {content} de {sender} (ts={timestamp})")
                del received_acks[key]
            else:
                print(f"Aguardando ACKs para: {content} de {sender}")

    def send_logs(self):
        try:
            clientSock = socket(AF_INET, SOCK_STREAM)
            clientSock.connect((SERVER_ADDR, SERVER_PORT))
            clientSock.send(pickle.dumps(self.logList))
            clientSock.close()
        except Exception as e:
            print(f"Erro ao enviar logs: {e}")

def waitToStart():
    global myself, nMsgs
    conn, _ = serverSock.accept()
    msg = pickle.loads(conn.recv(1024))
    myself = msg[0]
    nMsgs = msg[1]
    conn.send(pickle.dumps(f"Peer {myself} iniciado"))
    conn.close()
    return (myself, nMsgs)

def main():
    global PEERS, handShakeCount
    
    registerWithGroupManager()
    myself, nMsgs = waitToStart()
    print(f"ID: {myself}, Mensagens: {nMsgs}")
    
    if nMsgs == 0:
        return
    
    PEERS = getListOfPeers()
    handler = MsgHandler()
    handler.start()
    
    # Fase de handshake
    for peer in PEERS:
        if peer != my_ip:
            msg = {
                'type': 'READY',
                'sender': myself,
                'timestamp': logical_clock,
                'ipaddr': my_ip
            }
            sendSocket.sendto(pickle.dumps(msg), (peer, PEER_UDP_PORT))
    
    # Aguarda handshakes
    while handShakeCount < N - 1:
        time.sleep(0.1)
    
    # Envia mensagens
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
            if peer != my_ip:
                sendSocket.sendto(pickle.dumps(msg), (peer, PEER_UDP_PORT))
        time.sleep(0.1)  # Pequeno delay entre mensagens
    
    # Envia sinal de parada
    for peer in PEERS:
        if peer != my_ip:
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