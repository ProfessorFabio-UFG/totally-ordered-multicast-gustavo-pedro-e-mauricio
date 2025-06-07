from socket import *
from constMP import *
import threading
import random
import time
import pickle
from requests import get
from collections import deque

# Variáveis globais
logical_clock = 0
message_queue = deque()
pending_acks = {}
PEERS = []
handShakeCount = 0
myself = None

# Sockets
sendSocket = socket(AF_INET, SOCK_DGRAM)
recvSocket = socket(AF_INET, SOCK_DGRAM)
recvSocket.bind(('0.0.0.0', PEER_UDP_PORT))

# TCP socket para receber sinal de início
serverSock = socket(AF_INET, SOCK_STREAM)
serverSock.bind(('0.0.0.0', PEER_TCP_PORT))
serverSock.listen(1)

def get_public_ip():
    ipAddr = get('https://api.ipify.org').content.decode('utf8')
    print('My public IP address is: {}'.format(ipAddr))
    return ipAddr

def registerWithGroupManager():
    clientSock = socket(AF_INET, SOCK_STREAM)
    clientSock.connect((GROUPMNGR_ADDR, GROUPMNGR_TCP_PORT))
    ipAddr = get_public_ip()
    req = {"op": "register", "ipaddr": ipAddr, "port": PEER_UDP_PORT}
    clientSock.send(pickle.dumps(req))
    clientSock.close()

def getListOfPeers():
    clientSock = socket(AF_INET, SOCK_STREAM)
    clientSock.connect((GROUPMNGR_ADDR, GROUPMNGR_TCP_PORT))
    clientSock.send(pickle.dumps({"op": "list"}))
    peers = pickle.loads(clientSock.recv(2048))
    clientSock.close()
    return peers

def update_logical_clock(received_clock):
    global logical_clock
    logical_clock = max(logical_clock, received_clock) + 1

def send_ack(dest_addr, msg_id, sender_clock):
    global logical_clock
    logical_clock += 1
    ack_msg = ('ACK', msg_id, logical_clock, myself)
    sendSocket.sendto(pickle.dumps(ack_msg), (dest_addr, PEER_UDP_PORT))

def process_message_queue():
    while True:
        if message_queue:
            msg = message_queue[0]
            msg_type = msg[0]
            
            if msg_type == 'DATA':
                sender_addr, msg_id = msg[4], msg[1]
                if (sender_addr, msg_id) in pending_acks and pending_acks[(sender_addr, msg_id)]:
                    message_queue.popleft()
                    deliver_message(*msg)
                    del pending_acks[(sender_addr, msg_id)]
            elif msg_type == 'ACK':
                message_queue.popleft()
                deliver_message(*msg)
        time.sleep(0.01)

def deliver_message(msg_type, msg_id, msg_clock, sender_id, sender_addr=None):
    global logical_clock
    update_logical_clock(msg_clock)
    
    if msg_type == 'DATA':
        print(f'Message {msg_id} from process {sender_id} with clock {msg_clock}')
    elif msg_type == 'ACK':
        print(f'ACK received for message {msg_id} from process {sender_id}')

def waitToStart():
    (conn, addr) = serverSock.accept()
    msg = pickle.loads(conn.recv(1024))
    global myself
    myself = msg[0]
    nMsgs = msg[1]
    conn.send(pickle.dumps(f'Peer process {myself} started.'))
    conn.close()
    return (myself, nMsgs)

class MsgHandler(threading.Thread):
    def __init__(self, sock):
        threading.Thread.__init__(self)
        self.sock = sock

    def run(self):
        global handShakeCount, logical_clock
        
        # Processa handshakes
        while handShakeCount < N:
            msgPack = self.sock.recv(1024)
            msg = pickle.loads(msgPack)
            if msg[0] == 'READY':
                handShakeCount += 1
                update_logical_clock(msg[2])
                # Envia confirmação
                reply = ('READY_ACK', myself, logical_clock)
                sendSocket.sendto(pickle.dumps(reply), (msg[3], PEER_UDP_PORT))
                print(f'--- Handshake received from {msg[1]}')

        print('Secondary Thread: Received all handshakes. Entering message loop.')

        stopCount = 0
        while True:
            msgPack = self.sock.recv(1024)
            msg = pickle.loads(msgPack)
            
            if msg[0] == 'DATA':
                update_logical_clock(msg[2])
                message_queue.append(msg)
                pending_acks[(msg[4], msg[1])] = False
                send_ack(msg[4], msg[1], logical_clock)
                
            elif msg[0] == 'ACK':
                update_logical_clock(msg[2])
                message_queue.append(msg)
                pending_acks[(msg[3], msg[1])] = True
                
            elif msg[0] == -1:
                stopCount += 1
                if stopCount == N:
                    break

        # Escreve log
        logFile = open(f'logfile{myself}.log', 'w')
        logFile.writelines(str(message_queue))
        logFile.close()
        
        # Envia logs para o servidor
        clientSock = socket(AF_INET, SOCK_STREAM)
        clientSock.connect((SERVER_ADDR, SERVER_PORT))
        clientSock.send(pickle.dumps(list(message_queue)))
        clientSock.close()

# Main execution
if __name__ == "__main__":
    registerWithGroupManager()
    (myself, nMsgs) = waitToStart()
    print(f'I am up, and my ID is: {myself}')

    if nMsgs == 0:
        exit(0)

    # Inicia thread para processar fila de mensagens
    queue_processor = threading.Thread(target=process_message_queue)
    queue_processor.daemon = True
    queue_processor.start()

    # Inicia handler de mensagens
    msgHandler = MsgHandler(recvSocket)
    msgHandler.start()

    PEERS = getListOfPeers()

    # Envia handshakes
    my_ip = get_public_ip()
    for peer in PEERS:
        if peer != my_ip:  # Não enviar handshake para si mesmo
            msg = ('READY', myself, logical_clock, peer)
            sendSocket.sendto(pickle.dumps(msg), (peer, PEER_UDP_PORT))

    while handShakeCount < N-1:  # N-1 porque não contamos conosco mesmos
        time.sleep(0.1)

    # Envia mensagens de dados
    for msgNumber in range(nMsgs):
        logical_clock += 1
        msg = ('DATA', msgNumber, logical_clock, myself, my_ip)
        for peer in PEERS:
            if peer != my_ip:  # Não enviar mensagem para si mesmo
                sendSocket.sendto(pickle.dumps(msg), (peer, PEER_UDP_PORT))
                pending_acks[(peer, msgNumber)] = False
                print(f'Sent message {msgNumber} to {peer}')

    # Envia mensagens de término
    for peer in PEERS:
        if peer != my_ip:
            msg = (-1, -1)
            sendSocket.sendto(pickle.dumps(msg), (peer, PEER_UDP_PORT))

    msgHandler.join()