from socket import *
from constMP import *
import threading
import random
import time
import pickle
from requests import get
from collections import deque

# Estruturas de dados adicionais
logical_clock = 0
message_queue = deque()
pending_acks = {}
PEERS = []
handShakeCount = 0

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
    PEERS = pickle.loads(clientSock.recv(2048))
    clientSock.close()
    return PEERS

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
            # Verifica se a primeira mensagem na fila pode ser processada
            msg_type, msg_id, msg_clock, sender_id, sender_addr = message_queue[0]
            
            if msg_type == 'DATA':
                # Verifica se temos o ACK correspondente
                if (sender_addr, msg_id) in pending_acks:
                    if pending_acks[(sender_addr, msg_id)]:
                        # Remove da fila e processa
                        message_queue.popleft()
                        del pending_acks[(sender_addr, msg_id)]
                        deliver_message(msg_type, msg_id, msg_clock, sender_id)
            elif msg_type == 'ACK':
                # Processa ACK imediatamente
                message_queue.popleft()
                deliver_message(msg_type, msg_id, msg_clock, sender_id)
        time.sleep(0.01)  # Pequena pausa para evitar uso excessivo de CPU

def deliver_message(msg_type, msg_id, msg_clock, sender_id):
    global logical_clock
    update_logical_clock(msg_clock)
    
    if msg_type == 'DATA':
        print(f'Message {msg_id} from process {sender_id} with clock {msg_clock}')
    elif msg_type == 'ACK':
        print(f'ACK received for message {msg_id} from process {sender_id}')

class MsgHandler(threading.Thread):
    def __init__(self, sock):
        threading.Thread.__init__(self)
        self.sock = sock

    def run(self):
        global handShakeCount
        
        logList = []
        
        # Processa handshakes
        while handShakeCount < N:
            msgPack = self.sock.recv(1024)
            msg = pickle.loads(msgPack)
            if msg[0] == 'READY':
                handShakeCount += 1
                # Envia confirmação
                reply = ('READY_ACK', myself, logical_clock)
                sendSocket.sendto(pickle.dumps(reply), (msg[2], PEER_UDP_PORT))
                print(f'--- Handshake received from {msg[1]}')

        print('Secondary Thread: Received all handshakes. Entering message loop.')

        stopCount = 0
        while True:
            msgPack = self.sock.recv(1024)
            msg = pickle.loads(msgPack)
            
            if msg[0] == 'DATA':
                # Atualiza relógio lógico
                update_logical_clock(msg[2])
                
                # Adiciona à fila
                message_queue.append((msg[0], msg[1], msg[2], msg[3], msg[4]))
                
                # Marca ACK como pendente
                pending_acks[(msg[4], msg[1])] = False
                
                # Envia ACK
                send_ack(msg[4], msg[1], logical_clock)
                
            elif msg[0] == 'ACK':
                # Atualiza relógio lógico
                update_logical_clock(msg[2])
                
                # Adiciona à fila
                message_queue.append((msg[0], msg[1], msg[2], msg[3], None))
                
                # Marca ACK como recebido
                if (msg[3], msg[1]) in pending_acks:
                    pending_acks[(msg[3], msg[1])] = True
                
            elif msg[0] == -1:
                stopCount += 1
                if stopCount == N:
                    break
            else:
                print('Unknown message type:', msg[0])

        # Escreve log
        logFile = open(f'logfile{myself}.log', 'w')
        logFile.writelines(str(logList))
        logFile.close()
        
        # Envia logs para o servidor
        clientSock = socket(AF_INET, SOCK_STREAM)
        clientSock.connect((SERVER_ADDR, SERVER_PORT))
        clientSock.send(pickle.dumps(logList))
        clientSock.close()
        
        handShakeCount = 0
        exit(0)

def waitToStart():
    (conn, addr) = serverSock.accept()
    msg = pickle.loads(conn.recv(1024))
    myself = msg[0]
    nMsgs = msg[1]
    conn.send(pickle.dumps(f'Peer process {myself} started.'))
    conn.close()
    return (myself, nMsgs)

# Inicialização
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
for addrToSend in PEERS:
    msg = ('READY', myself, logical_clock, addrToSend)
    sendSocket.sendto(pickle.dumps(msg), (addrToSend, PEER_UDP_PORT))

while handShakeCount < N:
    time.sleep(0.1)

# Envia mensagens de dados
for msgNumber in range(nMsgs):
    logical_clock += 1
    msg = ('DATA', msgNumber, logical_clock, myself, get_public_ip())
    for addrToSend in PEERS:
        sendSocket.sendto(pickle.dumps(msg), (addrToSend, PEER_UDP_PORT))
        pending_acks[(addrToSend, msgNumber)] = False
        print(f'Sent message {msgNumber} to {addrToSend}')

# Envia mensagens de término
for addrToSend in PEERS:
    msg = (-1, -1)
    sendSocket.sendto(pickle.dumps(msg), (addrToSend, PEER_UDP_PORT))