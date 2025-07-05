from socket import *
from constMP import *
import threading
import random
import time
import pickle
from requests import get

def increment_clock(clock):
    return clock + 1

def update_clock(local_clock, received_ts):
    return max(local_clock, received_ts) + 1

def get_public_ip():
    ipAddr = get('https://api.ipify.org').content.decode('utf8')
    print('My public IP address is: {}'.format(ipAddr))
    return ipAddr

def registerWithGroupManager():
    clientSock = socket(AF_INET, SOCK_STREAM)
    print('Connecting to group manager: ', (GROUPMNGR_ADDR, GROUPMNGR_TCP_PORT))
    clientSock.connect((GROUPMNGR_ADDR, GROUPMNGR_TCP_PORT))
    ipAddr = get_public_ip()
    req = {"op": "register", "ipaddr": ipAddr, "port": PEER_UDP_PORT}
    msg = pickle.dumps(req)
    print('Registering with group manager: ', req)
    clientSock.send(msg)
    clientSock.close()

def getListOfPeers():
    clientSock = socket(AF_INET, SOCK_STREAM)
    print('Connecting to group manager: ', (GROUPMNGR_ADDR, GROUPMNGR_TCP_PORT))
    clientSock.connect((GROUPMNGR_ADDR, GROUPMNGR_TCP_PORT))
    req = {"op": "list"}
    msg = pickle.dumps(req)
    clientSock.send(msg)
    msg = clientSock.recv(2048)
    peer_list = pickle.loads(msg)
    clientSock.close()
    return peer_list

class MsgHandler(threading.Thread):
    def __init__(self, sock, myself, nMsgs, peer_count):
        threading.Thread.__init__(self)
        self.sock = sock
        self.myself = myself
        self.nMsgs = nMsgs
        self.peer_count = peer_count
        self.handShakeCount = 0
        self.stopCount = 0
        self.lamport_clock = 0
        self.LIST_MESSAGES = []
        self.running = True

    def run(self):
        print('[LOG] Handler thread started. Waiting for handshakes...')
        self.sock.settimeout(5)
        while self.handShakeCount < self.peer_count:
            try:
                msgPack = self.sock.recv(1024)
                msg = pickle.loads(msgPack)
                if msg[0] == 'READY':
                    self.handShakeCount += 1
                    print(f'[LOG] Received handshake from process {msg[1]} | Total: {self.handShakeCount}/{self.peer_count}')
            except timeout:
                continue

        print('[LOG] All handshakes received. Starting message processing...')
        while self.stopCount < self.peer_count:
            try:
                msgPack = self.sock.recv(1024)
                msg = pickle.loads(msgPack)
                if msg[0] == -1:
                    self.stopCount += 1
                    print(f'[LOG] Received STOP from process {msg[1]} | Total: {self.stopCount}/{self.peer_count}')
                elif isinstance(msg, tuple) and len(msg) == 3:
                    sender, msg_number, ts = msg
                    self.lamport_clock = update_clock(self.lamport_clock, ts)
                    self.LIST_MESSAGES.append(msg)
            except timeout:
                continue

        print('[LOG] Sorting messages...')
        self.LIST_MESSAGES.sort(key=lambda x: (x[2], x[0], x[1]))
        with open(f'logfile{self.myself}.log', 'w') as logFile:
            logFile.writelines(str(self.LIST_MESSAGES))

        clientSock = socket(AF_INET, SOCK_STREAM)
        clientSock.connect((SERVER_ADDR, SERVER_PORT))
        clientSock.send(pickle.dumps(self.LIST_MESSAGES))
        clientSock.close()
        print('[LOG] Messages sent to server.')
        self.sock.close()
        print('[LOG] Handler finished.')

serverSock = socket(AF_INET, SOCK_STREAM)
serverSock.bind(('0.0.0.0', PEER_TCP_PORT))
serverSock.listen(1)

print('[LOG] Starting peer process...')
registerWithGroupManager()

while True:
    print('\n[LOG] ===== NEW ITERATION =====')
    print('[LOG] Waiting for start signal...')
    conn, addr = serverSock.accept()
    msgPack = conn.recv(1024)
    myself, nMsgs = pickle.loads(msgPack)
    conn.send(pickle.dumps(f'Peer process {myself} started.'))
    conn.close()

    if nMsgs == 0:
        print('[LOG] Termination signal received.')
        break

    PEERS = getListOfPeers()
    peer_count = len(PEERS)

    recvSocket = socket(AF_INET, SOCK_DGRAM)
    recvSocket.bind(('0.0.0.0', PEER_UDP_PORT))

    msgHandler = MsgHandler(recvSocket, myself, nMsgs, peer_count)
    msgHandler.start()

    time.sleep(1)
    sendSocket = socket(AF_INET, SOCK_DGRAM)

    for addr in PEERS:
        sendSocket.sendto(pickle.dumps(('READY', myself)), (addr, PEER_UDP_PORT))

    time.sleep(1)
    for i in range(nMsgs):
        lamport_ts = i + 1
        msg = (myself, i, lamport_ts)
        for addr in PEERS:
            sendSocket.sendto(pickle.dumps(msg), (addr, PEER_UDP_PORT))

    time.sleep(1)
    for addr in PEERS:
        sendSocket.sendto(pickle.dumps((-1, myself)), (addr, PEER_UDP_PORT))

    msgHandler.join()
    sendSocket.close()

print('[LOG] Peer process terminated.')