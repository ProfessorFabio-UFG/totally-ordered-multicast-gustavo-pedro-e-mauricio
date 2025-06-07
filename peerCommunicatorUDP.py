from socket import *
from constMP import * #-
import threading
import random
import time
import pickle
from requests import get

# Counter to make sure we have received handshakes from all other processes
handShakeCount = 0
PEERS = []
LIST_MESSAGES = []  # Lista global para armazenar mensagens ordenadas

# UDP sockets to send and receive data messages:
# Create send socket
sendSocket = socket(AF_INET, SOCK_DGRAM)
#Create and bind receive socket
recvSocket = socket(AF_INET, SOCK_DGRAM)
recvSocket.bind(('0.0.0.0', PEER_UDP_PORT))

# TCP socket to receive start signal from the comparison server:
serverSock = socket(AF_INET, SOCK_STREAM)
serverSock.bind(('0.0.0.0', PEER_TCP_PORT))
serverSock.listen(1)


def get_public_ip():
    ipAddr = get('https://api.ipify.org').content.decode('utf8')
    print('My public IP address is: {}'.format(ipAddr))
    return ipAddr

def registerWithGroupManager():
    clientSock = socket(AF_INET, SOCK_STREAM)
    print('Connecting to group manager: ', (GROUPMNGR_ADDR,GROUPMNGR_TCP_PORT))
    clientSock.connect((GROUPMNGR_ADDR,GROUPMNGR_TCP_PORT))
    ipAddr = get_public_ip()
    req = {"op":"register", "ipaddr":ipAddr, "port":PEER_UDP_PORT}
    msg = pickle.dumps(req)
    print('Registering with group manager: ', req)
    clientSock.send(msg)
    clientSock.close()

def getListOfPeers():
    clientSock = socket(AF_INET, SOCK_STREAM)
    print('Connecting to group manager: ', (GROUPMNGR_ADDR,GROUPMNGR_TCP_PORT))
    clientSock.connect((GROUPMNGR_ADDR,GROUPMNGR_TCP_PORT))
    req = {"op":"list"}
    msg = pickle.dumps(req)
    print('Getting list of peers from group manager: ', req)
    clientSock.send(msg)
    msg = clientSock.recv(2048)
    PEERS = pickle.loads(msg)
    print('Got list of peers: ', PEERS)
    clientSock.close()
    return PEERS

class MsgHandler(threading.Thread):
    def __init__(self, sock):
        threading.Thread.__init__(self)
        self.sock = sock

    def run(self):
        print('[LOG] Handler thread started. Waiting for handshakes...')
        global handShakeCount
        global LIST_MESSAGES
        
        # Wait until handshakes are received from all other processes
        while handShakeCount < N:
            msgPack = self.sock.recv(1024)
            msg = pickle.loads(msgPack)
            if msg[0] == 'READY':
                handShakeCount += 1
                print('[LOG] Received handshake from process', msg[1], 
                      '| Total handshakes:', handShakeCount, '/', N)

        print('[LOG] All handshakes received. Starting message processing...')

        stopCount = 0 
        while True:                
            msgPack = self.sock.recv(1024)
            msg = pickle.loads(msgPack)
            if msg[0] == -1:   # count the 'stop' messages
                stopCount += 1
                print('[LOG] Received STOP signal from process', msg[1], 
                      '| Total STOPs:', stopCount, '/', N)
                if stopCount == N:
                    break
            else:
                print('[LOG] Received message', msg[1], 'from process', msg[0])
                LIST_MESSAGES.append(msg)
                print('[LOG] Current message list size:', len(LIST_MESSAGES))
        
        # Ordenação das mensagens
        print('[LOG] Starting message sorting...')
        LIST_MESSAGES.sort(key=lambda x: (x[0], x[1]))
        print('[LOG] Sorting completed. First few messages:')
        for msg in LIST_MESSAGES[:5]:  # Mostra as primeiras 5 mensagens como amostra
            print('   ', msg)
        
        # Write log file
        logFile = open('logfile'+str(myself)+'.log', 'w')
        logFile.writelines(str(LIST_MESSAGES))
        logFile.close()
        print('[LOG] Messages saved to log file')
        
        # Send to server
        print('[LOG] Preparing to send ordered messages to server...')
        clientSock = socket(AF_INET, SOCK_STREAM)
        clientSock.connect((SERVER_ADDR, SERVER_PORT))
        msgPack = pickle.dumps(LIST_MESSAGES)
        clientSock.send(msgPack)
        clientSock.close()
        print('[LOG] Ordered messages sent to server')
        
        # Reset
        handShakeCount = 0
        LIST_MESSAGES = []
        print('[LOG] Handler thread finished')
        exit(0)

def waitToStart():
    print('[LOG] Waiting for start signal from server...')
    (conn, addr) = serverSock.accept()
    msgPack = conn.recv(1024)
    msg = pickle.loads(msgPack)
    myself = msg[0]
    nMsgs = msg[1]
    conn.send(pickle.dumps('Peer process '+str(myself)+' started.'))
    conn.close()
    print('[LOG] Start signal received. My ID:', myself, '| Messages to send:', nMsgs)
    return (myself, nMsgs)

# Main execution
print('[LOG] Starting peer process...')
registerWithGroupManager()
while True:
    print('\n[LOG] ===== NEW ITERATION =====')
    (myself, nMsgs) = waitToStart()
    print('[LOG] I am process', myself)

    if nMsgs == 0:
        print('[LOG] Termination signal received. Exiting.../')
        exit(0)

    time.sleep(5)
    print('[LOG] Creating message handler...')
    msgHandler = MsgHandler(recvSocket)
    msgHandler.start()

    PEERS = getListOfPeers()
    print('[LOG] Peer list obtained:', PEERS)
    
    # Send handshakes
    print('[LOG] Sending handshakes to all peers...')
    for addrToSend in PEERS:
        print('   Sending to:', addrToSend)
        msg = ('READY', myself)
        msgPack = pickle.dumps(msg)
        sendSocket.sendto(msgPack, (addrToSend, PEER_UDP_PORT))

    print('[LOG] All handshakes sent. Waiting for responses...')
    while handShakeCount < N:
        time.sleep(0.1)  # Evita CPU spinning
    print('[LOG] All handshakes confirmed. Proceeding to send messages...')

    # Send data messages
    print('[LOG] Starting to send', nMsgs, 'messages to each peer...')
    for msgNumber in range(0, nMsgs):
        delay = random.randrange(10,100)/1000
        time.sleep(delay)
        msg = (myself, msgNumber)
        msgPack = pickle.dumps(msg)
        for addrToSend in PEERS:
            sendSocket.sendto(msgPack, (addrToSend, PEER_UDP_PORT))
        print('   Sent message', msgNumber, 'to all peers | Delay:', delay)
    
    # Send stop signals
    print('[LOG] Sending STOP signals to all peers...')
    for addrToSend in PEERS:
        msg = (-1, -1)
        msgPack = pickle.dumps(msg)
        sendSocket.sendto(msgPack, (addrToSend, PEER_UDP_PORT))
    print('[LOG] All STOP signals sent. Waiting for handler to finish...')
    
    msgHandler.join()  # Espera a thread handler terminar
    print('[LOG] Handler finished. Ready for next iteration.')