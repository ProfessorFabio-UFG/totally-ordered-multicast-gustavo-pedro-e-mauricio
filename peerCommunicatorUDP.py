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
        print('Handler is ready. Waiting for the handshakes...')
        global handShakeCount
        global LIST_MESSAGES
        
        # Wait until handshakes are received from all other processes
        while handShakeCount < N:
            msgPack = self.sock.recv(1024)
            msg = pickle.loads(msgPack)
            if msg[0] == 'READY':
                handShakeCount += 1
                print('--- Handshake received: ', msg[1])

        print('Secondary Thread: Received all handshakes. Entering the loop to receive messages.')

        stopCount = 0 
        while True:                
            msgPack = self.sock.recv(1024)
            msg = pickle.loads(msgPack)
            if msg[0] == -1:   # count the 'stop' messages
                stopCount += 1
                if stopCount == N:
                    break
            else:
                print('Message ' + str(msg[1]) + ' from process ' + str(msg[0]))
                # Adiciona mensagem à lista global
                LIST_MESSAGES.append(msg)
        
        # Ordena todas as mensagens antes de enviar
        # Primeiro pelo ID do processo, depois pelo número da mensagem
        LIST_MESSAGES.sort(key=lambda x: (x[0], x[1]))
        
        # Write log file with ordered messages
        logFile = open('logfile'+str(myself)+'.log', 'w')
        logFile.writelines(str(LIST_MESSAGES))
        logFile.close()
        
        # Send the ordered list of messages to the server
        print('Sending the ordered list of messages to the server for comparison...')
        clientSock = socket(AF_INET, SOCK_STREAM)
        clientSock.connect((SERVER_ADDR, SERVER_PORT))
        msgPack = pickle.dumps(LIST_MESSAGES)
        clientSock.send(msgPack)
        clientSock.close()
        
        # Reset counters for next run
        handShakeCount = 0
        LIST_MESSAGES = []
        exit(0)

def waitToStart():
    (conn, addr) = serverSock.accept()
    msgPack = conn.recv(1024)
    msg = pickle.loads(msgPack)
    myself = msg[0]
    nMsgs = msg[1]
    conn.send(pickle.dumps('Peer process '+str(myself)+' started.'))
    conn.close()
    return (myself, nMsgs)

# Main execution
registerWithGroupManager()
while True:
    print('Waiting for signal to start...')
    (myself, nMsgs) = waitToStart()
    print('I am up, and my ID is: ', str(myself))

    if nMsgs == 0:
        print('Terminating.')
        exit(0)

    # Wait for other processes to be ready
    time.sleep(5)

    # Create receiving message handler
    msgHandler = MsgHandler(recvSocket)
    msgHandler.start()
    print('Handler started')

    PEERS = getListOfPeers()
    
    # Send handshakes
    for addrToSend in PEERS:
        print('Sending handshake to ', addrToSend)
        msg = ('READY', myself)
        msgPack = pickle.dumps(msg)
        sendSocket.sendto(msgPack, (addrToSend, PEER_UDP_PORT))

    print('Main Thread: Sent all handshakes. handShakeCount=', str(handShakeCount))

    while handShakeCount < N:
        pass  # wait for all handshakes

     for msgNumber in range(0, nMsgs):
        time.sleep(random.randrange(10,100)/1000)
        msg = (myself, msgNumber)
        msgPack = pickle.dumps(msg)
        for addrToSend in PEERS:
            sendSocket.sendto(msgPack, (addrToSend, PEER_UDP_PORT))
            print('Sent message ' + str(msgNumber))

     for addrToSend in PEERS:
        msg = (-1, -1)
        msgPack = pickle.dumps(msg)
        sendSocket.sendto(msgPack, (addrToSend, PEER_UDP_PORT))