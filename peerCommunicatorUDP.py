from socket import *
from constMP import *
import threading
import random
import time
import pickle
from requests import get
from collections import deque

# Global variables
handShakeCount = 0
PEERS = []
logical_clock = 0
message_queue = deque()
pending_acks = {}
received_messages = set()

# UDP sockets
sendSocket = socket(AF_INET, SOCK_DGRAM)
recvSocket = socket(AF_INET, SOCK_DGRAM)
recvSocket.bind(('0.0.0.0', PEER_UDP_PORT))

# TCP socket for start signal
serverSock = socket(AF_INET, SOCK_STREAM)
serverSock.bind(('0.0.0.0', PEER_TCP_PORT))
serverSock.listen(1)

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
    print('Getting list of peers from group manager: ', req)
    clientSock.send(msg)
    msg = clientSock.recv(2048)
    PEERS = pickle.loads(msg)
    print('Got list of peers: ', PEERS)
    clientSock.close()
    return PEERS

def update_logical_clock(received_clock):
    global logical_clock
    logical_clock = max(logical_clock, received_clock) + 1

def send_ack(dest_addr, msg_id, sender_clock):
    global logical_clock
    logical_clock += 1
    ack_msg = ('ACK', msg_id, logical_clock, myself)
    msgPack = pickle.dumps(ack_msg)
    sendSocket.sendto(msgPack, (dest_addr, PEER_UDP_PORT))

def process_message_queue():
    while True:
        if message_queue:
            # Sort queue by logical timestamp
            sorted_queue = sorted(message_queue, key=lambda x: x['timestamp'])
            next_msg = sorted_queue[0]
            
            # Check if we have all ACKs for this message
            if next_msg['id'] in pending_acks and len(pending_acks[next_msg['id']]) == N-1:
                message_queue.remove(next_msg)
                del pending_acks[next_msg['id']]
                
                # Deliver to application
                print(f"Delivering message {next_msg['id']} from process {next_msg['sender']}")
                return next_msg
        time.sleep(0.01)  # Small delay to prevent busy waiting

class MsgHandler(threading.Thread):
    def __init__(self, sock):
        threading.Thread.__init__(self)
        self.sock = sock

    def run(self):
        global handShakeCount, logical_clock
        
        print('Handler is ready. Waiting for the handshakes...')
        logList = []
        
        # Handshake phase
        while handShakeCount < N:
            msgPack = self.sock.recv(1024)
            msg = pickle.loads(msgPack)
            if msg[0] == 'READY':
                update_logical_clock(msg[2])  # Update clock with sender's timestamp
                handShakeCount += 1
                print(f'--- Handshake received from {msg[1]} with timestamp {msg[2]}')
                # Send ACK for handshake
                send_ack(msg[3], f"HS-{msg[1]}", logical_clock)

        print('Secondary Thread: Received all handshakes. Entering the loop to receive messages.')

        stopCount = 0
        while True:
            msgPack = self.sock.recv(1024)
            msg = pickle.loads(msgPack)
            
            # Update logical clock
            if msg[0] == 'ACK':
                received_clock = msg[2]
                update_logical_clock(received_clock)
                # Record ACK
                if msg[1] in pending_acks:
                    pending_acks[msg[1]].add(msg[3])  # msg[3] is sender ID
            elif msg[0] == 'DATA':
                received_clock = msg[3]  # msg[3] is sender's clock
                update_logical_clock(received_clock)
                
                # Create message entry
                msg_id = f"{msg[1]}-{msg[2]}"  # sender-seqnum
                if msg_id not in received_messages:
                    received_messages.add(msg_id)
                    message_entry = {
                        'type': 'DATA',
                        'id': msg_id,
                        'sender': msg[1],
                        'seqnum': msg[2],
                        'timestamp': received_clock,
                        'content': msg[4] if len(msg) > 4 else None
                    }
                    message_queue.append(message_entry)
                    pending_acks[msg_id] = set()
                    # Send ACK
                    send_ack(msg[5], msg_id, logical_clock)  # msg[5] is sender addr
            elif msg[0] == -1:  # stop message
                stopCount += 1
                if stopCount == N:
                    break
            
            # Process message queue
            delivered_msg = process_message_queue()
            if delivered_msg:
                logList.append((delivered_msg['sender'], delivered_msg['seqnum'], delivered_msg['timestamp']))

        # Write log file
        logFile = open(f'logfile{myself}.log', 'w')
        logFile.writelines(str(logList))
        logFile.close()

        # Send log to server
        print('Sending the list of messages to the server for comparison...')
        clientSock = socket(AF_INET, SOCK_STREAM)
        clientSock.connect((SERVER_ADDR, SERVER_PORT))
        msgPack = pickle.dumps(logList)
        clientSock.send(msgPack)
        clientSock.close()
        
        handShakeCount = 0
        exit(0)

def waitToStart():
    (conn, addr) = serverSock.accept()
    msgPack = conn.recv(1024)
    msg = pickle.loads(msgPack)
    myself = msg[0]
    nMsgs = msg[1]
    conn.send(pickle.dumps(f'Peer process {myself} started.'))
    conn.close()
    return (myself, nMsgs)

def send_data_messages(nMsgs):
    global logical_clock
    for msgNumber in range(nMsgs):
        # Update logical clock
        logical_clock += 1
        
        # Create message with timestamp
        msg_content = f"Message {msgNumber} from {myself}"
        for addrToSend in PEERS:
            if addrToSend != get_public_ip():  # Don't send to self
                msg = ('DATA', myself, msgNumber, logical_clock, msg_content, addrToSend)
                msgPack = pickle.dumps(msg)
                sendSocket.sendto(msgPack, (addrToSend, PEER_UDP_PORT))
                print(f'Sent message {msgNumber} with timestamp {logical_clock}')

def send_handshakes():
    global logical_clock
    for addrToSend in PEERS:
        if addrToSend != get_public_ip():  # Don't send to self
            logical_clock += 1
            msg = ('READY', myself, logical_clock, addrToSend)
            msgPack = pickle.dumps(msg)
            sendSocket.sendto(msgPack, (addrToSend, PEER_UDP_PORT))
            print(f'Sent handshake to {addrToSend} with timestamp {logical_clock}')

# Main execution
registerWithGroupManager()
while True:
    print('Waiting for signal to start...')
    (myself, nMsgs) = waitToStart()
    print(f'I am up, and my ID is: {myself}')

    if nMsgs == 0:
        print('Terminating.')
        exit(0)

    # Start message handler
    msgHandler = MsgHandler(recvSocket)
    msgHandler.start()
    print('Handler started')

    PEERS = getListOfPeers()
    
    # Send handshakes
    send_handshakes()
    print(f'Main Thread: Sent all handshakes. handShakeCount={handShakeCount}')

    # Wait for all handshakes
    while handShakeCount < N:
        time.sleep(0.1)

    # Send data messages
    send_data_messages(nMsgs)

    # Send stop messages
    for addrToSend in PEERS:
        if addrToSend != get_public_ip():
            msg = (-1, -1)
            msgPack = pickle.dumps(msg)
            sendSocket.sendto(msgPack, (addrToSend, PEER_UDP_PORT))