from socket import *
import threading
import random
import time
import pickle
from requests import get
import heapq

PEER_UDP_PORT = 6789
PEER_TCP_PORT = 5679
N = 6
SERVER_ADDR = '18.246.153.223'
SERVER_PORT = 5678
GROUPMNGR_ADDR = '18.246.153.223'
GROUPMNGR_TCP_PORT = 5680

handShakeCount = 0
PEERS = []
myself = -1
logical_clock = 0
message_queue = []
acks_received = {}
expected_acks = {}

sendSocket = socket(AF_INET, SOCK_DGRAM)
recvSocket = socket(AF_INET, SOCK_DGRAM)
recvSocket.bind(('0.0.0.0', PEER_UDP_PORT))

serverSock = socket(AF_INET, SOCK_STREAM)
serverSock.bind(('0.0.0.0', PEER_TCP_PORT))
serverSock.listen(1)

def get_public_ip():
  ipAddr = get('https://api.ipify.org').content.decode('utf8')
  print('My public IP address is: {}'.format(ipAddr))
  return ipAddr

def registerWithGroupManager():
  clientSock = socket(AF_INET, SOCK_STREAM)
  print ('Connecting to group manager: ', (GROUPMNGR_ADDR,GROUPMNGR_TCP_PORT))
  clientSock.connect((GROUPMNGR_ADDR,GROUPMNGR_TCP_PORT))
  ipAddr = get_public_ip()
  req = {"op":"register", "ipaddr":ipAddr, "port":PEER_UDP_PORT}
  msg = pickle.dumps(req)
  print ('Registering with group manager: ', req)
  clientSock.send(msg)
  clientSock.close()

def getListOfPeers():
  clientSock = socket(AF_INET, SOCK_STREAM)
  print ('Connecting to group manager: ', (GROUPMNGR_ADDR,GROUPMNGR_TCP_PORT))
  clientSock.connect((GROUPMNGR_ADDR,GROUPMNGR_TCP_PORT))
  req = {"op":"list"}
  msg = pickle.dumps(req)
  print ('Getting list of peers from group manager: ', req)
  clientSock.send(msg)
  msg = clientSock.recv(2048)
  peers_list = pickle.loads(msg)
  print ('Got list of peers: ', peers_list)
  clientSock.close()
  return peers_list

class MsgHandler(threading.Thread):
  def __init__(self, sock):
    threading.Thread.__init__(self)
    self.sock = sock

  def run(self):
    print('Handler is ready. Waiting for the handshakes...')
    
    global handShakeCount
    global logical_clock
    global message_queue
    global myself
    
    logList = []
    
    while handShakeCount < N:
      msgPack, addr = self.sock.recvfrom(1024)
      msg = pickle.loads(msgPack)
      if msg[0] == 'READY':
        logical_clock = max(logical_clock, msg[2]) + 1
        print(f"Updated logical clock after handshake: {logical_clock} (received: {msg[2]})")

        handShakeCount += 1
        print('--- Handshake received: ', msg[1])
        logical_clock += 1
        ack_msg = ('ACK_READY', myself, logical_clock)
        sendSocket.sendto(pickle.dumps(ack_msg), (addr[0], PEER_UDP_PORT))
        print(f"Sent ACK_READY to {addr[0]} with timestamp {logical_clock}")
      elif msg[0] == 'ACK_READY':
        pass 
      else:
        print(f"Received non-handshake message during handshake phase. Queuing: {msg}")
        if msg[0] == 'DATA' or msg[0] == 'ACK':
          logical_clock = max(logical_clock, msg[3]) + 1
          print(f"Updated logical clock after data/ACK: {logical_clock} (received: {msg[3]})")
          heapq.heappush(message_queue, (msg[3], msg[1], msg[2], msg[0]))
          if msg[0] == 'DATA':
            logical_clock += 1
            ack_msg = ('ACK', myself, msg[1], logical_clock, msg[2])
            sendSocket.sendto(pickle.dumps(ack_msg), (addr[0], PEER_UDP_PORT))
            print(f"Sent ACK for DATA from {msg[1]} msg {msg[2]} to {addr[0]} with timestamp {logical_clock}")


    print('Secondary Thread: Received all handshakes. Entering the loop to receive messages.')

    stopCount = 0
    processed_messages = set()

    while True:                
      msgPack, addr = self.sock.recvfrom(1024)
      msg = pickle.loads(msgPack)
      
      if isinstance(msg, tuple) and len(msg) >= 4:
          logical_clock = max(logical_clock, msg[3]) + 1
          print(f"Updated logical clock: {logical_clock} (received: {msg[3]})")

      if msg[0] == -1:
        stopCount += 1
        if stopCount == N:
          break
      elif msg[0] == 'DATA':
        heapq.heappush(message_queue, (msg[3], msg[1], msg[2], msg[0]))
        print(f'Received DATA message {msg[2]} from process {msg[1]} with timestamp {msg[3]}. Added to queue.')
        
        logical_clock += 1
        ack_msg = ('ACK', myself, msg[1], logical_clock, msg[2])
        sendSocket.sendto(pickle.dumps(ack_msg), (addr[0], PEER_UDP_PORT))
        print(f"Sent ACK for DATA from {msg[1]} msg {msg[2]} to {addr[0]} with timestamp {logical_clock}")

      elif msg[0] == 'ACK':
        ack_sender_id = msg[1]
        original_sender_id = msg[2]
        original_msg_number = msg[4]
        
        if (original_sender_id, original_msg_number) in expected_acks.get(ack_sender_id, set()):
            expected_acks[ack_sender_id].discard((original_sender_id, original_msg_number))
            print(f"Received ACK from {ack_sender_id} for message {original_msg_number} from {original_sender_id}")

        if (addr[0], PEER_UDP_PORT) not in acks_received:
            acks_received[(addr[0], PEER_UDP_PORT)] = {}
        acks_received[(addr[0], PEER_UDP_PORT)][(original_sender_id, original_msg_number)] = True

      else:
        print(f"Unknown message type received: {msg[0]}")

      while message_queue and message_queue[0][0] <= logical_clock:
          
          timestamp, sender_id, msg_number, msg_type = heapq.heappop(message_queue)
          
          if (sender_id, msg_number, timestamp) in processed_messages:
              print(f"Skipping duplicate message: {sender_id}, {msg_number}, {timestamp}")
              continue
              
          processed_messages.add((sender_id, msg_number, timestamp))

          if msg_type == 'DATA':
              print(f'Delivered message {msg_number} from process {sender_id} with timestamp {timestamp}')
              logList.append((sender_id, msg_number, timestamp))
          elif msg_type == 'ACK':
              pass

    logFile = open('logfile'+str(myself)+'.log', 'w')
    logFile.writelines([str(item) + '\n' for item in logList])
    logFile.close()
    
    print('Sending the list of messages to the server for comparison...')
    clientSock = socket(AF_INET, SOCK_STREAM)
    clientSock.connect((SERVER_ADDR, SERVER_PORT))
    msgPack = pickle.dumps(logList)
    clientSock.send(msgPack)
    clientSock.close()
    
    handShakeCount = 0
    message_queue.clear()
    processed_messages.clear()
    acks_received.clear()
    expected_acks.clear()
    logical_clock = 0

    exit(0)

def waitToStart():
  global myself
  (conn, addr) = serverSock.accept()
  msgPack = conn.recv(1024)
  msg = pickle.loads(msgPack)
  myself = msg[0]
  nMsgs = msg[1]
  conn.send(pickle.dumps('Peer process '+str(myself)+' started.'))
  conn.close()
  return (myself,nMsgs)

registerWithGroupManager()
while 1:
  print('Waiting for signal to start...')
  (myself, nMsgs) = waitToStart()
  print('I am up, and my ID is: ', str(myself))

  if nMsgs == 0:
    print('Terminating.')
    exit(0)

  msgHandler = MsgHandler(recvSocket)
  msgHandler.start()
  print('Handler started')

  PEERS = getListOfPeers()
  
  handshake_acks_pending = set()
  for addrToSend in PEERS:
    if addrToSend == get_public_ip():
        continue
    logical_clock += 1
    print(f"Incremented logical clock for handshake: {logical_clock}")
    msg = ('READY', myself, logical_clock)
    msgPack = pickle.dumps(msg)
    sendSocket.sendto(msgPack, (addrToSend,PEER_UDP_PORT))
    print(f'Sending handshake to {addrToSend} with timestamp {logical_clock}')
    handshake_acks_pending.add(addrToSend)

  while len(handshake_acks_pending) < (N - 1):
    pass

  print(f'Main Thread: Received all handshakes. handShakeCount={handShakeCount}')

  for msgNumber in range(0, nMsgs):
    time.sleep(random.randrange(10,100)/1000)

    logical_clock += 1
    print(f"Incremented logical clock for data message: {logical_clock}")
    msg = ('DATA', myself, msgNumber, logical_clock)
    msgPack = pickle.dumps(msg)
    
    expected_acks[(myself, msgNumber)] = set()

    for addrToSend in PEERS:
      if addrToSend == get_public_ip():
        continue
      sendSocket.sendto(msgPack, (addrToSend,PEER_UDP_PORT))
      print(f'Sent DATA message {msgNumber} to {addrToSend} with timestamp {logical_clock}')
      expected_acks[(myself, msgNumber)].add((addrToSend, PEER_UDP_PORT))
      
    while expected_acks.get((myself, msgNumber)) and len(expected_acks[(myself, msgNumber)]) > 0:
        pass

  for addrToSend in PEERS:
    if addrToSend == get_public_ip():
        continue
    logical_clock += 1
    msg = (-1,-1, logical_clock)
    msgPack = pickle.dumps(msg)
    sendSocket.sendto(msgPack, (addrToSend,PEER_UDP_PORT))
    print(f"Sent stop message to {addrToSend}")

  msgHandler.join()