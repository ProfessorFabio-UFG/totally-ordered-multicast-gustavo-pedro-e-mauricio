from socket import *
import threading
import random
import time
import pickle
from requests import get
from constMP import *

handShakeCount = 0
PEERS = []
sendSocket = socket(AF_INET, SOCK_DGRAM)
recvSocket = socket(AF_INET, SOCK_DGRAM)
recvSocket.bind(('0.0.0.0', PEER_UDP_PORT))
serverSock = socket(AF_INET, SOCK_STREAM)
serverSock.bind(('0.0.0.0', PEER_TCP_PORT))
serverSock.listen(1)

logical_clock = 0
message_queue = []
myself = -1

def get_public_ip():
  ipAddr = get('https://api.ipify.org').content.decode('utf8')
  return ipAddr

def registerWithGroupManager():
  clientSock = socket(AF_INET, SOCK_STREAM)
  clientSock.connect((GROUPMNGR_ADDR,GROUPMNGR_TCP_PORT))
  ipAddr = get_public_ip()
  req = {"op":"register", "ipaddr":ipAddr, "port":PEER_UDP_PORT}
  msg = pickle.dumps(req)
  clientSock.send(msg)
  clientSock.close()

def getListOfPeers():
  clientSock = socket(AF_INET, SOCK_STREAM)
  clientSock.connect((GROUPMNGR_ADDR,GROUPMNGR_TCP_PORT))
  req = {"op":"list"}
  msg = pickle.dumps(req)
  clientSock.send(msg)
  msg = clientSock.recv(2048)
  peers = pickle.loads(msg)
  clientSock.close()
  return peers

def update_logical_clock(received_timestamp):
  global logical_clock
  logical_clock = max(logical_clock, received_timestamp) + 1

def deliver_messages():
  global message_queue
  message_queue.sort(key=lambda x: x['timestamp'])
  
  delivered_messages = []
  i = 0
  while i < len(message_queue):
      msg = message_queue[i]
      if 'sender_id' in msg and 'timestamp' in msg and msg['sender_id'] != myself and msg['timestamp'] <= logical_clock:
          delivered_messages.append(msg)
          message_queue.pop(i)
      else:
          i += 1
  return delivered_messages

class MsgHandler(threading.Thread):
  def __init__(self, sock):
    threading.Thread.__init__(self)
    self.sock = sock

  def run(self):
    global handShakeCount
    global logical_clock
    logList = []
    
    while handShakeCount < N:
      msgPack = self.sock.recv(1024)
      msg = pickle.loads(msgPack)
      if msg[0] == 'READY':
        update_logical_clock(msg[2])
        handShakeCount = handShakeCount + 1
        ack_msg = ('ACK_HANDSHAKE', myself, logical_clock)
        sendSocket.sendto(pickle.dumps(ack_msg), (msg[3], PEER_UDP_PORT))

    stopCount = 0 
    while True:                
      msgPack = self.sock.recv(1024)   
      msg = pickle.loads(msgPack)

      update_logical_clock(msg[2])

      if msg[0] == -1:   
        stopCount = stopCount + 1
        if stopCount == N:
          break 
      elif msg[0] == 'ACK':
        pass
      elif msg[0] == 'ACK_HANDSHAKE':
        pass 
      else:
        # Assuming data messages are structured as (sender_id, msg_number, timestamp, sender_ip)
        # Store as a dictionary for easier access to fields and future extensibility
        processed_msg = {
            'sender_id': msg[0],
            'msg_number': msg[1],
            'timestamp': msg[2],
            'sender_ip': msg[3]
        }
        message_queue.append(processed_msg)
        
        ack_msg = ('ACK', myself, logical_clock, get_public_ip())
        sendSocket.sendto(pickle.dumps(ack_msg), (msg[3], PEER_UDP_PORT))
        
        delivered = deliver_messages()
        for d_msg in delivered:
            logList.append(d_msg)
        
    logFile = open('logfile'+str(myself)+'.log', 'w')
    logFile.writelines(str(logList))
    logFile.close()
    
    clientSock = socket(AF_INET, SOCK_STREAM)
    clientSock.connect((SERVER_ADDR, SERVER_PORT))
    msgPack = pickle.dumps(logList)
    clientSock.send(msgPack)
    clientSock.close()
    
    handShakeCount = 0
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
  (myself, nMsgs) = waitToStart()

  if nMsgs == 0:
    exit(0)

  msgHandler = MsgHandler(recvSocket)
  msgHandler.start()

  PEERS = getListOfPeers()
  
  my_public_ip = get_public_ip() # Get IP once to avoid multiple calls

  for addrToSend in PEERS:
    logical_clock += 1
    msg = ('READY', myself, logical_clock, my_public_ip)
    msgPack = pickle.dumps(msg)
    sendSocket.sendto(msgPack, (addrToSend,PEER_UDP_PORT))

  while (handShakeCount < N):
    pass  

  for msgNumber in range(0, nMsgs):
    logical_clock += 1
    # Include sender's IP address in data messages too
    msg = (myself, msgNumber, logical_clock, my_public_ip)
    msgPack = pickle.dumps(msg)
    for addrToSend in PEERS:
      sendSocket.sendto(msgPack, (addrToSend,PEER_UDP_PORT))

  for addrToSend in PEERS:
    logical_clock += 1
    msg = (-1,-1, logical_clock, my_public_ip) # Include IP for stop messages
    msgPack = pickle.dumps(msg)
    sendSocket.sendto(msgPack, (addrToSend,PEER_UDP_PORT))