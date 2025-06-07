from socket import *
from constMP import *
import threading
import random
import pickle
from requests import get
import heapq

handShakeCount = 0
PEERS = []
logical_clock = 0
message_queue = [] # Min-heap to store messages based on their timestamp
expected_acks = {} # Dictionary to keep track of acks for sent messages
received_acks = {} # Dictionary to store received acks for sent messages
messages_to_process = threading.Event()
myself = -1
nMsgs = -1

sendSocket = socket(AF_INET, SOCK_DGRAM)
recvSocket = socket(AF_INET, SOCK_DGRAM)
recvSocket.bind(('0.0.0.0', PEER_UDP_PORT))

serverSock = socket(AF_INET, SOCK_STREAM)
serverSock.bind(('0.0.0.0', PEER_TCP_PORT))
serverSock.listen(1)

def get_public_ip():
  ipAddr = get('https://api.ipify.org').content.decode('utf8')
  print('Meu endereço IP público é: {}'.format(ipAddr))
  return ipAddr

def registerWithGroupManager():
  clientSock = socket(AF_INET, SOCK_STREAM)
  print ('Conectando ao gerenciador de grupo: ', (GROUPMNGR_ADDR,GROUPMNGR_TCP_PORT))
  clientSock.connect((GROUPMNGR_ADDR,GROUPMNGR_TCP_PORT))
  ipAddr = get_public_ip()
  req = {"op":"register", "ipaddr":ipAddr, "port":PEER_UDP_PORT}
  msg = pickle.dumps(req)
  print ('Registrando com o gerenciador de grupo: ', req)
  clientSock.send(msg)
  clientSock.close()

def getListOfPeers():
  clientSock = socket(AF_INET, SOCK_STREAM)
  print ('Conectando ao gerenciador de grupo: ', (GROUPMNGR_ADDR,GROUPMNGR_TCP_PORT))
  clientSock.connect((GROUPMNGR_ADDR,GROUPMNGR_TCP_PORT))
  req = {"op":"list"}
  msg = pickle.dumps(req)
  print ('Obtendo lista de pares do gerenciador de grupo: ', req)
  clientSock.send(msg)
  msg = clientSock.recv(2048)
  PEERS = pickle.loads(msg)
  print ('Lista de pares obtida: ', PEERS)
  clientSock.close()
  return PEERS

def update_logical_clock(received_timestamp=0):
  global logical_clock
  logical_clock = max(logical_clock, received_timestamp) + 1

class MsgHandler(threading.Thread):
  def __init__(self, sock):
    threading.Thread.__init__(self)
    self.sock = sock
    self.logList = []
    self.stop_event = threading.Event()

  def run(self):
    print('Manipulador pronto. Aguardando handshakes...')
    
    global handShakeCount
    global logical_clock
    global myself

    while handShakeCount < N:
      msgPack = self.sock.recv(1024)
      msg_type, sender_id, msg_timestamp, _ = pickle.loads(msgPack)
      update_logical_clock(msg_timestamp)

      if msg_type == 'READY':
        handShakeCount += 1
        print('--- Handshake recebido: ', sender_id, ' Clock:', logical_clock)
        # Send handshake ACK
        ack_msg = ('ACK_READY', myself, logical_clock, sender_id)
        sendSocket.sendto(pickle.dumps(ack_msg), (sender_id, PEER_UDP_PORT))
      elif msg_type == 'ACK_READY':
        # This ACK means a handshake sent by me was acknowledged
        print(f"--- ACK_READY recebido de {sender_id}. Meu clock: {logical_clock}")

    print('Thread Secundária: Recebeu todos os handshakes. Entrando no loop para receber mensagens.')

    stopCount = 0 
    while not self.stop_event.is_set() or stopCount < N:                
      try:
        msgPack = self.sock.recv(1024)
        msg_type, sender_id, msg_timestamp, content = pickle.loads(msgPack)
        update_logical_clock(msg_timestamp)

        if msg_type == -1:
          stopCount += 1
          print(f"Mensagem de parada de {sender_id}. Contagem de paradas: {stopCount}/{N}")
          if stopCount == N:
            self.stop_event.set()
            break
        elif msg_type == 'DATA':
          # 1. Atualiza o relógio lógico (já feito acima)
          # 2. Coloca a mensagem na fila
          heapq.heappush(message_queue, (msg_timestamp, sender_id, content))
          print(f"Mensagem DATA {content} de {sender_id} (clock: {msg_timestamp}) enfileirada. Meu clock: {logical_clock}")
          # 3. Enviar ACK
          ack_msg = ('ACK', myself, logical_clock, (sender_id, content)) # ACK para a mensagem específica
          sendSocket.sendto(pickle.dumps(ack_msg), (sender_id, PEER_UDP_PORT))
        elif msg_type == 'ACK':
          # 1. Atualiza o relógio lógico (já feito acima)
          # 2. Coloca a mensagem na fila (ACKs não precisam de ACKs, mas registramos)
          original_sender, original_content = content
          if (original_sender, original_content) in received_acks:
            received_acks[(original_sender, original_content)].add(sender_id)
            print(f"ACK para DATA {original_content} de {original_sender} recebido de {sender_id}. Meu clock: {logical_clock}")
            messages_to_process.set() # Sinaliza que pode haver mensagens para processar
          else:
            print(f"ACK para mensagem desconhecida ({original_sender}, {original_content}) de {sender_id}. Meu clock: {logical_clock}")

      except BlockingIOError:
        pass # No data available, continue loop
      except Exception as e:
        print(f"Erro ao receber mensagem: {e}")
    
    # Process remaining messages in queue before finishing
    while message_queue:
      self.process_next_message()

    # Write log file
    logFile = open('logfile'+str(myself)+'.log', 'w')
    logFile.writelines(str(self.logList))
    logFile.close()
    
    # Send the list of messages to the server (using a TCP socket) for comparison
    print('Enviando a lista de mensagens para o servidor para comparação...')
    clientSock = socket(AF_INET, SOCK_STREAM)
    clientSock.connect((SERVER_ADDR, SERVER_PORT))
    msgPack = pickle.dumps(self.logList)
    clientSock.send(msgPack)
    clientSock.close()
    
    handShakeCount = 0
    logical_clock = 0
    print("Manipulador encerrado.")
    exit(0)

  def process_next_message(self):
    global PEERS
    global myself

    while message_queue:
      next_msg_timestamp, next_msg_sender, next_msg_content = heapq.nsmallest(1, message_queue)[0]
      message_id = (next_msg_sender, next_msg_content)

      # Check if all expected ACKs for this message have been received
      if message_id in expected_acks:
        if expected_acks[message_id].issubset(received_acks.get(message_id, set())):
          # Message is ready to be delivered
          timestamp, sender, content = heapq.heappop(message_queue)
          print(f'Mensagem {content} de processo {sender} (clock: {timestamp}) entregue à aplicação. Meu clock: {logical_clock}')
          self.logList.append((sender, content, timestamp))
          # Clear ACKs related to this message
          if message_id in expected_acks:
            del expected_acks[message_id]
          if message_id in received_acks:
            del received_acks[message_id]
        else:
          # Not all ACKs received, wait
          return
      else:
        # If no expected_acks entry, it means it's an ACK or a message sent by myself
        # (which doesn't require ACKs from others)
        # This part might need refinement based on exact message types and processing logic
        timestamp, sender, content = heapq.heappop(message_queue)
        print(f'Mensagem {content} de processo {sender} (clock: {timestamp}) entregue à aplicação (sem ACK esperado). Meu clock: {logical_clock}')
        self.logList.append((sender, content, timestamp))


# Function to wait for start signal from comparison server:
def waitToStart():
  global myself, nMsgs
  (conn, addr) = serverSock.accept()
  msgPack = conn.recv(1024)
  msg = pickle.loads(msgPack)
  myself = msg[0]
  nMsgs = msg[1]
  conn.send(pickle.dumps('Processo peer '+str(myself)+' iniciado.'))
  conn.close()
  return (myself,nMsgs)

registerWithGroupManager()
while True:
  print('Aguardando sinal para iniciar...')
  (myself, nMsgs) = waitToStart()
  print('Estou pronto, e meu ID é: ', str(myself))

  if nMsgs == 0:
    print('Finalizando.')
    break

  # Create receiving message handler
  msgHandler = MsgHandler(recvSocket)
  msgHandler.start()
  print('Manipulador iniciado')

  PEERS = getListOfPeers()
  
  # Send handshakes and wait for ACKs
  for addrToSend in PEERS:
    if addrToSend != get_public_ip(): # Don't send handshake to myself
      update_logical_clock()
      msg = ('READY', myself, logical_clock, None)
      msgPack = pickle.dumps(msg)
      sendSocket.sendto(msgPack, (addrToSend,PEER_UDP_PORT))

  # Wait for all handshakes to be received (including their ACKs)
  # The MsgHandler handles the handShakeCount, so main thread just waits for it to reach N
  while (handShakeCount < N -1): # -1 because I don't handshake myself
    pass

  print('Thread Principal: Handshakes enviados. handShakeCount=', str(handShakeCount))

  # Send a sequence of data messages to all other processes 
  for msgNumber in range(0, nMsgs):
    update_logical_clock()
    msg_content = msgNumber
    message_id = (myself, msg_content)
    expected_acks[message_id] = set(peer for peer in PEERS if peer != get_public_ip())
    received_acks[message_id] = set()

    msg = ('DATA', myself, logical_clock, msg_content)
    msgPack = pickle.dumps(msg)
    for addrToSend in PEERS:
      if addrToSend != get_public_ip(): # Don't send to myself
        sendSocket.sendto(msgPack, (addrToSend,PEER_UDP_PORT))
    
    print(f'Enviada mensagem DATA {msg_content} (clock: {logical_clock}). Aguardando ACKs.')

    # Wait for all ACKs for this specific message before sending the next
    while len(received_acks.get(message_id, set())) < len(expected_acks.get(message_id, set())):
      messages_to_process.wait(0.1) # Wait briefly, allowing MsgHandler to receive ACKs
      msgHandler.process_next_message() # Try to process messages in queue

  print('Thread Principal: Todas as mensagens de dados enviadas. Aguardando ACKs finais e processamento da fila.')

  # Ensure all ACKs are received and messages processed before sending stop signals
  # This part can be tricky to get right without blocking
  # For now, we will rely on the MsgHandler to eventually process everything.
  # A more robust solution might involve specific signaling for this.

  # Tell all processes that I have no more messages to send
  for addrToSend in PEERS:
    if addrToSend != get_public_ip(): # Don't send to myself
      update_logical_clock()
      msg = (-1, myself, logical_clock, None) # Type -1 for stop signal
      msgPack = pickle.dumps(msg)
      sendSocket.sendto(msgPack, (addrToSend,PEER_UDP_PORT))
  
  print('Thread Principal: Sinais de parada enviados.')

  # Wait for the message handler to finish processing all messages and send logs
  msgHandler.join()
  print('Thread Principal: Manipulador de mensagens finalizado.')
