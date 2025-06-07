from socket  import *
from constMP import *
import threading
import random
import time
import pickle
from requests import get
import heapq

handShakeCount = 0
N = 2
PEERS = []

sendSocket = socket(AF_INET, SOCK_DGRAM)
recvSocket = socket(AF_INET, SOCK_DGRAM)
recvSocket.bind(('0.0.0.0', PEER_UDP_PORT))

serverSock = socket(AF_INET, SOCK_STREAM)
serverSock.bind(('0.0.0.0', PEER_TCP_PORT))
serverSock.listen(1)

logical_clock = 0
message_queue = [] # (timestamp, sender_id, message_type, message_content)
acks_received = {} # { (sender_id, message_number): [ack_timestamps] }
expected_next_message = {} # { sender_id: next_expected_sequence_number }
delivered_messages = []

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
  raw_peers = pickle.loads(msg)
  # Garante IPs únicos e mantém a ordem para consistência de IDs
  unique_peers = sorted(list(set(raw_peers)))
  print ('Lista de pares obtida: ', unique_peers)
  clientSock.close()
  return unique_peers

def update_logical_clock(received_timestamp):
  global logical_clock
  old_clock = logical_clock
  logical_clock = max(logical_clock, received_timestamp) + 1
  print(f'Relógio lógico atualizado: {old_clock} -> {logical_clock} (recebido: {received_timestamp})')

def deliver_message():
  global delivered_messages
  global message_queue
  global expected_next_message

  print(f'Tentando entregar mensagens. Fila atual ({len(message_queue)} mensagens): {message_queue}')
  # Processar todas as mensagens que estão prontas para serem entregues, respeitando a ordem do relógio lógico
  while message_queue and message_queue[0][0] <= logical_clock:
    timestamp, sender_id, message_type, message_content = heapq.heappop(message_queue)
    print(f'Mensagem removida da fila: Tipo={message_type}, Conteúdo={message_content}, Timestamp={timestamp}')
    
    if message_type == 'DATA':
      # Para mensagens DATA, verificar a ordem de sequência do remetente
      if message_content[1] == expected_next_message.get(sender_id, 0):
        print('Mensagem ' + str(message_content[1]) + ' do processo ' + str(message_content[0]) + ' ENTREGUE no tempo lógico ' + str(timestamp))
        delivered_messages.append(message_content)
        expected_next_message[sender_id] = expected_next_message.get(sender_id, 0) + 1
      else:
        print(f'Mensagem DATA {message_content[1]} de {message_content[0]} fora de ordem. Esperava {expected_next_message.get(sender_id, 0)}. Recolocando na fila.')
        heapq.heappush(message_queue, (timestamp, sender_id, message_type, message_content))
        break # Esperar pela mensagem correta
    else: # ACK, READY, ou CONTROL
      # Mensagens ACK, READY e CONTROL são processadas assim que chegam e são retiradas da fila
      print(f'Mensagem {message_type} processada no tempo lógico {timestamp}. Conteúdo: {message_content}')

class MsgHandler(threading.Thread):
  def __init__(self, sock):
    threading.Thread.__init__(self)
    self.sock = sock

  def run(self):
    global handShakeCount
    global logical_clock
    global message_queue
    global acks_received

    logList = []
    
    print('Handler está pronto. Aguardando handshakes...')
    # Loop para receber handshakes (mensagens READY)
    while handShakeCount < N:
      msgPack = self.sock.recv(1024)
      msg_data = pickle.loads(msgPack)
      
      msg_type, msg_content, msg_timestamp = msg_data
      print(f'Handshake recebido: Tipo={msg_type}, Conteúdo={msg_content}, Timestamp={msg_timestamp}')

      update_logical_clock(msg_timestamp)
      
      # Corrigido: Para READY, msg_content é o ID do remetente (int), não um tupla
      sender_id_for_queue = msg_content # msg_content é o ID do par
      heapq.heappush(message_queue, (logical_clock, sender_id_for_queue, msg_type, msg_content))
      print(f'Mensagem {msg_type} adicionada à fila com timestamp {logical_clock}.')
      deliver_message()

      if msg_type == 'READY':
        handShakeCount = handShakeCount + 1
        print('--- Handshake recebido de: ', msg_content) # msg_content é o ID do par

    print('Thread Secundária: Todos os handshakes recebidos. Entrando no loop para receber mensagens.')

    stopCount=0 
    while True:                
      msgPack = self.sock.recv(1024)
      msg_data = pickle.loads(msgPack)
      msg_type, msg_content, msg_timestamp = msg_data
      print(f'Mensagem recebida: Tipo={msg_type}, Conteúdo={msg_content}, Timestamp={msg_timestamp}')

      update_logical_clock(msg_timestamp)
      
      sender_id_for_queue = None
      if msg_type == 'DATA':
        sender_id_for_queue = msg_content[0] # ID do remetente original da mensagem de dados
      elif msg_type == 'ACK':
        sender_id_for_queue = msg_content[0] # ID do par que enviou o ACK
      elif msg_type == 'CONTROL':
        # Para mensagens CONTROL, msg_content é -1, que não é um ID de remetente
        # Usamos uma string para evitar erro de subscript e indicar que é uma msg de controle
        sender_id_for_queue = 'CONTROL_MSG'
      
      heapq.heappush(message_queue, (logical_clock, sender_id_for_queue, msg_type, msg_content))
      print(f'Mensagem {msg_type} adicionada à fila com timestamp {logical_clock}.')
      
      if msg_type == 'DATA':
        if msg_content[0] not in expected_next_message:
          expected_next_message[msg_content[0]] = 0
        
        logical_clock += 1 # Incrementa o relógio lógico antes de enviar o ACK
        ack_msg = ('ACK', (myself, msg_content[0], msg_content[1]), logical_clock)
        ack_msg_pack = pickle.dumps(ack_msg)
        # Envia ACK para o remetente original da mensagem DATA
        sendSocket.sendto(ack_msg_pack, (PEERS[msg_content[0]], PEER_UDP_PORT))
        print('ACK enviado para mensagem ' + str(msg_content[1]) + ' de ' + str(msg_content[0]) + ' com timestamp ' + str(logical_clock))
      elif msg_type == 'ACK':
        sender_id, original_sender_id, message_number = msg_content
        if (original_sender_id, message_number) not in acks_received:
          acks_received[(original_sender_id, message_number)] = []
        acks_received[(original_sender_id, message_number)].append(msg_timestamp)
        print('ACK recebido para mensagem ' + str(message_number) + ' do remetente original ' + str(original_sender_id) + ' enviado por ' + str(sender_id) + ' com timestamp ' + str(msg_timestamp))
      elif msg_type == 'CONTROL' and msg_content == -1:
        print(f'Mensagem de controle (-1) recebida de {sender_id_for_queue}.')
        stopCount = stopCount + 1
        if stopCount == N:
          print('Todas as mensagens de parada recebidas. Encerrando o handler.')
          break
      
      deliver_message() # Tenta entregar mensagens após cada recebimento
        
    logFile = open('logfile'+str(myself)+'.log', 'w')
    logFile.writelines(str(delivered_messages))
    logFile.close()
    
    print('Enviando a lista de mensagens para o servidor para comparação...')
    clientSock = socket(AF_INET, SOCK_STREAM)
    clientSock.connect((SERVER_ADDR, SERVER_PORT))
    msgPack = pickle.dumps(delivered_messages)
    clientSock.send(msgPack)
    clientSock.close()
    
    handShakeCount = 0

    exit(0)

def waitToStart():
  (conn, addr) = serverSock.accept()
  msgPack = conn.recv(1024)
  msg = pickle.loads(msgPack)
  myself_id = msg[0]
  nMsgs = msg[1]
  conn.send(pickle.dumps('Processo par '+str(myself_id)+' iniciado.'))
  conn.close()
  return (myself_id,nMsgs)

registerWithGroupManager()
while 1:
  print('Aguardando sinal para iniciar...')
  (myself, nMsgs) = waitToStart()
  print('Estou ativo, e meu ID é: ', str(myself))

  if nMsgs == 0:
    print('Terminando.')
    exit(0)

  msgHandler = MsgHandler(recvSocket)
  msgHandler.start()
  print('Handler iniciado.')

  PEERS = getListOfPeers()
  
  for addrToSend in PEERS:
    logical_clock += 1 # Incrementa o relógio lógico antes de enviar a mensagem
    msg = ('READY', myself, logical_clock)
    msgPack = pickle.dumps(msg)
    sendSocket.sendto(msgPack, (addrToSend,PEER_UDP_PORT))
    print(f'Enviando handshake READY para {addrToSend} com timestamp {logical_clock}')

  print('Thread Principal: Todos os handshakes enviados. handShakeCount=', str(handShakeCount))

  while (handShakeCount < N):
    pass # Espera passivamente pelo contador de handshakes ser atualizado pelo handler
  print('Thread Principal: Todos os handshakes recebidos pelo handler. Iniciando envio de mensagens de dados.')

  for msgNumber in range(0, nMsgs):
    logical_clock += 1 # Incrementa o relógio lógico antes de enviar a mensagem
    msg = ('DATA', (myself, msgNumber), logical_clock)
    msgPack = pickle.dumps(msg)
    for i, addrToSend in enumerate(PEERS):
      # Verifica se o endereço na lista PEERS corresponde ao seu próprio IP para não enviar para si mesmo
      # ou se o ID do par na lista PEERS não é o seu próprio ID
      if PEERS[myself] != addrToSend: 
        sendSocket.sendto(msgPack, (addrToSend,PEER_UDP_PORT))
        print('Mensagem DATA ' + str(msgNumber) + ' enviada para ' + str(addrToSend) + ' com timestamp ' + str(logical_clock))
    
    ack_key = (myself, msgNumber)
    start_time = time.time()
    # Espera por ACKs de todos os outros pares, ou seja, N-1 ACKs
    print(f'Aguardando ACKs para mensagem {msgNumber} (esperando {len(PEERS) -1} ACKs).')
    # Espera até que todos os ACKs sejam recebidos ou que o tempo limite seja atingido
    while len(acks_received.get(ack_key, [])) < (len(PEERS) - 1):
        if time.time() - start_time > 5: # Tempo limite de 5 segundos
            print(f"Tempo limite excedido ao aguardar ACKs para a mensagem {msgNumber}. Recebido {len(acks_received.get(ack_key, []))} de {len(PEERS)-1} ACKs.")
            break
    print(f'Todos os ACKs recebidos (ou tempo limite) para a mensagem {msgNumber}.')

  # Envia mensagens de controle para sinalizar o fim da comunicação
  for addrToSend in PEERS:
    logical_clock += 1 # Incrementa o relógio lógico antes de enviar a mensagem
    msg = ('CONTROL', -1, logical_clock)
    msgPack = pickle.dumps(msg)
    sendSocket.sendto(msgPack, (addrToSend,PEER_UDP_PORT))
    print(f'Enviando mensagem de controle (-1) para {addrToSend} com timestamp {logical_clock}.')
