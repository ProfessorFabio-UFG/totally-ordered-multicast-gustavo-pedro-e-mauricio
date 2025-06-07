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
message_queue = [] # Min-heap para armazenar mensagens com base no seu timestamp
expected_acks = {} # Dicionário para rastrear os ACKs para mensagens enviadas
received_acks = {} # Dicionário para armazenar os ACKs recebidos para mensagens enviadas
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

    while handShakeCount < N - 1: # Aguarda handshakes de N-1 outros pares
      msgPack = self.sock.recv(1024)
      # Desempacota a mensagem, esperando o ID numérico do remetente e o endereço IP
      msg_type, sender_numerical_id, msg_timestamp, sender_ip_address = pickle.loads(msgPack)
      update_logical_clock(msg_timestamp)

      if msg_type == 'READY':
        handShakeCount += 1
        print(f'--- Handshake recebido de ID: {sender_numerical_id}, IP: {sender_ip_address}, Clock: {logical_clock}')
        # Envia ACK de handshake de volta para o endereço IP do remetente
        ack_msg = ('ACK_READY', myself, logical_clock, sender_numerical_id) # Conteúdo é o ID numérico do peer cujo handshake está sendo ACKado
        sendSocket.sendto(pickle.dumps(ack_msg), (sender_ip_address, PEER_UDP_PORT))
      elif msg_type == 'ACK_READY':
        # Este ACK significa que um handshake enviado por mim foi reconhecido
        # O 'content' de ACK_READY é o ID numérico do peer que enviou o READY original
        acked_peer_numerical_id = sender_numerical_id # O 'sender_id' do ACK é o 'myself' do peer que ACKou
        print(f"--- ACK_READY recebido de {sender_numerical_id}. Meu clock: {logical_clock}")

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
          # Para enviar o ACK, precisamos do IP do remetente original. Assumimos que PEERS lista de IPs
          # Para simplificar, o peer que enviou a mensagem de dados (sender_id) é um ID numérico.
          # O sistema atual não tem um mapeamento direto de sender_id para IP aqui.
          # No entanto, como o ACK_READY já está enviando para o IP, vamos assumir que para DATA,
          # o 'sender_id' na mensagem DATA é o IP. Se 'sender_id' for o ID numérico, teremos um problema similar.
          # Pela estrutura atual, 'sender_id' de DATA é o ID numérico do peer.
          # PRECISAMOS SABER O IP DO REMETENTE DA MENSAGEM DATA PARA ENVIAR O ACK!
          # Para resolver isso, a mensagem DATA também deveria conter o IP do remetente.
          # Por agora, vamos usar PEERS[sender_id] se os IDs numéricos corresponderem aos índices da lista PEERS.
          # Se não, precisaríamos de um mapeamento explícito ou de outra estrutura.

          # ASSUMIMOS AQUI QUE PEERS É UMA LISTA DE IPS E QUE O ID NUMÉRICO DO REMETENTE (sender_id)
          # PODE SER USADO COMO ÍNDICE PARA OBTER SEU IP. ISSO PODE NÃO SER GARANTIDO NO CENÁRIO REAL.
          # Uma solução mais robusta seria a mensagem DATA conter o IP de origem.
          if sender_id < len(PEERS): # Se sender_id é um ID numérico que corresponde a um índice na lista PEERS
              sendSocket.sendto(pickle.dumps(ack_msg), (PEERS[sender_id], PEER_UDP_PORT))
          else: # Se sender_id não é um ID numérico, ou não corresponde a um índice
              print(f"Erro: Não foi possível enviar ACK para {sender_id}. IP do remetente de DATA não encontrado.")

        elif msg_type == 'ACK':
          # 1. Atualiza o relógio lógico (já feito acima)
          # 2. Coloca a mensagem na fila (ACKs não precisam de ACKs, mas registramos)
          original_sender, original_content = content
          if (original_sender, original_content) not in received_acks:
              received_acks[(original_sender, original_content)] = set()
          received_acks[(original_sender, original_content)].add(sender_id)
          print(f"ACK para DATA {original_content} de {original_sender} recebido de {sender_id}. Meu clock: {logical_clock}")
          messages_to_process.set() # Sinaliza que pode haver mensagens para processar

      except BlockingIOError:
        pass # Nenhum dado disponível, continua o loop
      except Exception as e:
        print(f"Erro ao receber mensagem: {e}")
    
    # Processa as mensagens restantes na fila antes de finalizar
    while message_queue:
      self.process_next_message()

    # Escreve o arquivo de log
    logFile = open('logfile'+str(myself)+'.log', 'w')
    logFile.writelines(str(self.logList))
    logFile.close()
    
    # Envia a lista de mensagens para o servidor (usando um socket TCP) para comparação
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

      # Verifica se todos os ACKs esperados para esta mensagem foram recebidos
      if message_id in expected_acks:
        if expected_acks[message_id].issubset(received_acks.get(message_id, set())):
          # Mensagem pronta para ser entregue
          timestamp, sender, content = heapq.heappop(message_queue)
          print(f'Mensagem {content} de processo {sender} (clock: {timestamp}) entregue à aplicação. Meu clock: {logical_clock}')
          self.logList.append((sender, content, timestamp))
          # Limpa os ACKs relacionados a esta mensagem
          if message_id in expected_acks:
            del expected_acks[message_id]
          if message_id in received_acks:
            del received_acks[message_id]
        else:
          # Nem todos os ACKs recebidos, espera
          return
      else:
        # Se não houver entrada em expected_acks, significa que é um ACK ou uma mensagem enviada por mim mesmo
        # (que não requer ACKs de outros)
        timestamp, sender, content = heapq.heappop(message_queue)
        print(f'Mensagem {content} de processo {sender} (clock: {timestamp}) entregue à aplicação (sem ACK esperado). Meu clock: {logical_clock}')
        self.logList.append((sender, content, timestamp))


# Função para aguardar o sinal de início do servidor de comparação:
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

  # Cria o manipulador de mensagens de recebimento
  msgHandler = MsgHandler(recvSocket)
  msgHandler.start()
  print('Manipulador iniciado')

  PEERS = getListOfPeers()
  my_ip_address = get_public_ip()
  
  # Envia handshakes e aguarda ACKs
  for addrToSend in PEERS:
    if addrToSend != my_ip_address: # Não envia handshake para si mesmo
      update_logical_clock()
      # Inclui o endereço IP do remetente na mensagem READY
      msg = ('READY', myself, logical_clock, my_ip_address)
      msgPack = pickle.dumps(msg)
      sendSocket.sendto(msgPack, (addrToSend,PEER_UDP_PORT))

  # Aguarda que todos os handshakes sejam recebidos (incluindo seus ACKs)
  # O MsgHandler lida com a contagem de handshakes, então a thread principal apenas espera que ela atinja N-1
  while (handShakeCount < N -1): # -1 porque não incluo meu próprio handshake na contagem
    pass

  print('Thread Principal: Handshakes enviados. handShakeCount=', str(handShakeCount))

  # Envia uma sequência de mensagens de dados para todos os outros processos 
  for msgNumber in range(0, nMsgs):
    update_logical_clock()
    msg_content = msgNumber
    message_id = (myself, msg_content)
    # expected_acks deve conter os IPs dos pares, não os IDs numéricos
    expected_acks[message_id] = set(peer_ip for peer_ip in PEERS if peer_ip != my_ip_address)
    received_acks[message_id] = set()

    msg = ('DATA', myself, logical_clock, msg_content)
    msgPack = pickle.dumps(msg)
    for addrToSend in PEERS:
      if addrToSend != my_ip_address: # Não envia para si mesmo
        sendSocket.sendto(msgPack, (addrToSend,PEER_UDP_PORT))
    
    print(f'Enviada mensagem DATA {msg_content} (clock: {logical_clock}). Aguardando ACKs.')

    # Aguarda todos os ACKs para esta mensagem específica antes de enviar a próxima
    while len(received_acks.get(message_id, set())) < len(expected_acks.get(message_id, set())):
      messages_to_process.wait(0.1) # Aguarda brevemente, permitindo que MsgHandler receba ACKs
      msgHandler.process_next_message() # Tenta processar mensagens na fila

  print('Thread Principal: Todas as mensagens de dados enviadas. Aguardando ACKs finais e processamento da fila.')

  # Garante que todos os ACKs sejam recebidos e as mensagens processadas antes de enviar sinais de parada
  # Para o propósito desta demonstração, dependemos do MsgHandler para eventualmente processar tudo.

  # Informa a todos os processos que não tenho mais mensagens para enviar
  for addrToSend in PEERS:
    if addrToSend != my_ip_address: # Não envia para si mesmo
      update_logical_clock()
      msg = (-1, myself, logical_clock, None) # Tipo -1 para sinal de parada
      msgPack = pickle.dumps(msg)
      sendSocket.sendto(msgPack, (addrToSend,PEER_UDP_PORT))
  
  print('Thread Principal: Sinais de parada enviados.')

  # Aguarda o manipulador de mensagens terminar de processar todas as mensagens e enviar logs
  msgHandler.join()
  print('Thread Principal: Manipulador de mensagens finalizado.')
