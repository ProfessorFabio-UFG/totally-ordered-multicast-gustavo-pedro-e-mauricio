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
expected_acks = {} # Dicionário para rastrear os ACKs para mensagens enviadas por este peer OU recebidas de outros
received_acks = {} # Dicionário para armazenar os ACKs recebidos para qualquer mensagem rastreada em expected_acks
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
  # Garante que a lista de pares seja única antes de tentar conectar
  unique_peers = list(set(pickle.loads(msg)))
  print ('Lista de pares única obtida: ', unique_peers)
  clientSock.close()
  return unique_peers

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

    # Fase 1: Handshake
    while handShakeCount < N - 1: # Aguarda handshakes de N-1 outros pares
      try:
        msgPack = self.sock.recv(1024)
        received_msg = pickle.loads(msgPack)
        
        if len(received_msg) == 4 and received_msg[0] == 'READY':
            msg_type, sender_numerical_id, msg_timestamp, sender_ip_address = received_msg
            update_logical_clock(msg_timestamp)
            handShakeCount += 1
            print(f'--- Handshake recebido de ID: {sender_numerical_id}, IP: {sender_ip_address}, Clock: {logical_clock}')
            ack_msg = ('ACK_READY', myself, logical_clock, sender_numerical_id) 
            sendSocket.sendto(pickle.dumps(ack_msg), (sender_ip_address, PEER_UDP_PORT))
        elif len(received_msg) == 4 and received_msg[0] == 'ACK_READY':
            msg_type, ack_sender_id, ack_timestamp, original_sender_id = received_msg
            update_logical_clock(ack_timestamp)
            print(f"--- ACK_READY recebido de {ack_sender_id}. Meu clock: {logical_clock}")
        else:
            print(f"Aviso: Mensagem de handshake inesperada durante fase de handshake: {received_msg}")
      except Exception as e:
        print(f"Erro durante o handshake: {e}")

    print('Thread Secundária: Recebeu todos os handshakes. Entrando no loop para receber mensagens.')

    # Fase 2: Troca de Mensagens de Dados e Tratamento de ACK
    stopCount = 0 
    while not self.stop_event.is_set() or stopCount < N:                
      try:
        msgPack = self.sock.recv(1024)
        received_msg = pickle.loads(msgPack)

        msg_type = received_msg[0]
        
        if msg_type == -1 and len(received_msg) == 5:
          sender_id, msg_timestamp, content, sender_data_ip_address = received_msg[1:]
          update_logical_clock(msg_timestamp)
          stopCount += 1
          print(f"Mensagem de parada de {sender_id}. Contagem de paradas: {stopCount}/{N}. Meu clock: {logical_clock}")
          if stopCount == N:
            self.stop_event.set()
            break
        elif msg_type == 'DATA' and len(received_msg) == 5:
          sender_id, msg_timestamp, content, sender_data_ip_address = received_msg[1:]
          update_logical_clock(msg_timestamp)
          
          message_id = (sender_id, content) # O ID da mensagem, independentemente de quem a enviou
          heapq.heappush(message_queue, (msg_timestamp, sender_id, content))
          print(f"Mensagem DATA {content} de {sender_id} (clock: {msg_timestamp}) enfileirada. Meu clock: {logical_clock}")
          
          # Envia ACK para o remetente original da mensagem DATA
          ack_msg = ('ACK', myself, logical_clock, message_id) # ACK para a mensagem específica
          sendSocket.sendto(pickle.dumps(ack_msg), (sender_data_ip_address, PEER_UDP_PORT))

          # Inicializa expected_acks e received_acks para esta mensagem se for a primeira vez que a vemos
          if message_id not in expected_acks:
              # Espera ACKs de todos os outros peers (N-1) para que esta mensagem possa ser entregue localmente em ordem total.
              # O remetente original da mensagem é responsável por coletar os ACKs.
              # Aqui, cada peer rastreia a confirmação dos outros.
              expected_acks[message_id] = set(i for i in range(N) if i != sender_id)
              received_acks[message_id] = set()
          
          # Adiciona o próprio ID do peer ao conjunto de ACKs recebidos, pois ele acabou de processar a mensagem
          # Isso é um reconhecimento local.
          received_acks[message_id].add(myself)
          messages_to_process.set() # Sinaliza que pode haver mensagens para processar

        elif msg_type == 'ACK' and len(received_msg) == 4:
          ack_sender_id, ack_timestamp, original_msg_info = received_msg[1:]
          update_logical_clock(ack_timestamp)
          original_sender, original_content = original_msg_info
          message_id = (original_sender, original_content)

          # Garante que os dicionários existam para a mensagem que está sendo reconhecida
          if message_id not in expected_acks:
              # Isso pode acontecer se recebemos um ACK para uma mensagem que ainda não processamos como DATA.
              # Nesse caso, assumimos que é uma mensagem que eventualmente será enfileirada e precisa de ACKs de todos.
              expected_acks[message_id] = set(i for i in range(N) if i != original_sender)
          
          if message_id not in received_acks:
              received_acks[message_id] = set()
          received_acks[message_id].add(ack_sender_id)
          print(f"ACK para DATA {original_content} de {original_sender} recebido de {ack_sender_id}. Meu clock: {logical_clock}")
          messages_to_process.set() # Sinaliza que pode haver mensagens para processar
        else:
            print(f"Aviso: Mensagem recebida com formato inesperado: {received_msg}. Ignorando.")

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
      # Use uma cópia do cabeçalho para verificar as condições, remove da fila apenas se estiver pronto
      next_msg_timestamp, next_msg_sender, next_msg_content = heapq.nsmallest(1, message_queue)[0]
      message_id = (next_msg_sender, next_msg_content)

      # A mensagem só é entregue se estiver no topo da fila (menor timestamp)
      # E se todos os ACKs esperados para ela foram recebidos.
      if message_id in expected_acks:
        expected_ack_senders = expected_acks[message_id]
        received_ack_senders = received_acks.get(message_id, set())

        # Verifica se o conjunto de ACKs recebidos contém todos os ACKs esperados
        # (N-1 peers que devem ter enviado ACK para esta mensagem)
        if expected_ack_senders.issubset(received_ack_senders) and len(expected_ack_senders) == len(received_ack_senders):
          # Mensagem pronta para ser entregue
          timestamp, sender, content = heapq.heappop(message_queue)
          print(f'Mensagem {content} de processo {sender} (clock: {timestamp}) entregue à aplicação. Meu clock: {logical_clock}')
          self.logList.append((sender, content, timestamp))
          # Limpa os ACKs relacionados a esta mensagem para evitar crescimento de memória
          if message_id in expected_acks:
            del expected_acks[message_id]
          if message_id in received_acks:
            del received_acks[message_id]
        else:
          # Nem todos os ACKs recebidos, espera e não processa outras mensagens da fila por enquanto
          return
      else:
        # Este caso NÃO DEVERIA MAIS ACONTECER para mensagens DATA ou STOP,
        # pois expected_acks é agora inicializado para todas as mensagens enfileiradas.
        # Se acontecer, pode indicar uma falha na lógica de inicialização de expected_acks.
        # Para fins de depuração, ainda deixamos um log.
        # Se uma mensagem não está em expected_acks, ela não está aguardando ACKs.
        # Isso pode ser para mensagens internas ou outros tipos que não exigem total order.
        timestamp, sender, content = heapq.heappop(message_queue)
        print(f'Aviso: Mensagem {content} de processo {sender} (clock: {timestamp}) entregue sem rastreamento de ACK (não em expected_acks). Meu clock: {logical_clock}')
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
    message_id = (myself, msg_content) # ID da mensagem enviada por mim
    
    # Inicializa expected_acks e received_acks para a mensagem que estou enviando
    # Espero ACKs de todos os outros peers (N-1)
    expected_acks[message_id] = set(i for i in range(N) if i != myself)
    received_acks[message_id] = set()
    # Adiciono meu próprio ID como "ACK" local de que processei esta mensagem enviada por mim
    received_acks[message_id].add(myself)

    # A mensagem DATA agora inclui o IP do remetente para que o receptor saiba para onde enviar o ACK
    msg = ('DATA', myself, logical_clock, msg_content, my_ip_address)
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
      # A mensagem de parada agora inclui o IP do remetente para consistência, embora não seja estritamente necessário para ela
      msg = (-1, myself, logical_clock, None, my_ip_address) # Tipo -1 para sinal de parada
      msgPack = pickle.dumps(msg)
      sendSocket.sendto(msgPack, (addrToSend,PEER_UDP_PORT))
  
  print('Thread Principal: Sinais de parada enviados.')

  # Aguarda o manipulador de mensagens terminar de processar todas as mensagens e enviar logs
  msgHandler.join()
  print('Thread Principal: Manipulador de mensagens finalizado.')
