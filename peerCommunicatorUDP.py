from socket import *
from constMP import *
import threading
import random
import pickle
from requests import get
import heapq

handShakeCount = 0
ready_acks_for_my_handshake_count = 0 # Novo contador para ACKs de handshakes que eu enviei
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
    global ready_acks_for_my_handshake_count # Acessa a nova variável global

    # Fase 1: Handshake
    # Este loop processa tanto 'READY' quanto 'ACK_READY' durante a fase de handshake
    # A transição para a fase 2 (data exchange) é controlada pela thread principal.
    while True: # Loop infinito durante a fase de handshake, a saída é controlada externamente
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
            msg_type, ack_sender_id, ack_timestamp, original_sender_id_of_ready = received_msg
            update_logical_clock(ack_timestamp)
            print(f"--- ACK_READY recebido de {ack_sender_id} para READY de {original_sender_id_of_ready}. Meu clock: {logical_clock}")
            if original_sender_id_of_ready == myself:
                # Este ACK_READY é para um handshake *que eu enviei*
                ready_acks_for_my_handshake_count += 1
                print(f"ACK para meu handshake recebido de {ack_sender_id}. Total de ACKs de handshake recebidos: {ready_acks_for_my_handshake_count}/{N-1}")
        else:
            print(f"Aviso: Mensagem de handshake inesperada durante fase de handshake: {received_msg}")
        
        # Se as condições do handshake forem atendidas, o manipulador pode sair do loop de handshake.
        # No entanto, como a thread principal também espera por isso, manteremos o loop infinito
        # e a thread principal fará a transição.
        if handShakeCount == N - 1 and ready_acks_for_my_handshake_count == N - 1:
            break # Todos os handshakes e seus ACKs foram processados ou aguardados

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
              # Espera ACKs de TODOS os N peers, incluindo o próprio remetente original da DATA
              expected_acks[message_id] = set(range(N))
              received_acks[message_id] = set()
          
          # Adiciona o próprio ID do peer ao conjunto de ACKs recebidos, pois ele acabou de processar a mensagem
          received_acks[message_id].add(myself)
          messages_to_process.set() # Sinaliza que pode haver mensagens para processar

        elif msg_type == 'ACK' and len(received_msg) == 4:
          ack_sender_id, ack_timestamp, original_msg_info = received_msg[1:]
          update_logical_clock(ack_timestamp)
          original_sender, original_content = original_msg_info
          message_id = (original_sender, original_content)

          # Garante que os dicionários existam para a mensagem que está sendo reconhecida
          if message_id not in expected_acks:
              # Se um ACK chega antes da DATA correspondente (ex: reordenação de rede),
              # inicializamos a expectativa de ACK para que a DATA possa ser processada corretamente depois.
              expected_acks[message_id] = set(range(N))
          
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
    ready_acks_for_my_handshake_count = 0 # Resetar também
    print("Manipulador encerrado.")
    exit(0)

  def process_next_message(self):
    global PEERS
    global myself

    # Só processa se a fila não estiver vazia
    if not message_queue:
        return

    # Verifica a mensagem no topo da fila sem removê-la ainda
    next_msg_timestamp, next_msg_sender, next_msg_content = heapq.nsmallest(1, message_queue)[0]
    message_id = (next_msg_sender, next_msg_content)

    # Uma mensagem só pode ser entregue se ela tem entradas em expected_acks
    # Isso implica que é uma mensagem DATA (ou similar que requer ordem total)
    if message_id in expected_acks:
        expected_ack_senders = expected_acks[message_id]
        received_ack_senders = received_acks.get(message_id, set())

        # Verifica se todos os N ACKs esperados foram recebidos (todos os peers do sistema)
        if len(received_ack_senders) == N: # Agora esperamos N ACKs (1 de si mesmo + N-1 dos outros)
            # Mensagem pronta para ser entregue
            timestamp, sender, content = heapq.heappop(message_queue) # Remove da fila
            print(f'Mensagem {content} de processo {sender} (clock: {timestamp}) entregue à aplicação. Meu clock: {logical_clock}')
            self.logList.append((sender, content, timestamp))
            # Limpa os ACKs relacionados a esta mensagem para evitar crescimento de memória
            if message_id in expected_acks: 
              del expected_acks[message_id]
            if message_id in received_acks:
              del received_acks[message_id]
            # Chama recursivamente para verificar se a próxima mensagem na fila pode ser processada
            self.process_next_message()
        else:
            # Nem todos os ACKs recebidos para a mensagem no topo, então nenhuma mensagem pode ser entregue agora.
            return
    else:
        # Este bloco indica uma mensagem no topo da fila que não está sendo rastreada para ACKs.
        # Para um protocolo de ordenação total, isso não deve acontecer para mensagens DATA.
        print(f'ERRO CRÍTICO: Mensagem {next_msg_content} de processo {next_msg_sender} (clock: {next_msg_timestamp}) no topo da fila sem rastreamento de ACK. Isso indica um problema de inicialização. Não entregando. Meu clock: {logical_clock}')
        return # Não entrega esta mensagem se ela não está sendo rastreada para ACKs


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

  # Aguarda que todos os handshakes sejam recebidos (READYs de outros) E que meus handshakes sejam ACKados
  while (handShakeCount < N - 1) or (ready_acks_for_my_handshake_count < N - 1):
    pass

  print('Thread Principal: Handshakes enviados e ACKs recebidos. handShakeCount=', str(handShakeCount), 'ready_acks_for_my_handshake_count=', ready_acks_for_my_handshake_count)

  # Envia uma sequência de mensagens de dados para todos os outros processos 
  for msgNumber in range(0, nMsgs):
    update_logical_clock()
    msg_content = msgNumber
    message_id = (myself, msg_content) # ID da mensagem enviada por mim
    
    # Inicializa expected_acks e received_acks para a mensagem que estou enviando
    # Espero ACKs de TODOS os N peers (1 de si mesmo + N-1 dos outros)
    expected_acks[message_id] = set(range(N))
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
    # A condição mudou para esperar N ACKs no total
    while len(received_acks.get(message_id, set())) < N:
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
