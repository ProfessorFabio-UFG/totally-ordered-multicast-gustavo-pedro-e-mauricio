from socket import *
from constMP import *
import threading
import pickle
from requests import get
from collections import defaultdict

# Variáveis globais
handShakeCount = 0
PEERS = []
LIST_MESSAGES = []
lamport_clock = 0  # Relógio lógico de Lamport

# Configuração de sockets (mantida igual)
sendSocket = socket(AF_INET, SOCK_DGRAM)
recvSocket = socket(AF_INET, SOCK_DGRAM)
recvSocket.bind(('0.0.0.0', PEER_UDP_PORT))
serverSock = socket(AF_INET, SOCK_STREAM)
serverSock.bind(('0.0.0.0', PEER_TCP_PORT))
serverSock.listen(1)

# --- Classe do Relógio de Lamport ---
class LamportClock:
    def __init__(self):
        self.time = 0
    
    def increment(self):
        self.time += 1
        print(f'[CLOCK] Incrementado → {self.time}')
        return self.time
    
    def update(self, received_time):
        self.time = max(self.time, received_time) + 1
        print(f'[CLOCK] Atualizado {received_time} → {self.time}')
        return self.time

clock = LamportClock()

# --- Funções Auxiliares (com logs) ---
def get_public_ip():
    ipAddr = get('https://api.ipify.org').content.decode('utf8')
    print(f'[NET] IP público: {ipAddr}')
    return ipAddr

def registerWithGroupManager():
    print('[REGISTER] Conectando ao group manager...')
    clientSock = socket(AF_INET, SOCK_STREAM)
    clientSock.connect((GROUPMNGR_ADDR, GROUPMNGR_TCP_PORT))
    msg = pickle.dumps({"op":"register", "ipaddr":get_public_ip(), "port":PEER_UDP_PORT})
    clientSock.send(msg)
    clientSock.close()
    print('[REGISTER] Registro completo')

def getListOfPeers():
    print('[PEERS] Solicitando lista de peers...')
    clientSock = socket(AF_INET, SOCK_STREAM)
    clientSock.connect((GROUPMNGR_ADDR, GROUPMNGR_TCP_PORT))
    clientSock.send(pickle.dumps({"op":"list"}))
    PEERS = pickle.loads(clientSock.recv(2048))
    clientSock.close()
    print(f'[PEERS] Lista obtida: {PEERS}')
    return PEERS

# --- Thread Handler com Lamport ---
class MsgHandler(threading.Thread):
    def __init__(self, sock):
        threading.Thread.__init__(self)
        self.sock = sock
    
    def run(self):
        global handShakeCount, LIST_MESSAGES, clock
        
        print('[HANDLER] Iniciado. Aguardando handshakes...')
        
        # Fase 1: Handshakes
        while handShakeCount < N:
            msgPack = self.sock.recv(1024)
            sender_clock, msg_type, sender_id = pickle.loads(msgPack)
            
            if msg_type == 'READY':
                clock.update(sender_clock)
                handShakeCount += 1
                print(f'[HANDSHAKE] Recebido de {sender_id} (clock: {sender_clock}) | Total: {handShakeCount}/{N} | Meu clock: {clock.time}')

        print(f'[HANDLER] Todos handshakes confirmados | Clock: {clock.time}')

        # Fase 2: Recebimento de mensagens
        stopCount = 0
        while True:
            msgPack = self.sock.recv(1024)
            sender_clock, msg_type, sender_id, msg_content = pickle.loads(msgPack)
            
            clock.update(sender_clock)
            
            if msg_type == 'STOP':
                stopCount += 1
                print(f'[STOP] Recebido de {sender_id} | Total: {stopCount}/{N} | Clock: {clock.time}')
                if stopCount == N: break
            else:
                LIST_MESSAGES.append((sender_id, msg_content, sender_clock))
                print(f'[MSG] Recebida de {sender_id}: "{msg_content}" (clock: {sender_clock}) | Meu clock: {clock.time}')

        # Ordenação final pelo relógio lógico
        LIST_MESSAGES.sort(key=lambda x: x[2])
        print('[ORDER] Mensagens ordenadas:')
        for idx, (pid, content, t) in enumerate(LIST_MESSAGES[:10]):
            print(f'  {idx+1}. Processo {pid} (clock {t}): {content}')

        # Envio para servidor (mantido igual)
        clientSock = socket(AF_INET, SOCK_STREAM)
        clientSock.connect((SERVER_ADDR, SERVER_PORT))
        clientSock.send(pickle.dumps(LIST_MESSAGES))
        clientSock.close()
        print('[SERVER] Dados enviados para análise')

# --- Função Principal Modificada ---
def main_loop():
    global handShakeCount, LIST_MESSAGES, clock
    
    registerWithGroupManager()
    
    while True:
        print('\n[MAIN] Aguardando sinal de início...')
        conn, _ = serverSock.accept()
        myself, nMsgs = pickle.loads(conn.recv(1024))
        conn.send(pickle.dumps(f'Peer {myself} iniciado'))
        conn.close()
        
        print(f'[MAIN] Iniciando como processo {myself} com {nMsgs} mensagens')
        if nMsgs == 0: break

        # Prepara handler
        LIST_MESSAGES = []
        handShakeCount = 0
        handler = MsgHandler(recvSocket)
        handler.start()

        # Obtem peers e envia handshakes
        PEERS = getListOfPeers()
        for peer in PEERS:
            clock.increment()
            msg = (clock.time, 'READY', myself)
            sendSocket.sendto(pickle.dumps(msg), (peer, PEER_UDP_PORT))
            print(f'[HANDSHAKE] Enviado para {peer} (clock: {clock.time})')

        # Envia mensagens regulares
        for i in range(nMsgs):
            clock.increment()
            msg = (clock.time, 'DATA', myself, f'Msg_{i}_from_{myself}')
            for peer in PEERS:
                sendSocket.sendto(pickle.dumps(msg), (peer, PEER_UDP_PORT))
            print(f'[SEND] Mensagem {i} enviada (clock: {clock.time})')

        # Envia STOP
        clock.increment()
        stop_msg = (clock.time, 'STOP', myself, None)
        for peer in PEERS:
            sendSocket.sendto(pickle.dumps(stop_msg), (peer, PEER_UDP_PORT))
        print(f'[STOP] Sinal enviado (clock: {clock.time})')

        handler.join()
        print('[MAIN] Ciclo completo. Reiniciando...')

if __name__ == "__main__":
    main_loop()