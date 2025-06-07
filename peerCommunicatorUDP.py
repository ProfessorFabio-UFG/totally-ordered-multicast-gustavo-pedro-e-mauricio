from socket import *
from constMP import *
import threading
import pickle
from requests import get

# Variáveis globais
handShakeCount = 0
PEERS = []
LIST_MESSAGES = []

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

class MsgHandler(threading.Thread):
    def __init__(self, sock):
        threading.Thread.__init__(self)
        self.sock = sock
    
    def run(self):
        global handShakeCount, LIST_MESSAGES, clock
        
        print('[HANDLER] Iniciado. Aguardando handshakes...')
        
        while handShakeCount < N:
            try:
                msgPack = self.sock.recv(1024)
                if not msgPack:
                    continue
                    
                msg = pickle.loads(msgPack)
                
                # Handshake message format: (clock, 'READY', sender_id)
                if len(msg) == 3 and msg[1] == 'READY':
                    sender_clock, msg_type, sender_id = msg
                    clock.update(sender_clock)
                    handShakeCount += 1
                    print(f'[HANDSHAKE] Recebido de {sender_id} (clock: {sender_clock}) | Total: {handShakeCount}/{N}')
                else:
                    print(f'[WARNING] Formato inválido de handshake: {msg}')
                    
            except Exception as e:
                print(f'[ERROR] Erro ao processar handshake: {str(e)}')
                continue

        print(f'[HANDLER] Todos handshakes confirmados | Clock: {clock.time}')

        stopCount = 0
        while True:
            try:
                msgPack = self.sock.recv(1024)
                if not msgPack:
                    continue
                    
                msg = pickle.loads(msgPack)
                
                # Data message format: (clock, 'DATA', sender_id, content)
                if len(msg) == 4 and msg[1] == 'DATA':
                    sender_clock, msg_type, sender_id, content = msg
                    clock.update(sender_clock)
                    LIST_MESSAGES.append((sender_id, content, sender_clock))
                    print(f'[MSG] Recebida de {sender_id}: "{content}" (clock: {sender_clock})')
                
                # Stop message format: (clock, 'STOP', sender_id, None)
                elif len(msg) == 4 and msg[1] == 'STOP':
                    sender_clock, msg_type, sender_id, _ = msg
                    clock.update(sender_clock)
                    stopCount += 1
                    print(f'[STOP] Recebido de {sender_id} | Total: {stopCount}/{N}')
                    if stopCount == N: 
                        break
                else:
                    print(f'[WARNING] Mensagem com formato inválido: {msg}')
                    
            except Exception as e:
                print(f'[ERROR] Erro ao processar mensagem: {str(e)}')
                continue

        LIST_MESSAGES.sort(key=lambda x: x[2])  # Ordena pelo clock
        print('[ORDER] Mensagens ordenadas:')
        for idx, (pid, content, t) in enumerate(LIST_MESSAGES):
            print(f'  {idx+1}. Processo {pid} (clock {t}): {content}')

        try:
            clientSock = socket(AF_INET, SOCK_STREAM)
            clientSock.connect((SERVER_ADDR, SERVER_PORT))
            clientSock.send(pickle.dumps(LIST_MESSAGES))
            clientSock.close()
            print('[SERVER] Dados enviados para análise')
        except Exception as e:
            print(f'[ERROR] Falha ao enviar para servidor: {str(e)}')

def main_loop():
    global handShakeCount, LIST_MESSAGES
    
    registerWithGroupManager()
    
    while True:
        print('\n[MAIN] Aguardando sinal de início...')
        try:
            conn, _ = serverSock.accept()
            data = conn.recv(1024)
            if not data:
                continue
                
            myself, nMsgs = pickle.loads(data)
            conn.send(pickle.dumps(f'Peer {myself} iniciado'))
            conn.close()
            
            print(f'[MAIN] Iniciando como processo {myself} com {nMsgs} mensagens')
            if nMsgs == 0: 
                break

            LIST_MESSAGES = []
            handShakeCount = 0
            handler = MsgHandler(recvSocket)
            handler.start()

            PEERS = getListOfPeers()
            for peer in PEERS:
                try:
                    clock.increment()
                    msg = (clock.time, 'READY', myself)
                    sendSocket.sendto(pickle.dumps(msg), (peer, PEER_UDP_PORT))
                    print(f'[HANDSHAKE] Enviado para {peer} (clock: {clock.time})')
                except Exception as e:
                    print(f'[ERROR] Falha ao enviar handshake para {peer}: {str(e)}')

            for i in range(nMsgs):
                try:
                    clock.increment()
                    msg = (clock.time, 'DATA', myself, f'Msg_{i}_from_{myself}')
                    for peer in PEERS:
                        sendSocket.sendto(pickle.dumps(msg), (peer, PEER_UDP_PORT))
                    print(f'[SEND] Mensagem {i} enviada (clock: {clock.time})')
                except Exception as e:
                    print(f'[ERROR] Falha ao enviar mensagem {i}: {str(e)}')

            clock.increment()
            stop_msg = (clock.time, 'STOP', myself, None)
            for peer in PEERS:
                try:
                    sendSocket.sendto(pickle.dumps(stop_msg), (peer, PEER_UDP_PORT))
                except Exception as e:
                    print(f'[ERROR] Falha ao enviar STOP para {peer}: {str(e)}')
            print(f'[STOP] Sinal enviado (clock: {clock.time})')

            handler.join()
            print('[MAIN] Ciclo completo. Reiniciando...')
            
        except Exception as e:
            print(f'[MAIN ERROR] Erro no loop principal: {str(e)}')
            continue

if __name__ == "__main__":
    try:
        main_loop()
    except KeyboardInterrupt:
        print('\n[SHUTDOWN] Encerrando peer...')
    finally:
        sendSocket.close()
        recvSocket.close()
        serverSock.close()