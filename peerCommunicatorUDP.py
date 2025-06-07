from socket import *
from constMP import *
import threading
import pickle
from collections import deque

logical_clock = 0
message_queue = deque()  # filas ordenadas por timestamp lógico
pending_acks = {}
delivered_msgs = set()  # controla msgs já entregues para não duplicar
PEERS = []
handShakeCount = 0
myself = None

sendSocket = socket(AF_INET, SOCK_DGRAM)
recvSocket = socket(AF_INET, SOCK_DGRAM)
recvSocket.bind(('0.0.0.0', PEER_UDP_PORT))

serverSock = socket(AF_INET, SOCK_STREAM)
serverSock.bind(('0.0.0.0', PEER_TCP_PORT))
serverSock.listen(1)


def update_logical_clock(received_clock):
    global logical_clock
    logical_clock = max(logical_clock, received_clock) + 1


def send_ack(dest_addr, msg_id):
    global logical_clock
    logical_clock += 1
    ack_msg = ('ACK', msg_id, logical_clock, myself)
    sendSocket.sendto(pickle.dumps(ack_msg), (dest_addr, PEER_UDP_PORT))


def deliver_ready_messages():
    """
    Entrega mensagens da fila na ordem crescente do clock lógico, 
    e somente as que têm ACK recebido.
    """
    global message_queue, pending_acks, delivered_msgs

    # Ordena a fila por timestamp lógico da mensagem
    message_queue = deque(sorted(message_queue, key=lambda x: x[2]))

    while message_queue:
        msg = message_queue[0]
        msg_type, msg_id, msg_clock, sender_id = msg[:4]

        # Se já entregamos essa mensagem, descartamos
        if (sender_id, msg_id) in delivered_msgs:
            message_queue.popleft()
            continue

        # Para mensagens DATA, só entrega se ACK recebido
        if msg_type == 'DATA':
            sender_addr = msg[4]
            if (sender_addr, msg_id) in pending_acks and pending_acks[(sender_addr, msg_id)]:
                # Entrega a mensagem
                print(f'DELIVER: Message {msg_id} from process {sender_id} with clock {msg_clock}')
                delivered_msgs.add((sender_id, msg_id))
                message_queue.popleft()
                del pending_acks[(sender_addr, msg_id)]
            else:
                # Ainda não recebeu ACK, para entrega
                break
        elif msg_type == 'ACK':
            # ACK não precisa entrega especial, só remove da fila
            print(f'ACK received for message {msg_id} from process {sender_id}')
            message_queue.popleft()
        else:
            # Mensagem desconhecida, remove
            message_queue.popleft()


def process_messages():
    """
    Thread que fica processando a fila sem usar sleep.
    Usa um evento para esperar a chegada de novas mensagens.
    """
    global message_queue
    while True:
        if message_queue:
            deliver_ready_messages()


def waitToStart():
    (conn, addr) = serverSock.accept()
    msg = pickle.loads(conn.recv(1024))
    global myself
    myself = msg[0]
    nMsgs = msg[1]
    conn.send(pickle.dumps(f'Peer process {myself} started.'))
    conn.close()
    return (myself, nMsgs)


class MsgHandler(threading.Thread):
    def __init__(self, sock):
        threading.Thread.__init__(self)
        self.sock = sock

    def run(self):
        global handShakeCount, logical_clock

        while handShakeCount < N:
            msgPack = self.sock.recv(1024)
            msg = pickle.loads(msgPack)
            if msg[0] == 'READY':
                handShakeCount += 1
                update_logical_clock(msg[2])
                reply = ('READY_ACK', myself, logical_clock)
                sendSocket.sendto(pickle.dumps(reply), (msg[3], PEER_UDP_PORT))
                print(f'--- Handshake received from {msg[1]}')

        print('Secondary Thread: Received all handshakes. Entering message loop.')

        stopCount = 0
        while True:
            msgPack = self.sock.recv(1024)
            msg = pickle.loads(msgPack)

            if msg[0] == 'DATA':
                update_logical_clock(msg[2])
                message_queue.append(msg)
                pending_acks[(msg[4], msg[1])] = False
                send_ack(msg[4], msg[1])

            elif msg[0] == 'ACK':
                update_logical_clock(msg[2])
                message_queue.append(msg)
                pending_acks[(msg[3], msg[1])] = True

            elif msg[0] == -1:
                stopCount += 1
                if stopCount == N:
                    break


# Main
if __name__ == "__main__":
    registerWithGroupManager()
    (myself, nMsgs) = waitToStart()
    print(f'I am up, and my ID is: {myself}')

    if nMsgs == 0:
        exit(0)

    PEERS = getListOfPeers()

    queue_processor = threading.Thread(target=process_messages)
    queue_processor.daemon = True
    queue_processor.start()

    msgHandler = MsgHandler(recvSocket)
    msgHandler.start()

    # Envia handshakes
    my_ip = get_public_ip()
    for peer in PEERS:
        if peer != my_ip:
            msg = ('READY', myself, logical_clock, peer)
            sendSocket.sendto(pickle.dumps(msg), (peer, PEER_UDP_PORT))

    # Aguarda todas respostas de handshake
    while handShakeCount < N - 1:
        pass  # aqui pode usar threading.Event para melhorar

    # Envia mensagens DATA para peers
    for msgNumber in range(nMsgs):
        logical_clock += 1
        msg = ('DATA', msgNumber, logical_clock, myself, my_ip)
        for peer in PEERS:
            if peer != my_ip:
                sendSocket.sendto(pickle.dumps(msg), (peer, PEER_UDP_PORT))
                pending_acks[(peer, msgNumber)] = False
                print(f'Sent message {msgNumber} to {peer}')

    # Envia mensagens de término
    for peer in PEERS:
        if peer != my_ip:
            msg = (-1, -1)
            sendSocket.sendto(pickle.dumps(msg), (peer, PEER_UDP_PORT))

    msgHandler.join()
