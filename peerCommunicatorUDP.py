import socket
import threading
import time
import heapq
import pickle
from collections import defaultdict
from constMP import *

class Peer:
    def __init__(self, myself):
        self.myself = myself
        self.peers = []
        self.timestamp = 0
        self.lock = threading.Lock()
        self.condition = threading.Condition(self.lock)
        self.message_queue = []
        self.acks_received = defaultdict(set)
        
        # Sockets
        self.udp_sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        self.udp_sock.bind(('0.0.0.0', PEER_UDP_PORT))
        
        self.tcp_sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.tcp_sock.bind(('0.0.0.0', PEER_TCP_PORT))
        self.tcp_sock.listen(5)
        
        # Registrar no Group Manager
        self.register_with_group_manager()
        
        # Obter lista de peers
        self.get_peer_list()
        
        # Iniciar threads
        threading.Thread(target=self.udp_receive_loop, daemon=True).start()
        threading.Thread(target=self.tcp_accept_loop, daemon=True).start()
        threading.Thread(target=self.deliver_messages, daemon=True).start()
        
        print(f"Peer {self.myself} iniciado e pronto")

    def register_with_group_manager(self):
        try:
            sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            sock.connect((GROUPMNGR_ADDR, GROUPMNGR_TCP_PORT))
            req = {"op": "register", "ipaddr": self.myself, "port": PEER_UDP_PORT}
            sock.send(pickle.dumps(req))
            sock.close()
        except Exception as e:
            print(f"Erro ao registrar no Group Manager: {e}")

    def get_peer_list(self):
        try:
            sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            sock.connect((GROUPMNGR_ADDR, GROUPMNGR_TCP_PORT))
            req = {"op": "list"}
            sock.send(pickle.dumps(req))
            self.peers = pickle.loads(sock.recv(2048))
            sock.close()
            print(f"Lista de peers obtida: {self.peers}")
        except Exception as e:
            print(f"Erro ao obter lista de peers: {e}")

    def increment_timestamp(self):
        with self.lock:
            self.timestamp += 1
            return self.timestamp

    def udp_receive_loop(self):
        while True:
            try:
                data, addr = self.udp_sock.recvfrom(4096)
                message = pickle.loads(data)
                self.handle_incoming_message(message)
            except Exception as e:
                print(f"Erro no loop UDP: {e}")

    def handle_incoming_message(self, message):
        with self.lock:
            msg_type = message["type"]
            if msg_type == "DATA":
                sender = message["sender"]
                timestamp = message["timestamp"]
                content = message["content"]
                heapq.heappush(self.message_queue, (timestamp, sender, content))
                self.send_ack(sender, timestamp)
            elif msg_type == "ACK":
                sender = message["sender"]
                timestamp = message["timestamp"]
                source = message["source"]
                self.acks_received[(timestamp, sender)].add(source)
            self.condition.notify()

    def send_ack(self, sender, timestamp):
        ack_msg = {
            "type": "ACK",
            "sender": sender,
            "timestamp": timestamp,
            "source": self.myself,
        }
        data = pickle.dumps(ack_msg)
        for peer in self.peers:
            if peer != self.myself:
                self.udp_sock.sendto(data, (peer, PEER_UDP_PORT))

    def multicast_message(self, content):
        ts = self.increment_timestamp()
        msg = {
            "type": "DATA",
            "sender": self.myself,
            "timestamp": ts,
            "content": content,
        }
        data = pickle.dumps(msg)
        for peer in self.peers:
            if peer != self.myself:
                self.udp_sock.sendto(data, (peer, PEER_UDP_PORT))
        with self.lock:
            heapq.heappush(self.message_queue, (ts, self.myself, content))
        self.condition.notify()

    def deliver_messages(self):
        while True:
            with self.condition:
                self.condition.wait()
                while self.message_queue:
                    ts, sender, content = self.message_queue[0]
                    key = (ts, sender)
                    if key in self.acks_received:
                        acks = self.acks_received[key]
                        if all(peer in acks for peer in self.peers if peer != sender):
                            heapq.heappop(self.message_queue)
                            print(f"[ENTREGA] {content} (ts={ts}, de={sender})")

    def tcp_accept_loop(self):
        while True:
            try:
                conn, addr = self.tcp_sock.accept()
                threading.Thread(target=self.handle_tcp_connection, args=(conn,), daemon=True).start()
            except Exception as e:
                print(f"Erro ao aceitar conexão TCP: {e}")

    def handle_tcp_connection(self, conn):
        with conn:
            try:
                data = conn.recv(1024)
                if data:
                    content = pickle.loads(data)
                    self.multicast_message(content)
            except Exception as e:
                print(f"Erro na conexão TCP: {e}")

    def send_tcp_message(self, peer, content):
        try:
            sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            sock.connect((peer, PEER_TCP_PORT))
            sock.send(pickle.dumps(content))
            sock.close()
        except Exception as e:
            print(f"Erro ao enviar mensagem TCP para {peer}: {e}")

if __name__ == "__main__":
    # Obter IP público
    import requests
    my_ip = requests.get('https://api.ipify.org').text
    
    # Iniciar peer
    peer = Peer(my_ip)
    
    # Interface simples para enviar mensagens
    while True:
        msg = input("Digite uma mensagem para multicast (ou 'exit' para sair): ")
        if msg.lower() == 'exit':
            break
        peer.multicast_message(msg)