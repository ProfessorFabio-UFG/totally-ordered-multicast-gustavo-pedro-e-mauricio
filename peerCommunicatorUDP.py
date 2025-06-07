import socket
import threading
import time
import heapq
import pickle
from collections import defaultdict

PEER_TCP_PORT = 6000
PEER_UDP_PORT = 5000

class Peer:
    def __init__(self, myself, peers):
        self.myself = myself
        self.peers = sorted(peers)
        self.timestamp = 0
        self.lock = threading.Lock()
        self.condition = threading.Condition(self.lock)

        self.message_queue = []
        self.acks_received = defaultdict(set)

        self.start_udp_listener()
        self.start_tcp_listener()
        self.start_peers_tcp_connection()

        threading.Thread(target=self.deliver_messages, daemon=True).start()

    def log(self, msg):
        print(f"[{self.myself}] {msg}")

    def increment_timestamp(self):
        with self.lock:
            self.timestamp += 1
            return self.timestamp

    def start_udp_listener(self):
        self.udp_sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        self.udp_sock.bind(("", PEER_UDP_PORT))
        threading.Thread(target=self.udp_receive_loop, daemon=True).start()
        self.log("UDP listener iniciado")

    def udp_receive_loop(self):
        while True:
            data, _ = self.udp_sock.recvfrom(4096)
            message = pickle.loads(data)
            self.handle_incoming_message(message)

    def handle_incoming_message(self, message):
        with self.lock:
            msg_type = message["type"]
            if msg_type == "msg":
                sender = message["sender"]
                timestamp = message["timestamp"]
                content = message["content"]
                heapq.heappush(self.message_queue, (timestamp, sender, content))
                self.send_ack(sender, timestamp)
            elif msg_type == "ack":
                sender = message["sender"]
                timestamp = message["timestamp"]
                source = message["source"]
                self.acks_received[(timestamp, sender)].add(source)
            self.condition.notify()

    def send_ack(self, sender, timestamp):
        ack_msg = {
            "type": "ack",
            "sender": sender,
            "timestamp": timestamp,
            "source": self.myself,
        }
        data = pickle.dumps(ack_msg)
        for peer in self.peers:
            self.udp_sock.sendto(data, (peer, PEER_UDP_PORT))

    def multicast_message(self, content):
        ts = self.increment_timestamp()
        msg = {
            "type": "msg",
            "sender": self.myself,
            "timestamp": ts,
            "content": content,
        }
        data = pickle.dumps(msg)
        for peer in self.peers:
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
                    acks = self.acks_received[(ts, sender)]
                    if all(peer in acks for peer in self.peers):
                        heapq.heappop(self.message_queue)
                        self.log(f"ENTREGA: {content} (ts={ts}, de={sender})")
                    else:
                        break

    def start_tcp_listener(self):
        def listen():
            sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            sock.bind(("", PEER_TCP_PORT))
            sock.listen()
            self.log(f"TCP escutando na porta {PEER_TCP_PORT}")
            while True:
                conn, _ = sock.accept()
                threading.Thread(target=self.handle_tcp_connection, args=(conn,), daemon=True).start()

        threading.Thread(target=listen, daemon=True).start()

    def handle_tcp_connection(self, conn):
        with conn:
            try:
                data = conn.recv(1024).decode()
                if data:
                    self.multicast_message(data)
            except Exception as e:
                self.log(f"Erro na conexão TCP: {e}")

    def start_peers_tcp_connection(self):
        def try_connect(peer, port, retries=10, delay=0.5):
            for _ in range(retries):
                try:
                    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                    sock.settimeout(1.5)
                    sock.connect((peer, port))
                    sock.close()
                    return True
                except Exception:
                    time.sleep(delay)
            return False

        for peer in self.peers:
            if peer != self.myself:
                if try_connect(peer, PEER_TCP_PORT):
                    self.log(f"Conexão TCP com {peer} estabelecida")
                else:
                    self.log(f"Falha ao conectar com {peer} após várias tentativas")