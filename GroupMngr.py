from socket import *
import pickle
from constMP import *

membership = []

def serverLoop():
    serverSock = socket(AF_INET, SOCK_STREAM)
    serverSock.bind(('0.0.0.0', GROUPMNGR_TCP_PORT))
    serverSock.listen(5)
    print(f"Group Manager rodando na porta {GROUPMNGR_TCP_PORT}")
    
    while True:
        conn, addr = serverSock.accept()
        try:
            msgPack = conn.recv(2048)
            req = pickle.loads(msgPack)
            
            if req["op"] == "register":
                membership.append((req["ipaddr"], req["port"]))
                print(f"Peer registrado: {req['ipaddr']}:{req['port']}")
                conn.send(pickle.dumps({"status": "ok"}))
            elif req["op"] == "list":
                peer_list = [m[0] for m in membership]
                print(f"Enviando lista de peers: {peer_list}")
                conn.send(pickle.dumps(peer_list))
            else:
                conn.send(pickle.dumps({"error": "Operação desconhecida"}))
        except Exception as e:
            print(f"Erro no Group Manager: {e}")
        finally:
            conn.close()

if __name__ == "__main__":
    serverLoop()