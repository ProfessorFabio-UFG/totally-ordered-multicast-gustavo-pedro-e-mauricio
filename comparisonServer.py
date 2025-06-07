from socket import *
import pickle
from constMP import *
import time
import sys

serverSock = socket(AF_INET, SOCK_STREAM)
serverSock.bind(('0.0.0.0', SERVER_PORT))
serverSock.listen(6)

def mainLoop():
	cont = 1
	while 1:
		nMsgs = promptUser()
		if nMsgs == 0:
			break
		clientSock = socket(AF_INET, SOCK_STREAM)
		clientSock.connect((GROUPMNGR_ADDR,GROUPMNGR_TCP_PORT))
		req = {"op":"list"}
		msg = pickle.dumps(req)
		clientSock.send(msg)
		msg = clientSock.recv(2048)
		clientSock.close()
		
		# Garante que a lista de pares seja única antes de tentar conectar
		peerList_raw = pickle.loads(msg)
		peerList = list(set(peerList_raw)) # Remove duplicatas
		
		print("Lista de Peers (únicos): ", peerList)
		
		# Verifica se o número de peers na lista corresponde a N
		if len(peerList) != N:
			print(f"Aviso: O número de peers únicos ({len(peerList)}) não corresponde ao N configurado ({N}). Isso pode causar problemas de sincronização.")
			# Decida aqui se deseja continuar ou abortar. Por enquanto, continuamos.

		startPeers(peerList,nMsgs)
		print('Agora, aguarde os logs de mensagens dos pares comunicadores...')
		waitForLogsAndCompare(nMsgs)
	serverSock.close()

def promptUser():
	nMsgs = int(input('Digite o número de mensagens para cada par enviar (0 para terminar)=> '))
	return nMsgs

def startPeers(peerList,nMsgs):
	# Conecta a cada um dos pares e envia o sinal 'initiate':
	peerNumber = 0
	for peer in peerList:
		try:
			clientSock = socket(AF_INET, SOCK_STREAM)
			print(f"Tentando conectar ao peer {peer} na porta {PEER_TCP_PORT} para iniciar...")
			clientSock.connect((peer, PEER_TCP_PORT))
			msg = (peerNumber,nMsgs)
			msgPack = pickle.dumps(msg)
			clientSock.send(msgPack)
			msgPack = clientSock.recv(512)
			print(pickle.loads(msgPack))
			clientSock.close()
			peerNumber = peerNumber + 1
		except ConnectionRefusedError:
			print(f"Erro: Conexão recusada pelo peer {peer}. Certifique-se de que o processo peerCommunicatorUDP.py esteja rodando e o firewall esteja configurado corretamente.")
		except Exception as e:
			print(f"Erro inesperado ao conectar ao peer {peer}: {e}")


def waitForLogsAndCompare(N_MSGS):
	# Loop para aguardar os logs de mensagens para comparação:
	numPeers = 0
	msgs = [] # cada msg é uma lista de tuplas (com as mensagens originais recebidas pelos processos pares)

	# Recebe os logs de mensagens dos processos pares
	while numPeers < N:
		try:
			(conn, addr) = serverSock.accept()
			msgPack = conn.recv(32768)
			print (f'Log recebido do peer {addr}')
			conn.close()
			msgs.append(pickle.loads(msgPack))
			numPeers = numPeers + 1
		except Exception as e:
			print(f"Erro ao receber log do peer: {e}")
			# Implementar uma lógica para lidar com peers que não enviam logs,
			# por exemplo, um timeout ou pular o peer se ele falhou anteriormente.
			# Por simplicidade, este loop continuará esperando até N logs.


	unordered = 0

	# Compara as listas de mensagens
	# Esta lógica de comparação pode precisar de ajuste dependendo do formato exato dos logs e do objetivo de ordenação.
	if len(msgs) == N and N_MSGS > 0:
		for j in range(N_MSGS): # Itera sobre o número de mensagens esperadas
			# Pega a mensagem do primeiro peer como referência
			if j < len(msgs[0]): # Verifica se a mensagem existe no log do primeiro peer
				firstMsg = msgs[0][j]
				for i in range(1,N): # Itera sobre os outros peers (começa do segundo peer)
					if j < len(msgs[i]): # Verifica se a mensagem existe no log do peer atual
						if firstMsg != msgs[i][j]:
							unordered = unordered + 1
							print(f"Desordem encontrada na mensagem {j}: Peer 0 recebeu {firstMsg}, Peer {i} recebeu {msgs[i][j]}")
							break # Quebra o loop interno se encontrar uma desordem nesta rodada
					else:
						print(f"Aviso: Peer {i} não recebeu a mensagem {j}. Log incompleto.")
						unordered = unordered + 1 # Considera como desordenado ou falho
						break
			else:
				print(f"Aviso: Mensagem {j} não encontrada no log do primeiro peer. Log incompleto.")
				unordered = unordered + 1 # Considera como desordenado ou falho
				break
	elif N_MSGS > 0:
		print(f"Aviso: Número de logs recebidos ({len(msgs)}) não corresponde ao N configurado ({N}). Não é possível comparar completamente.")
		unordered = -1 # Indica uma falha na obtenção de todos os logs

	print ('Encontradas ' + str(unordered) + ' rodadas de mensagens desordenadas')


# Inicia o servidor:
mainLoop()
