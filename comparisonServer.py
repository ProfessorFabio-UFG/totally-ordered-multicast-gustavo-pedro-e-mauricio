from socket import *
import pickle
from constMP import * # Certifique-se que N esteja correto aqui!
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
		peerList = pickle.loads(msg)
		print("List of Peers: ", peerList)
		startPeers(peerList,nMsgs)
		print('Agora, aguarde os logs de mensagens dos pares comunicantes...')
		waitForLogsAndCompare(nMsgs)
	serverSock.close()

def promptUser():
	nMsgs = int(input('Digite o número de mensagens para cada par enviar (0 para terminar)=> '))
	return nMsgs

def startPeers(peerList,nMsgs):
	# Conecta a cada um dos pares e envia o sinal de 'iniciar':
	peerNumber = 0
	for peer in peerList:
		clientSock = socket(AF_INET, SOCK_STREAM)
		clientSock.connect((peer, PEER_TCP_PORT))
		msg = (peerNumber,nMsgs)
		msgPack = pickle.dumps(msg)
		clientSock.send(msgPack)
		msgPack = clientSock.recv(512)
		print(pickle.loads(msgPack))
		clientSock.close()
		peerNumber = peerNumber + 1

def waitForLogsAndCompare(N_MSGS):
	# Loop para aguardar os logs de mensagens para comparação:
	numPeers = 0
	msgs = [] # cada msg é uma lista de tuplas (com as mensagens originais recebidas pelos processos pares)

	# Recebe os logs de mensagens dos processos pares
	while numPeers < N: # Usa N do constMP.py
		(conn, addr) = serverSock.accept()
		msgPack = conn.recv(32768)
		print ('Log recebido do par')
		conn.close()
		log_data = pickle.loads(msgPack)
		msgs.append(log_data)
		print(f"Log do par {addr}: comprimento={len(log_data)}") # Adiciona info sobre o comprimento do log
		numPeers = numPeers + 1

	unordered = 0

	# Determina o comprimento mínimo entre todos os logs recebidos para comparação
	min_log_length = N_MSGS # Inicializa com o máximo esperado
	if msgs: # Garante que msgs não está vazia
		min_log_length = min(len(log) for log in msgs)
	
	print(f"Comparando até {min_log_length} mensagens.")

	# Compara as listas de mensagens até o comprimento mínimo
	# Certifica-se de que há pelo menos um log para comparar (msgs[0])
	if msgs:
		for j in range(0, min_log_length):
			firstMsg = msgs[0][j]
			for i in range(1, len(msgs)): # Itera por todos os logs recebidos
				if firstMsg != msgs[i][j]:
					unordered = unordered + 1
					break
	else:
		print("Nenhum log de mensagem recebido para comparação.")
	
	print ('Encontradas ' + str(unordered) + ' rodadas de mensagens desordenadas')


# Inicia o servidor:
mainLoop()

