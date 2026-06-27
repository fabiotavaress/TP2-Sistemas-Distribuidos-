import subprocess
import time
import sys

def main():
    print("Iniciando Simulação do TP2 (Alternativa 3 - RabbitMQ)")
    
    processes = []
    
    # Sobe 5 Nós do Cluster Sync
    for i in range(1, 6):
        print(f"Iniciando Nó {i}...")
        p = subprocess.Popen([sys.executable, 'sync_node.py', str(i)])
        processes.append(p)
        
    time.sleep(2) # Dá um tempinho para os nós se conectarem ao RabbitMQ
    
    # Sobe 5 Clientes (cada um se conectando a um nó respectivo)
    for i in range(1, 6):
        print(f"Iniciando Cliente {i}...")
        p = subprocess.Popen([sys.executable, 'client.py', f'C{i}', str(i)])
        processes.append(p)
        
    print("\n--- Todos os processos rodando. Pressione Ctrl+C para parar ---\n")
    
    try:
        # Aguarda apenas os processos dos Clientes terminarem (eles encerram sozinhos após N requisições)
        # Como lançamos 5 nós primeiro, os clientes estão do índice 5 em diante na lista
        for p in processes[5:]:
            p.wait()
            
        print("\nTodos os clientes finalizaram suas requisições com sucesso!")
        print("Encerrando os Nós do Cluster (que são servidores 24/7) automaticamente...")
        for p in processes:
            p.terminate()
            
    except KeyboardInterrupt:
        print("\nEncerrando todos os processos...")
        for p in processes:
            p.terminate()

if __name__ == "__main__":
    main()
