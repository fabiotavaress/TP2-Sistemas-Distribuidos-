import subprocess
import time
import sys

def main():
    print("Iniciando Cluster Sync do TP2 (Alternativa 3 - RabbitMQ)")
    print("Os pedidos serao feitos pelos usuarios pelo Dashboard: http://localhost:5000\n")
    
    processes = []
    
    # Sobe apenas os 5 Nos do Cluster Sync (os "servidores" que processam os pedidos)
    # Os clientes sao os usuarios que clicam no Dashboard
    for i in range(1, 6):
        print(f"Iniciando No {i}...")
        p = subprocess.Popen([sys.executable, 'sync_node.py', str(i)])
        processes.append(p)
        
    print("\n--- 5 Nos do Cluster online. Aguardando pedidos pelo Dashboard. Ctrl+C para parar ---\n")
    
    try:
        for p in processes:
            p.wait()
    except KeyboardInterrupt:
        print("\nEncerrando todos os Nos...")
        for p in processes:
            p.terminate()

if __name__ == "__main__":
    main()
