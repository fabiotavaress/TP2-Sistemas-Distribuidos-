import subprocess
import time
import sys
import urllib.request
import json
import base64

DASHBOARD_URL = "http://localhost:5000"
RABBITMQ_API  = "http://localhost:15672/api"
RABBITMQ_CRED = base64.b64encode(b"admin:admin123").decode()

def notify_dashboard(endpoint):
    try:
        req = urllib.request.Request(f"{DASHBOARD_URL}/{endpoint}", method="POST")
        urllib.request.urlopen(req, timeout=2)
    except Exception as e:
        print(f"[Aviso] Nao foi possivel notificar o dashboard ({endpoint}): {e}")

def get_queue_messages(queue_name):
    """Retorna quantas mensagens estao na fila. -1 se erro."""
    try:
        url = f"{RABBITMQ_API}/queues/%2F/{queue_name}"
        req = urllib.request.Request(url)
        req.add_header("Authorization", f"Basic {RABBITMQ_CRED}")
        resp = urllib.request.urlopen(req, timeout=3)
        data = json.loads(resp.read())
        return data.get("messages", 0)
    except:
        return -1

def all_queues_empty():
    """Retorna True se todas as 5 rpc_queues estiverem vazias."""
    for i in range(1, 6):
        msgs = get_queue_messages(f"rpc_queue_{i}")
        if msgs != 0:
            return False
    return True

def main():
    print("Iniciando Cluster Sync do TP2 (Alternativa 3 - RabbitMQ)")
    print("Os pedidos serao feitos pelos usuarios pelo Dashboard\n")

    processes = []

    try:
        notify_dashboard("lock")
        
        print("\n[Aguardando 2 segundos antes de iniciar o processamento...]\n")
        time.sleep(2)

        for i in range(1, 6):
            print(f"Iniciando No {i}...")
            p = subprocess.Popen([sys.executable, 'sync_node.py', str(i)])
            processes.append(p)

        print("\n--- 5 Nos online. Monitorando filas do RabbitMQ... ---\n")

        # Aguarda as filas ficarem vazias (todos os pedidos consumidos)
        consecutive_empty = 0
        while True:
            time.sleep(3)
            if all_queues_empty():
                consecutive_empty += 1
                print(f"[Monitor] Filas vazias ({consecutive_empty}/3)...")
                if consecutive_empty >= 3:  # 9 segundos consecutivos vazias = encerrado
                    print("\nTodos os pedidos foram consumidos! Encerrando nos...")
                    break
            else:
                consecutive_empty = 0  # Resetar se ainda tiver mensagem

    except KeyboardInterrupt:
        print("\nCtrl+C detectado. Encerrando...")
    finally:
        for p in processes:
            p.terminate()
        notify_dashboard("unlock")
        print("Nos encerrados. Cliques liberados no Dashboard.")

if __name__ == "__main__":
    main()
