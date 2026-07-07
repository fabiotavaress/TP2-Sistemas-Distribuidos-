import subprocess
import sys
import urllib.request

DASHBOARD_URL = "http://localhost:5000"

def notify_dashboard(endpoint):
    try:
        req = urllib.request.Request(f"{DASHBOARD_URL}/{endpoint}", method="POST")
        urllib.request.urlopen(req, timeout=2)
    except Exception as e:
        print(f"[Aviso] Nao foi possivel notificar o dashboard ({endpoint}): {e}")

def main():
    print("Iniciando Cluster Sync do TP2 (Alternativa 3 - RabbitMQ)")
    print("Os pedidos serao feitos pelos usuarios pelo Dashboard\n")

    processes = []

    try:
        # Bloqueia novos cliques no dashboard — os nos vao processar a fila
        notify_dashboard("lock")

        for i in range(1, 6):
            print(f"Iniciando No {i}...")
            p = subprocess.Popen([sys.executable, 'sync_node.py', str(i)])
            processes.append(p)

        print("\n--- 5 Nos online. Processando pedidos (Fila F). Pressione Ctrl+C para encerrar ---\n")

        # Aguarda os processos rodarem até o usuário interromper
        for p in processes:
            p.wait()

    except KeyboardInterrupt:
        print("\nCtrl+C detectado. Encerrando todos os Nos...")
    finally:
        for p in processes:
            p.terminate()
        # Sempre desbloqueia os cliques ao final
        notify_dashboard("unlock")
        print("Nos encerrados. Cliques liberados no Dashboard.")

if __name__ == "__main__":
    main()
