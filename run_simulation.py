import subprocess
import sys
import urllib.request
import pika
import json

DASHBOARD_URL = "http://localhost:5000"
RABBITMQ_HOST = "localhost"

def notify_dashboard(endpoint):
    try:
        req = urllib.request.Request(f"{DASHBOARD_URL}/{endpoint}", method="POST")
        urllib.request.urlopen(req, timeout=2)
    except Exception as e:
        print(f"[Aviso] Nao foi possivel notificar o dashboard ({endpoint}): {e}")

def get_initial_message_count(channel):
    total = 0
    for i in range(1, 6):
        try:
            res = channel.queue_declare(queue=f'rpc_queue_{i}')
            total += res.method.message_count
        except Exception:
            pass
    return total

def main():
    print("Iniciando Cluster Sync do TP2 (Alternativa 3 - RabbitMQ)")
    print("Os pedidos serao feitos pelos usuarios pelo Dashboard\n")

    # 0. Garante estado limpo: mata qualquer processo zumbi de simulações anteriores
    # que possa ter ficado rodando em background e roubando mensagens das filas!
    subprocess.run(["pkill", "-f", "sync_node.py"], stderr=subprocess.DEVNULL)

    # 1. Conecta ao RabbitMQ para monitoramento
    credentials = pika.PlainCredentials('admin', 'admin123')
    params = pika.ConnectionParameters(host=RABBITMQ_HOST, credentials=credentials)
    try:
        connection = pika.BlockingConnection(params)
        channel = connection.channel()
    except Exception as e:
        print(f"Erro ao conectar no RabbitMQ: {e}")
        return

    # 2. Conta exatamente quantos pedidos existem nas filas ANTES dos nós ligarem
    total_requests = get_initial_message_count(channel)
    print(f"[{total_requests} pedidos pendentes encontrados nas filas]")

    if total_requests == 0:
        print("Nenhum pedido para processar. Encerrando.")
        connection.close()
        return

    # 3. Cria uma fila exclusiva para ouvir os eventos do dashboard (para saber quando acabaram)
    channel.exchange_declare(exchange='dashboard_topic', exchange_type='fanout')
    res = channel.queue_declare(queue='', exclusive=True)
    tracker_queue = res.method.queue
    channel.queue_bind(exchange='dashboard_topic', queue=tracker_queue)

    processes = []
    completed = 0

    try:
        notify_dashboard("lock")

        for i in range(1, 6):
            print(f"Iniciando No {i}...")
            p = subprocess.Popen([sys.executable, 'sync_node.py', str(i)])
            processes.append(p)

        print(f"\n--- Processando {total_requests} pedidos automaticamente. Aguarde... ---\n")

        # 4. Escuta os eventos e conta quantos saíram da seção crítica
        for method_frame, properties, body in channel.consume(tracker_queue):
            msg = json.loads(body)
            if msg.get('type') == 'NODE_EXIT':
                completed += 1
                print(f"[Progresso] Pedido processado. ({completed}/{total_requests})")
                if completed >= total_requests:
                    break

        print("\nTodos os pedidos foram processados com sucesso!")

    except KeyboardInterrupt:
        print("\nCtrl+C detectado. Encerrando todos os Nos...")
    finally:
        channel.cancel()
        connection.close()
        for p in processes:
            p.terminate()
        notify_dashboard("unlock")
        print("Nos encerrados. Cliques liberados no Dashboard.")

if __name__ == "__main__":
    main()
