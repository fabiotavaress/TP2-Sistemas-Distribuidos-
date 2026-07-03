import pika
import time
import threading
import json
import uuid
import queue
from flask import Flask, Response, render_template

app = Flask(__name__)

# Lista para manter os clientes SSE (Server-Sent Events) conectados
listeners = []

# Fila thread-safe para aguentar milhares de cliques simultâneos
publish_queue = queue.Queue()

def pika_consumer():
    """Thread que escuta os eventos do RabbitMQ em background."""
    credentials = pika.PlainCredentials('admin', 'admin123')
    params = pika.ConnectionParameters(host='localhost', credentials=credentials)
    
    while True:
        try:
            connection = pika.BlockingConnection(params)
            channel = connection.channel()
            
            # Declara a exchange de telemetria
            channel.exchange_declare(exchange='dashboard_topic', exchange_type='fanout')
            
            # Cria uma fila exclusiva para este dashboard
            result = channel.queue_declare(queue='', exclusive=True)
            queue_name = result.method.queue
            channel.queue_bind(exchange='dashboard_topic', queue=queue_name)
            
            def callback(ch, method, properties, body):
                data = body.decode('utf-8')
                # Envia o evento para todos os navegadores abertos (thread-safe no Python por causa do GIL)
                for l in listeners:
                    l.append(data)
                    
            print("[Dashboard] Conectado ao RabbitMQ! Escutando eventos...")
            channel.basic_consume(queue=queue_name, on_message_callback=callback, auto_ack=True)
            channel.start_consuming()
        except Exception as e:
            print(f"[Dashboard] Tentando reconectar consumidor... ({e})")
            time.sleep(2)

def pika_publisher():
    """Thread dedicada APENAS para enviar as requisições. Garante que 40 pessoas clicando ao mesmo tempo não travem o servidor."""
    credentials = pika.PlainCredentials('admin', 'admin123')
    params = pika.ConnectionParameters(host='localhost', credentials=credentials)
    
    while True:
        try:
            connection = pika.BlockingConnection(params)
            channel = connection.channel()
            
            while True:
                # Fica esperando alguém clicar no botão (bloqueia até ter item na fila)
                task = publish_queue.get()
                if task['type'] == 'FAKE_TELEMETRY':
                    channel.basic_publish(exchange='dashboard_topic', routing_key='', body=json.dumps(task['payload']))
                elif task['type'] == 'REAL_REQ':
                    channel.basic_publish(exchange='', routing_key=task['queue'], body=json.dumps(task['payload']))
                
        except Exception as e:
            print(f"[Dashboard] Tentando reconectar publicador... ({e})")
            time.sleep(2)

@app.route("/")
def index():
    return render_template("index.html")

@app.route("/stream")
def stream():
    """Endpoint que o navegador consome para receber eventos em tempo real."""
    def event_stream():
        q = []
        listeners.append(q)
        try:
            while True:
                if q:
                    msg = q.pop(0)
                    yield f"data: {msg}\n\n"
                else:
                    time.sleep(0.05) # Aumentei a frequência de tick para melhor responsividade
        except GeneratorExit:
            if q in listeners:
                listeners.remove(q)
            
    return Response(event_stream(), content_type="text/event-stream")

@app.route("/force_request/<client_id>", methods=["POST"])
def force_request(client_id):
    """Endpoint chamado pelo clique no front-end para forçar um pedido de cliente."""
    try:
        # Pega o número do cliente (ex: C3 -> 3)
        node_num = client_id.replace("C", "")
        
        # Joga na fila do Publisher (Super rápido, não bloqueia o navegador do aluno)
        dash_event = {"type": "CLIENT_REQ", "client_id": client_id, "node_id": node_num}
        publish_queue.put({"type": "FAKE_TELEMETRY", "payload": dash_event})
        
        msg = {
            'client_id': f"{client_id} (Turma)", 
            'timestamp': time.time(),
            'req_id': str(uuid.uuid4())
        }
        publish_queue.put({"type": "REAL_REQ", "queue": f'rpc_queue_{node_num}', "payload": msg})
        
        return {"status": "ok"}
    except Exception as e:
        print("Erro ao forçar request:", e)
        return {"status": "error"}

if __name__ == "__main__":
    # Inicia as threads do RabbitMQ
    threading.Thread(target=pika_consumer, daemon=True).start()
    threading.Thread(target=pika_publisher, daemon=True).start()
    
    print("[Dashboard] Iniciando Servidor Web na porta 5000... (Pronto para 40 alunos!)")
    # threaded=True garante que o Flask possa atender várias conexões SSE simultâneas
    app.run(host="0.0.0.0", port=5000, debug=False, threaded=True)
