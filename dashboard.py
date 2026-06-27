import pika
import time
import threading
from flask import Flask, Response, render_template

app = Flask(__name__)

# Lista para manter os clientes SSE (Server-Sent Events) conectados
listeners = []

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
                # Envia o evento para todos os navegadores abertos
                for l in listeners:
                    l.append(data)
                    
            print("[Dashboard] Conectado ao RabbitMQ! Escutando eventos...")
            channel.basic_consume(queue=queue_name, on_message_callback=callback, auto_ack=True)
            channel.start_consuming()
        except Exception as e:
            print(f"[Dashboard] Tentando reconectar ao RabbitMQ... ({e})")
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
                    time.sleep(0.1)
        except GeneratorExit:
            listeners.remove(q)
            
    return Response(event_stream(), content_type="text/event-stream")

if __name__ == "__main__":
    # Inicia o consumidor do RabbitMQ em uma thread separada
    threading.Thread(target=pika_consumer, daemon=True).start()
    print("[Dashboard] Iniciando Servidor Web na porta 5000...")
    app.run(host="0.0.0.0", port=5000, debug=False)
