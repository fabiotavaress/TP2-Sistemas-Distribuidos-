import pika
import uuid
import json
import time
import random
import sys
from colorama import init, Fore

init(autoreset=True)

CLIENT_ID = sys.argv[1] if len(sys.argv) > 1 else str(uuid.uuid4())[:6]
NODE_ID = sys.argv[2] if len(sys.argv) > 2 else "1"

RABBITMQ_HOST = "localhost"

def log(msg, color=Fore.WHITE):
    print(color + f"[Client {CLIENT_ID}] {msg}")

class SyncClient:
    def __init__(self):
        credentials = pika.PlainCredentials('admin', 'admin123')
        self.connection = pika.BlockingConnection(
            pika.ConnectionParameters(host=RABBITMQ_HOST, credentials=credentials)
        )
        self.channel = self.connection.channel()
        
        # Declara fila exclusiva para receber a resposta do Cluster Sync
        result = self.channel.queue_declare(queue='', exclusive=True)
        self.reply_queue = result.method.queue
        
        # Declara a exchange do dashboard
        self.channel.exchange_declare(exchange='dashboard_topic', exchange_type='fanout')
        
        self.channel.basic_consume(
            queue=self.reply_queue,
            on_message_callback=self.on_response,
            auto_ack=True
        )
        self.response = None
        self.corr_id = None

    def on_response(self, ch, method, props, body):
        if self.corr_id == props.correlation_id:
            self.response = json.loads(body)

    def acquire_resource(self):
        self.response = None
        self.corr_id = str(uuid.uuid4())
        req_id = str(uuid.uuid4())
        timestamp = time.time()
        
        target_queue = f'rpc_queue_{NODE_ID}'
        msg = {
            'client_id': CLIENT_ID, 
            'timestamp': timestamp,
            'req_id': req_id
        }
        
        # Publica evento de telemetria pro Dashboard
        dash_event = {"type": "CLIENT_REQ", "client_id": CLIENT_ID, "node_id": NODE_ID}
        self.channel.basic_publish(exchange='dashboard_topic', routing_key='', body=json.dumps(dash_event))
        
        self.channel.basic_publish(
            exchange='',
            routing_key=target_queue,
            properties=pika.BasicProperties(
                reply_to=self.reply_queue,
                correlation_id=self.corr_id,
            ),
            body=json.dumps(msg)
        )
        
        # Espera ativamente pela resposta processando eventos do Pika
        while self.response is None:
            self.connection.process_data_events(time_limit=None)
            
        return self.response

def main():
    log(f"Iniciando. Conectado ao Nó {NODE_ID} do Cluster Sync.", Fore.MAGENTA)
    client = SyncClient()
    
    num_requests = random.randint(10, 50)
    log(f"Irá realizar {num_requests} requisições de escrita.", Fore.CYAN)
    
    for i in range(num_requests):
        log(f"Pedindo acesso ao recurso R ({i+1}/{num_requests})...", Fore.YELLOW)
        
        # O cliente faz o pedido e trava aqui até receber COMMITTED
        res = client.acquire_resource()
        
        if res.get('status') == 'COMMITTED':
            log(f"Recebeu COMMITTED. Acesso concluído com sucesso.", Fore.GREEN)
        
        # Sleep aleatório de 1 a 5 segundos conforme requisito
        wait_time = random.uniform(1, 5)
        log(f"Dormindo por {wait_time:.1f} segundos...\n")
        time.sleep(wait_time)
        
    log("Todas as requisições finalizadas. Cliente encerrando.", Fore.MAGENTA)
    client.connection.close()

if __name__ == "__main__":
    main()
