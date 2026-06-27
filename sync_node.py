import pika
import json
import random
import time
import sys
from colorama import init, Fore, Style

init(autoreset=True)

NODE_ID = sys.argv[1] if len(sys.argv) > 1 else str(random.randint(1000, 9999))
RABBITMQ_HOST = "localhost"

F = [] # Fila local do protocolo (Privada ao nó)
pending_rpc = {} # Guarda as requisições RPC dos clientes para responder depois

def conectar():
    credentials = pika.PlainCredentials('admin', 'admin123')
    params = pika.ConnectionParameters(host=RABBITMQ_HOST, credentials=credentials)
    return pika.BlockingConnection(params)

def setup_rabbit(channel):
    # O prompt exige um tópico denominado R_topic
    channel.exchange_declare(exchange='R_topic', exchange_type='fanout')
    
    # Exchange para enviar eventos para o Dashboard Visual
    channel.exchange_declare(exchange='dashboard_topic', exchange_type='fanout')
    
    # Fila para escutar os eventos de sincronização
    result = channel.queue_declare(queue='', exclusive=True)
    sync_queue = result.method.queue
    channel.queue_bind(exchange='R_topic', queue=sync_queue)
    
    # Fila para receber requisições dos clientes (RPC)
    rpc_queue_name = f'rpc_queue_{NODE_ID}'
    channel.queue_declare(queue=rpc_queue_name)
    
    return sync_queue, rpc_queue_name

def log(msg, color=Fore.WHITE):
    print(color + f"[Node {NODE_ID}] {msg}")

def process_F(channel, connection):
    """
    Avalia a fila F. Se o topo for uma requisição DESTE nó, entra na seção crítica.
    """
    global F
    if not F:
        return
    
    top = F[0]
    
    # O protocolo diz: avalia a fila F. Se o ACQUIRE no topo é meu, posso entrar.
    if top['node_id'] == NODE_ID and top['req_id'] in pending_rpc:
        client_id = top.get('pedido_cliente', {}).get('client_id', 'Desconhecido')
        log(f"Entrando na Seção Crítica (Req: {top['req_id'][:8]} do Cliente {client_id})", Fore.GREEN)
        
        # Telemetria: Avisa o front que entramos na CS
        dash_enter = {"type": "NODE_ENTER", "node_id": NODE_ID, "client_id": client_id}
        channel.basic_publish(exchange='dashboard_topic', routing_key='', body=json.dumps(dash_enter))
        
        # Sleep de 0.2 a 1 segundo (usamos connection.sleep para não bugar o pika)
        connection.sleep(random.uniform(0.2, 1.0))
        
        log(f"Saindo da Seção Crítica.", Fore.RED)
        
        # Telemetria: Avisa o front que saímos da CS
        dash_exit = {"type": "NODE_EXIT", "node_id": NODE_ID, "client_id": client_id}
        channel.basic_publish(exchange='dashboard_topic', routing_key='', body=json.dumps(dash_exit))
        
        # Remove a requisição dos pendentes (já foi processada)
        props = pending_rpc.pop(top['req_id'])
        
        # Envia o RELEASE para o broker
        msg_release = {
            'action': 'RELEASE',
            'node_id': NODE_ID,
            'pedido_cliente': top['pedido_cliente']
        }
        channel.basic_publish(
            exchange='R_topic',
            routing_key='',
            body=json.dumps(msg_release)
        )
        
        # Responde ao cliente
        channel.basic_publish(
            exchange='',
            routing_key=props.reply_to,
            properties=pika.BasicProperties(correlation_id=props.correlation_id),
            body=json.dumps({'status': 'COMMITTED'})
        )

def on_rpc_request(ch, method, props, body):
    """Quando o nó recebe um pedido do cliente"""
    pedido_cliente = json.loads(body)
    req_id = pedido_cliente.get('req_id')
    client_id = pedido_cliente.get('client_id')
    
    log(f"Recebeu pedido do Cliente {client_id} (Req: {req_id[:8]}). Publicando ACQUIRE...", Fore.CYAN)
    pending_rpc[req_id] = props
    
    msg_acquire = {
        'action': 'ACQUIRE',
        'node_id': NODE_ID,
        'pedido_cliente': pedido_cliente,
        'req_id': req_id # Atalho mantido para lógica interna de F
    }
    
    # Publica o ACQUIRE no broker para que todos os nós (inclusive este) recebam na mesma ordem
    ch.basic_publish(exchange='R_topic', routing_key='', body=json.dumps(msg_acquire))
    ch.basic_ack(delivery_tag=method.delivery_tag)

def on_sync_message(ch, method, props, body):
    """Quando o nó recebe um ACQUIRE ou RELEASE da exchange de sincronização"""
    global F
    msg = json.loads(body)
    
    if msg['action'] == 'ACQUIRE':
        # Adiciona no final da fila local F
        F.append(msg)
        # log(f"ACQUIRE enfileirado na posição {len(F)} (Req: {msg['req_id'][:8]})")
        
    elif msg['action'] == 'RELEASE':
        # Remove o pedido correspondente da fila F
        # Para encontrar pelo req_id guardado em pedido_cliente ou atalho
        req_id_to_remove = msg.get('pedido_cliente', {}).get('req_id')
        F = [req for req in F if req['req_id'] != req_id_to_remove]
        # log(f"RELEASE processado. Tamanho da fila agora é {len(F)}")
    
    ch.basic_ack(delivery_tag=method.delivery_tag)
    
    # Após processar a mensagem, avalia a fila F para ver se é a vez deste nó
    # Aqui passamos o connection porque a seção crítica dá sleep
    process_F(ch, ch.connection)


def main():
    log("Iniciando Node do Cluster Sync...", Fore.YELLOW)
    connection = conectar()
    channel = connection.channel()
    
    sync_queue, rpc_queue_name = setup_rabbit(channel)
    
    # Consumidor do Client RPC
    channel.basic_consume(queue=rpc_queue_name, on_message_callback=on_rpc_request)
    
    # Consumidor dos eventos do Cluster (ACQUIRE/RELEASE)
    channel.basic_consume(queue=sync_queue, on_message_callback=on_sync_message)
    
    log(f"Node operante. Escutando clientes na fila {rpc_queue_name} e sinc. na {sync_queue}", Fore.YELLOW)
    
    try:
        channel.start_consuming()
    except KeyboardInterrupt:
        connection.close()

if __name__ == "__main__":
    main()
