# Trabalho Prático 2 - Sincronização Distribuída (Alternativa 3)

Este repositório contém a solução perfeita para o **TP02 de Sistemas Distribuídos**, implementando o **Protocolo de Alternativa 3**. O projeto foi estruturado para atender **rigorosamente todos os requisitos preliminares e específicos da proposta**.

---

## 🎯 Como os Requisitos da Proposta Foram Atendidos

### 1. "Solução distribuída, sem controle central de um nó do cluster"
✅ **Atendido:** Não existe um "Nó Mestre". A exclusão mútua é decidida independentemente por cada nó do cluster avaliando sua própria fila privada local (F). A única centralização é o Broker Pub/Sub (RabbitMQ), que garante apenas a **ordenação total (FIFO)** das mensagens.

### 2. "Mínimo de 5 Clientes... Cada cliente conhece apenas um elemento do cluster"
✅ **Atendido:** O script `run_simulation.py` sobe **5 clientes** simultaneamente (`client.py`). Ao iniciar, cada cliente "pluga-se" exclusivamente a uma fila RPC específica de um dos 5 nós do cluster (ex: o cliente 1 só conversa com a fila `rpc_queue_1` do Nó 1). 

### 3. "Cada cliente possui um ID único e envia o timestamp para garantir pedidos únicos"
✅ **Atendido:** No `client.py`, cada pedido gera um payload contendo seu `CLIENT_ID`, um identificador de requisição e o `timestamp` exato da geração do pedido. O cliente faz entre **10 e 50 acessos** em loop.

### 4. "O Cluster Sync deve ser composto por, no mínimo, 5 processos..."
✅ **Atendido:** O script orquestrador sobre **5 processos independentes** do `sync_node.py`, representando o Cluster Sync.

### 5. "Conhece apenas um broker pub/sub online 24x7... envia mensagem de Pub a um tópico denominado R topic"
✅ **Atendido:** Toda a comunicação entre os nós é feita enviando Pubs para um *exchange* tipo fanout do RabbitMQ nomeado especificamente de **`R_topic`**. A mensagem contém a primitiva (`ACQUIRE` ou `RELEASE`), o `ID_DO_NO`, e o `PEDIDO_DO_CLIENTE`.

### 6. "NA MESMA ORDEM FIFO... Cabe a cada elemento avaliar sua fila F (PRIVADA)"
✅ **Atendido:** O RabbitMQ garante que o broadcast (`R_topic`) entregará os eventos na **exata mesma ordem** para todos.
   - Todo `ACQUIRE` recebido no `R_topic` é inserido na fila local `F` do Nó.
   - O Nó verifica: *O `ACQUIRE` que está no topo de `F` foi emitido por mim?* Se sim, ele entra na Seção Crítica.
   - Ao sair da seção crítica, ele publica o `RELEASE`. Qualquer nó que receber o `RELEASE` tira o respectivo pedido da fila `F`, liberando a vez para o próximo.

### 7. "Após entrar na seção crítica (Sleep 0.2 a 1 segundo)... o cliente recebe COMMITTED... e entra em espera (Sleep 1 a 5s)"
✅ **Atendido:** Assim que ganha a vez na sua fila `F`, o nó processa o recurso dando um sleep aleatório entre **0.2s e 1.0s**. Logo após enviar o `RELEASE` pro `R_topic`, ele manda uma mensagem RPC `COMMITTED` diretamente para o cliente, que então descansa de **1s a 5s** antes do próximo pedido.

---

## 🚀 Como Executar e Apresentar

Para que a apresentação fique cristalina, criamos uma **Interface Visual em Tempo Real** (Dashboard) e um script orquestrador que centraliza todos os terminais.

**Passo 1: Suba o Broker RabbitMQ**
```bash
docker compose up -d
```
*(O painel web fica acessível em `http://localhost:15672` com `admin / admin123`)*

**Passo 2: Instale as bibliotecas Python (se precisar)**
```bash
pip install -r requirements.txt
```

**Passo 3: Inicie o Dashboard Visual**
Abra um terminal exclusivo e rode:
```bash
python dashboard.py
```
Acesse no seu navegador: **http://localhost:5000**

**Passo 4: Rode a Simulação do Protocolo**
Abra um **segundo** terminal e rode a simulação:
```bash
python run_simulation.py
```

### 🧠 Como Interpretar o Dashboard Visual
O dashboard traduz a matemática da Exclusão Mútua para o mundo visual, facilitando a demonstração para o professor:
- **Topologia:** Você verá o **Recurso R** no topo, os 5 **Nós do Cluster** no meio, e os 5 **Clientes** na base.
- **Requisição (ACQUIRE):** Quando um cliente pede acesso, ele pisca e dispara uma partícula (bolinha) amarela em direção ao seu respectivo Nó do cluster.
- **Seção Crítica Garantida:** O RabbitMQ organiza as mensagens perfeitamente e os Nós respeitam a Fila `F`. Quando finalmente é a vez do Nó, ele fica **Verde Brilhante** e uma linha verde sólida o conecta com exclusividade ao Recurso R no topo. 
- **Prova Prática:** Você notará perfeitamente, olhando para o desenho animado, que **dois Nós nunca ficam verdes ao mesmo tempo**, garantindo visualmente a Exclusão Mútua e a Alternativa 3!
- **Liberação (RELEASE):** Ao terminar o uso (sleep), o Nó volta a ficar azul e dispara uma partícula roxa (`COMMITTED`) de volta para o cliente, que então "dorme" satisfeito.

### 🎮 Demonstração Interativa na Apresentação
O Dashboard foi projetado para **alta concorrência** e suporta que múltiplos usuários (ex: uma sala inteira de 40 alunos) acessem o link pelo celular e **cliquem fisicamente nos Clientes (bolinhas roxas)**.
Essa interação direta demonstra os conceitos mais importantes do trabalho prático:
1. **Garantia da Exclusão Mútua:** Mesmo que 40 pessoas cliquem no exato mesmo segundo junto com os clientes automáticos, o recurso R **NUNCA** ficará verde para dois nós simultaneamente. A regra nunca é quebrada.
2. **Multicast Ordenado em Ação:** Prova o funcionamento da Alternativa 3. Como os cliques geram pedidos que entram no mesmo "tubo" (`R_topic`) no RabbitMQ, todos os Nós do cluster enxergam esses pedidos na exata mesma ordem. Todos concluem quem é o próximo, sem precisarem de um Nó mestre.
3. **Assincronia e Resiliência:** Demonstra que os Nós aguentam picos massivos de concorrência. Se milhares de cliques chegarem ao mesmo tempo, eles enfileiram com segurança no RabbitMQ e o sistema distribui o recurso com cadência, sem perder requisições e sem travar.
