# Delivery Tracking Pipeline

Pipeline de dados em tempo real desenvolvido com **Apache Kafka, Python, Redis, Docker, Prometheus e Grafana** para simular e processar eventos de localização de uma frota de entregas.

O projeto simula um cenário no qual motoristas enviam continuamente informações de localização e status, permitindo explorar conceitos de **event streaming, arquitetura distribuída, integração de dados e observabilidade**.

---

## 🎯 Objetivo

Em sistemas de logística, eventos como localização e status de entrega são produzidos continuamente.

O objetivo deste projeto é construir uma arquitetura capaz de receber e processar esse fluxo de eventos utilizando Kafka como plataforma de streaming.

O pipeline implementado segue o fluxo:

```text
Python Simulator
       ↓
 Apache Kafka
   (3 brokers)
       ↓
 Kafka Connect
       ↓
     Redis

       +

Kafka Brokers
       ↓
JMX Exporter
       ↓
 Prometheus
       ↓
   Grafana
```

---

## 🏗️ Arquitetura

### 🐍 Python Producer

O script `generate_delivery_tracking.py` simula motoristas realizando entregas na região de São Paulo.

Cada motorista produz eventos contendo informações como:

* identificador do motorista;
* identificador da entrega;
* latitude;
* longitude;
* status;
* timestamp.

Os eventos são publicados no tópico Kafka:

```text
driver-location
```

### 📨 Apache Kafka

O ambiente utiliza um cluster composto por **3 brokers Kafka**, permitindo explorar uma configuração distribuída do serviço.

Kafka funciona como a camada central de ingestão e distribuição dos eventos produzidos pelo simulador.

### 🔌 Kafka Connect

O Kafka Connect é responsável pela integração entre o Kafka e a camada de persistência.

Um **Sink Connector** consome os eventos do tópico `driver-location` e os envia ao Redis.

Isso permite desacoplar a lógica de integração do código do produtor.

### 🗄️ Redis

Redis funciona como destino dos eventos processados pelo pipeline, permitindo armazenar informações produzidas pelos motoristas.

### 📊 Observabilidade

A infraestrutura também inclui uma camada de monitoramento:

```text
Kafka
  ↓
JMX Exporter
  ↓
Prometheus
  ↓
Grafana
```

O **Prometheus** coleta métricas relacionadas ao cluster e o **Grafana** permite sua visualização por meio de dashboards.

---

## 🔄 Fluxo dos dados

```text
1. Python gera um evento de localização

              ↓

2. Evento é publicado no Kafka

              ↓

3. Kafka armazena/distribui o evento

              ↓

4. Kafka Connect consome o tópico

              ↓

5. Sink Connector envia o evento ao Redis

              ↓

6. Redis persiste os dados

Enquanto isso:

Kafka → JMX Exporter → Prometheus → Grafana
```

---

## 📦 Exemplo de evento

As mensagens produzidas seguem uma estrutura semelhante a:

```json
{
  "timestamp": 1710345600,
  "driver_id": "driver_101",
  "delivery_id": "del_1001",
  "latitude": -23.5505,
  "longitude": -46.6333,
  "status": "DELIVERING"
}
```

---

## 🛠️ Tecnologias utilizadas

* **Apache Kafka** — streaming e distribuição dos eventos
* **Python** — geração dos eventos simulados
* **Kafka Connect** — integração entre Kafka e Redis
* **Redis** — persistência dos eventos
* **Docker / Docker Compose** — provisionamento da infraestrutura
* **Prometheus** — coleta de métricas
* **Grafana** — visualização e monitoramento
* **JMX Exporter** — exposição de métricas do Kafka

---

## 📁 Estrutura do projeto

```text
kafka/
│
├── connectors/
├── monitoring/
├── generate_delivery_tracking.py
├── redis-sink.json
├── docker-compose.yml
└── README.md
```

Os diretórios relacionados ao Kafka armazenam configurações e dados necessários para execução local do ambiente.

---

## ▶️ Como executar

### Pré-requisitos

É necessário ter instalado:

* Docker
* Docker Compose
* Python 3.x

### 1. Clone o projeto

```bash
git clone https://github.com/VieiraNando96/kafka.git
cd kafka
```

### 2. Suba a infraestrutura

```bash
docker-compose up -d
```

Esse comando inicia os serviços necessários para o pipeline.

### 3. Configure o Redis Sink Connector

Após o Kafka Connect estar disponível:

```bash
curl -X POST \
  -H "Content-Type: application/json" \
  --data @redis-sink.json \
  http://localhost:8083/connectors
```

### 4. Instale a dependência do producer

```bash
pip install confluent-kafka
```

### 5. Execute o simulador

```bash
python generate_delivery_tracking.py --drivers 15 --updates 2
```

O producer começará a gerar eventos de localização e publicá-los no tópico `driver-location`.

---

## 📊 Interfaces

Depois que a infraestrutura estiver em execução:

**Grafana**

```text
http://localhost:3000
```

**Prometheus**

```text
http://localhost:9090
```

**Kafka Connect API**

```text
http://localhost:8083/connectors
```

---

## 🧠 Conceitos explorados

Este projeto permite explorar na prática conceitos relacionados a:

* Event Streaming
* Producers e Topics
* Arquiteturas distribuídas
* Kafka Connect
* Sink Connectors
* Integração entre sistemas
* Containerização
* Observabilidade
* Monitoramento de infraestrutura

---

## 🚀 Possíveis evoluções

Algumas evoluções possíveis incluem:

* adicionar consumers específicos para processamento dos eventos;
* implementar Schema Registry;
* adicionar particionamento baseado em `driver_id`;
* implementar tratamento de falhas e Dead Letter Queue;
* adicionar testes automatizados;
* criar dashboards específicos para métricas do pipeline;
* persistir histórico de eventos em uma camada analítica.

---

### Sobre o projeto

Este projeto foi desenvolvido como estudo prático de **arquiteturas orientadas a eventos e Engenharia de Dados**, implementando um pipeline local completo desde a geração dos eventos até persistência e observabilidade.
