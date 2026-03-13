### Delivery Tracking Pipeline com Kafka, Redis e Grafana
Este projeto demonstra um pipeline de dados em tempo real para monitoramento de frotas de entrega. Ele utiliza um cluster Kafka para ingestão de dados, um simulador Python para gerar coordenadas de motoristas, Kafka Connect para persistir os dados no Redis e uma stack de monitoramento (Prometheus/Grafana) para observar a saúde do cluster.

#### Arquitetura do Projeto
Simulador Python: Gera dados fictícios de geolocalização (latitude/longitude) e status de motoristas na região de São Paulo.

Cluster Kafka: Composto por 3 brokers configurados para alta disponibilidade e resiliência.

Kafka Connect: Utiliza um conector de Sink para enviar as mensagens do tópico driver-location diretamente para o Redis.

Redis: Armazena o estado mais recente ou o histórico de posições dos motoristas.

Monitoramento: Prometheus coleta métricas dos brokers via JMX Exporter, e o Grafana as visualiza em dashboards.

#### Pré-requisitos
Docker e Docker Compose instalados.
Python 3.x (para rodar o simulador localmente).
Biblioteca confluent-kafka para Python.

#### Como Executar

1. Subir a Infraestrutura
Inicie todos os serviços (Kafka, Redis, Connect, Prometheus, Grafana):

  ```
  docker-compose up -d
  ```

2. Configurar o Conector Redis
Após o Kafka Connect estar online (porta 8083), registre o conector de Sink:

  ```
  curl -X POST -H "Content-Type: application/json" --data @redis-sink.json http://localhost:8083/connectors
  ```

3. Iniciar o Simulador de Entregas
Instale as dependências e execute o script para começar a produzir dados para o tópico driver-location:

  ```
  pip install confluent-kafka
  python generate_delivery_tracking.py --drivers 15 --updates 2
  ```

#### Acessando as Interfaces
Grafana: http://localhost:3000 (Credenciais padrão: admin/admin)

Prometheus: http://localhost:9090

Kafka Connect API: http://localhost:8083/connectors

#### Estrutura de Dados (JSON)
As mensagens enviadas ao Kafka seguem este formato:

  ```
   {
    "timestamp": 1710345600,
    "driver_id": "driver_101",
    "delivery_id": "del_1001",
    "latitude": -23.5505,
    "longitude": -46.6333,
    "status": "DELIVERING"
  }
  ```


