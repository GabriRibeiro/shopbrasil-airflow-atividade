# ShopBrasil - Pipeline de Preços com Airflow

Este projeto substitui um script Python agendado via cron, que era usado para gerar um painel de preços por categoria de produtos. O script antigo apresentava problemas: falhava silenciosamente quando a API instabilizava, duplicava dados quando era reexecutado manualmente, e exigia alteração de código a cada nova categoria de produto.

O pipeline aqui construído resolve esses problemas usando Apache Airflow.

## O que o pipeline faz

Todos os dias, às 6h da manhã (horário de Brasília), o pipeline executa automaticamente os seguintes passos:

1. Busca a lista de produtos na FakeStore API
2. Identifica as categorias de produtos existentes
3. Calcula, para cada categoria, o preço médio, mínimo, máximo e a quantidade de produtos
4. Salva os resultados em uma tabela no PostgreSQL

Em caso de falha na comunicação com a API, o pipeline tenta novamente automaticamente, aumentando o tempo de espera entre as tentativas. Caso o pipeline seja executado novamente (reprocessamento), os dados não são duplicados — apenas atualizados.

## Estrutura do projeto

```
shopbrasil-airflow-atividade/
├── dags/
│   └── shopbrasil_pricing_pipeline.py   -> código do pipeline
├── logs/                                  -> logs do Airflow (gerado automaticamente)
├── plugins/                               -> pasta padrão do Airflow
├── config/                                -> pasta padrão do Airflow
├── docker-compose.yaml                    -> sobe o ambiente completo (Airflow + bancos)
└── .env                                   -> variável de configuração do Docker
```

## Pré-requisitos

- Docker Desktop instalado e em execução (com WSL2 habilitado, no caso de Windows)
- Git instalado

## Como executar o projeto

### 1. Clonar o repositório
```
git clone <url-do-repositorio>
cd shopbrasil-airflow-atividade
```

### 2. Criar o arquivo de configuração
Criar um arquivo `.env` na raiz do projeto com o seguinte conteúdo:
```
AIRFLOW_UID=50000
```

### 3. Inicializar o Airflow
Comando executado apenas na primeira vez:
```
docker compose up airflow-init
```

### 4. Subir o ambiente
```
docker compose up -d
```

Para verificar se todos os serviços subiram corretamente:
```
docker compose ps
```
Todos os serviços devem aparecer como `Up` ou `healthy`.

### 5. Acessar a interface do Airflow
Endereço: `http://localhost:8080`

- usuário: `admin`
- senha: `admin`

### 6. Configurar a conexão com o banco de dados

Em `Admin > Connections`, criar uma nova conexão com os seguintes dados:

| Campo | Valor |
|---|---|
| Connection Id | `postgres_analytics` |
| Connection Type | `Postgres` |
| Host | `postgres-analytics` |
| Database | `analytics` |
| Login | `analytics` |
| Password | `analytics` |
| Port | `5432` |

### 7. Configurar o pool de concorrência

Em `Admin > Pools`, criar um novo pool:

| Campo | Valor |
|---|---|
| Pool | `ecommerce_pool` |
| Slots | `2` |

### 8. Ativar e executar o pipeline

Na lista de DAGs, localizar `shopbrasil_pricing_pipeline`, ativá-lo e disparar uma execução manual pelo botão de play.

### 9. Verificar o resultado

O progresso pode ser acompanhado nas abas "Graph" ou "Grid" da interface do Airflow.

Para consultar os dados gravados diretamente no banco:
```
docker exec -it shopbrasil-airflow-atividade-postgres-analytics-1 psql -U analytics -d analytics -c "SELECT * FROM category_price_metrics;"
```

## Validação de idempotência

Para confirmar que o pipeline não duplica dados ao ser reprocessado, a mesma execução foi disparada duas vezes. Em ambas as vezes a tabela manteve o mesmo número de linhas (uma por categoria), com apenas o campo de data de atualização sendo alterado — evidenciando que ocorreu uma atualização (`UPDATE`) e não uma nova inserção.

## Decisões técnicas

**Separação entre banco de metadados do Airflow e banco analítico:** o Airflow utiliza um banco próprio para controlar suas execuções internas. O banco onde ficam os dados de preços foi mantido separado, refletindo uma arquitetura mais próxima de um ambiente real, em que o orquestrador e o banco de dados analítico não compartilham a mesma instância.

**Categorias extraídas dinamicamente:** as categorias de produtos não estão fixas no código. Elas são extraídas a partir dos produtos retornados pela API a cada execução. Isso permite que o pipeline processe automaticamente novas categorias, sem necessidade de alteração de código.

**Pool de concorrência com 2 slots:** como o cálculo de métricas roda em paralelo (uma tarefa por categoria), o pool limita a quantidade de tarefas executando simultaneamente, evitando sobrecarga na API ou no banco de dados.

## Observação sobre credenciais

As credenciais presentes no `docker-compose.yaml` (usuários e senhas de banco de dados e do Airflow) são valores padrão de ambiente local de desenvolvimento/estudo. Não representam credenciais de produção e não estão expostas externamente, pois os serviços rodam isoladamente em containers Docker.
