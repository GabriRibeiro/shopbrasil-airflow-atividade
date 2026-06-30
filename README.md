# ShopBrasil - Pipeline de Preços com Airflow

Esse projeto foi feito pra uma atividade da faculdade. A ideia é resolver um problema bem comum: a empresa fictícia ShopBrasil tinha um script Python rodando via cron que buscava dados de produtos numa API e gerava um painel de preços por categoria. O problema é que esse script era frágil — quando a API falhava de madrugada, ninguém ficava sabendo, e quando alguém rodava de novo na mão, os dados duplicavam no banco.

Minha missão aqui foi trocar esse script por um pipeline de verdade, construído no Apache Airflow, que resolve todos esses problemas.

## O que esse pipeline faz

Todo dia, às 6h da manhã (horário de Brasília), ele faz isso sozinho:

1. Busca a lista de produtos numa API pública de testes (a FakeStore API)
2. Descobre quais categorias de produtos existem (eletrônicos, joias, roupas, etc.)
3. Calcula, pra cada categoria, o preço médio, o preço mínimo, o preço máximo e quantos produtos tem
4. Salva tudo isso numa tabela no banco de dados PostgreSQL

E o mais importante: se algo der errado no meio do caminho (a API cair, por exemplo), ele tenta de novo sozinho, indo aumentando o tempo de espera entre tentativas. E se alguém precisar rodar tudo de novo manualmente, ele não duplica os dados — só atualiza os números.

## Como o projeto está organizado

```
shopbrasil-airflow-atividade/
├── dags/
│   └── shopbrasil_pricing_pipeline.py   -> o código do pipeline
├── logs/                                  -> logs gerados pelo Airflow (criado automaticamente)
├── plugins/                               -> pasta padrão do Airflow (não usei nada aqui)
├── config/                                -> pasta padrão do Airflow (não usei nada aqui)
├── docker-compose.yaml                    -> sobe todo o ambiente (Airflow + bancos de dados)
└── .env                                   -> uma configuração simples que o Docker precisa
```

## O que eu usei pra rodar tudo isso

Como eu uso Windows e não tinha nada configurado, precisei instalar algumas ferramentas antes:

- **Docker Desktop** (com o WSL2 ativado) - é o que permite rodar o Airflow e o banco de dados em "caixinhas" isoladas (containers), sem precisar instalar nada direto no Windows
- **Git** - pra versionar o código e subir pro GitHub
- **VS Code** - pra editar os arquivos

Importante: pra usar o Docker Desktop, foi preciso habilitar a virtualização na BIOS do computador (minha placa-mãe é uma ASUS B450M). Se alguém for rodar esse projeto e o Docker reclamar de "virtualization support not detected", é isso que precisa ser ativado lá na BIOS antes de mais nada.

## Como rodar esse projeto do zero

### 1. Pré-requisitos
- Ter o Docker Desktop instalado e rodando (com WSL2 ativado, no caso do Windows)
- Ter o Git instalado

### 2. Clonar o repositório
```
git clone <url-do-repositorio>
cd shopbrasil-airflow-atividade
```

### 3. Criar o arquivo de configuração
Cria um arquivo chamado `.env` na raiz do projeto com essa linha dentro:
```
AIRFLOW_UID=50000
```

### 4. Inicializar o Airflow
Esse comando só precisa ser rodado uma vez, na primeira vez que for usar o projeto:
```
docker compose up airflow-init
```

### 5. Subir o ambiente
```
docker compose up -d
```

Isso vai demorar um pouco na primeira vez, porque o Docker precisa baixar as imagens. Depois disso, pra conferir se subiu tudo certo:
```
docker compose ps
```

Todos os serviços precisam aparecer como "Up" ou "healthy".

### 6. Acessar o Airflow
Abre o navegador em `http://localhost:8080`

- usuário: `admin`
- senha: `admin`

### 7. Configurar a conexão com o banco
Dentro do Airflow, antes de rodar o pipeline pela primeira vez, é preciso configurar duas coisas (eu só fiz isso uma vez):

**Criar a conexão com o banco de dados:**
- Vai em `Admin > Connections` e clica no `+`
- Preenche assim:
  - Connection Id: `postgres_analytics`
  - Connection Type: `Postgres`
  - Host: `postgres-analytics`
  - Database: `analytics`
  - Login: `analytics`
  - Password: `analytics`
  - Port: `5432`

**Criar o "pool" que limita quantas tarefas rodam ao mesmo tempo:**
- Vai em `Admin > Pools` e clica no `+`
- Preenche assim:
  - Pool: `ecommerce_pool`
  - Slots: `2`

### 8. Ativar e rodar o pipeline
- Na lista de DAGs, procura por `shopbrasil_pricing_pipeline`
- Ativa o botãozinho ao lado do nome dele (de cinza pra azul)
- Clica no nome do DAG e depois no botão de "play" (▶) pra disparar uma execução manualmente, sem precisar esperar até às 6h da manhã

### 9. Conferir se funcionou
Pode acompanhar a execução na própria tela do Airflow (aba "Graph" ou "Grid"). Se tudo ficar verde, deu certo.

Pra conferir os dados direto no banco, dá pra rodar esse comando no terminal:
```
docker exec -it shopbrasil-airflow-atividade-postgres-analytics-1 psql -U analytics -d analytics -c "SELECT * FROM category_price_metrics;"
```

Deve aparecer uma linha pra cada categoria de produto, com o preço médio, mínimo, máximo e a quantidade de produtos.

## Como eu testei que não duplica dados

Esse era um dos requisitos mais importantes: mesmo rodando o pipeline de novo, ele não pode duplicar as informações no banco. Eu testei isso na prática rodando o pipeline duas vezes seguidas e comparando o resultado.

Na primeira vez que rodou, apareceram 4 linhas na tabela (uma por categoria). Quando rodei de novo, continuaram aparecendo as mesmas 4 linhas — só que com o horário de atualização mais recente. Ou seja, ele atualizou os dados, mas não criou linhas duplicadas. Isso acontece porque a tabela tem uma regra (constraint) que impede duas linhas com a mesma categoria e mesma data, e o código usa um comando que, se já existir aquela combinação, atualiza em vez de inserir de novo.

## Algumas decisões que tomei

- **Por que separei o banco do Airflow do banco da análise?** O Airflow precisa de um banco próprio só pra controlar as execuções dele (isso é interno, eu nem mexo nele diretamente). Achei mais correto deixar o banco onde ficam os dados de preços separado, simulando como seria numa empresa de verdade, onde o orquestrador e o banco de dados analítico não são a mesma coisa.

- **Por que as categorias não estão fixas no código?** Porque um dos requisitos era que o pipeline "escalasse sozinho" quando aparecessem categorias novas. Então, em vez de eu escrever uma lista fixa tipo "eletrônicos, roupas, joias", o código pega as categorias direto dos produtos que vêm da API. Se amanhã a API tiver uma categoria nova, o pipeline já processa ela automaticamente, sem eu precisar mudar nada no código.

- **Por que existe um "pool" com 2 slots?** Como o cálculo das métricas roda em paralelo (uma tarefa por categoria), o pool limita pra no máximo 2 dessas tarefas rodarem ao mesmo tempo. Isso evita sobrecarregar a API ou o banco de dados com muitas chamadas simultâneas.
