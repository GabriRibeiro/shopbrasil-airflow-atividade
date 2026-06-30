from __future__ import annotations

import logging
from datetime import timedelta

import pendulum
import requests
from airflow.decorators import dag, task, task_group
from airflow.providers.postgres.hooks.postgres import PostgresHook

logger = logging.getLogger(__name__)

FAKESTORE_BASE_URL = "https://fakestoreapi.com"
POSTGRES_CONN_ID = "postgres_analytics"
TARGET_TABLE = "category_price_metrics"
POOL_NAME = "ecommerce_pool"

LOCAL_TZ = pendulum.timezone("America/Sao_Paulo")


def on_failure_callback(context):
    ti = context["task_instance"]
    logger.error(
        "[ALERTA] Task '%s' falhou definitivamente. dag_run=%s",
        ti.task_id,
        context["run_id"],
    )


def on_retry_callback(context):
    ti = context["task_instance"]
    logger.warning(
        "[RETRY] Task '%s' será reexecutada. Tentativa atual: %s",
        ti.task_id,
        ti.try_number,
    )


def on_success_callback(context):
    ti = context["task_instance"]
    logger.info("[OK] Task '%s' concluída com sucesso.", ti.task_id)


default_args = {
    "owner": "data-team-shopbrasil",
    "retries": 3,
    "retry_delay": timedelta(seconds=30),
    "retry_exponential_backoff": True,
    "max_retry_delay": timedelta(minutes=10),
}


@dag(
    dag_id="shopbrasil_pricing_pipeline",
    description="Pipeline diário de métricas de preço por categoria - ShopBrasil",
    schedule="0 6 * * *",
    start_date=pendulum.datetime(2024, 1, 1, tz=LOCAL_TZ),
    catchup=False,
    default_args=default_args,
    tags=["shopbrasil", "pricing", "taskflow"],
)
def shopbrasil_pricing_pipeline():

    @task_group(group_id="ingestao")
    def ingestao_group():

        @task(
            task_id="buscar_produtos",
            on_failure_callback=on_failure_callback,
            on_retry_callback=on_retry_callback,
            on_success_callback=on_success_callback,
        )
        def buscar_produtos() -> list[dict]:
            url = f"{FAKESTORE_BASE_URL}/products"
            try:
                response = requests.get(url, timeout=10)
                response.raise_for_status()
                produtos = response.json()
            except requests.exceptions.RequestException as exc:
                logger.error("Falha de comunicação com a FakeStore API: %s", exc)
                raise
            except ValueError as exc:
                logger.error("Resposta da API não é um JSON válido: %s", exc)
                raise

            if not produtos:
                raise ValueError("A API retornou uma lista vazia de produtos.")

            logger.info("Coleta concluída: %s produtos recebidos.", len(produtos))
            return produtos

        @task(task_id="extrair_categorias")
        def extrair_categorias(produtos: list[dict]) -> list[str]:
            categorias = sorted({produto["category"] for produto in produtos})
            logger.info("Categorias identificadas: %s", categorias)
            return categorias

        produtos = buscar_produtos()
        categorias = extrair_categorias(produtos)
        return produtos, categorias

    @task_group(group_id="analise")
    def analise_group(produtos: list[dict], categorias: list[str]):

        @task(task_id="calcular_metricas_categoria", pool=POOL_NAME)
        def calcular_metricas_categoria(categoria: str, produtos: list[dict]) -> dict:
            precos = [
                float(produto["price"])
                for produto in produtos
                if produto["category"] == categoria
            ]

            if not precos:
                logger.warning("Categoria '%s' sem produtos no momento.", categoria)
                return {
                    "categoria": categoria,
                    "preco_medio": 0.0,
                    "preco_minimo": 0.0,
                    "preco_maximo": 0.0,
                    "qtd_produtos": 0,
                }

            metrica = {
                "categoria": categoria,
                "preco_medio": round(sum(precos) / len(precos), 2),
                "preco_minimo": round(min(precos), 2),
                "preco_maximo": round(max(precos), 2),
                "qtd_produtos": len(precos),
            }
            logger.info("Métricas da categoria '%s': %s", categoria, metrica)
            return metrica

        @task(task_id="consolidar_metricas")
        def consolidar_metricas(lista_metricas: list[dict]) -> list[dict]:
            logger.info(
                "Consolidando métricas de %s categorias.", len(lista_metricas)
            )
            return lista_metricas

        metricas_mapeadas = calcular_metricas_categoria.partial(
            produtos=produtos
        ).expand(categoria=categorias)

        return consolidar_metricas(metricas_mapeadas)

    @task(task_id="salvar_postgres")
    def salvar_postgres(metricas: list[dict]) -> None:
        hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)

        create_table_sql = f"""
        CREATE TABLE IF NOT EXISTS {TARGET_TABLE} (
            categoria       VARCHAR(100)   NOT NULL,
            data_execucao   DATE           NOT NULL,
            preco_medio     NUMERIC(10, 2) NOT NULL,
            preco_minimo    NUMERIC(10, 2) NOT NULL,
            preco_maximo    NUMERIC(10, 2) NOT NULL,
            qtd_produtos    INTEGER        NOT NULL,
            atualizado_em   TIMESTAMP      NOT NULL DEFAULT NOW(),
            CONSTRAINT uq_categoria_execucao UNIQUE (categoria, data_execucao)
        );
        """
        hook.run(create_table_sql)

        upsert_sql = f"""
        INSERT INTO {TARGET_TABLE} (
            categoria, data_execucao, preco_medio,
            preco_minimo, preco_maximo, qtd_produtos
        )
        VALUES (
            %(categoria)s, %(data_execucao)s, %(preco_medio)s,
            %(preco_minimo)s, %(preco_maximo)s, %(qtd_produtos)s
        )
        ON CONFLICT (categoria, data_execucao)
        DO UPDATE SET
            preco_medio   = EXCLUDED.preco_medio,
            preco_minimo  = EXCLUDED.preco_minimo,
            preco_maximo  = EXCLUDED.preco_maximo,
            qtd_produtos  = EXCLUDED.qtd_produtos,
            atualizado_em = NOW();
        """

        data_execucao = pendulum.now(LOCAL_TZ).to_date_string()

        for metrica in metricas:
            params = {**metrica, "data_execucao": data_execucao}
            hook.run(upsert_sql, parameters=params)

        logger.info(
            "Persistência concluída: %s linhas gravadas/atualizadas em '%s'.",
            len(metricas),
            TARGET_TABLE,
        )

    produtos, categorias = ingestao_group()
    metricas_consolidadas = analise_group(produtos, categorias)
    salvar_postgres(metricas_consolidadas)


shopbrasil_pricing_pipeline()