"""
FASE 3: Módulo de Carga no Banco Relacional PostgreSQL Aiven (load.py)
Executa a criação do esquema Estrela (schema.sql) e realiza a carga em batches (chunking)
verificando continuamente o consumo de armazenamento para respeitar o limite de 1 GB da Aiven.
"""
import logging
from pathlib import Path
from typing import Dict
import pandas as pd
from sqlalchemy import create_engine, text

from etl.config import CHUNK_SIZE, MAX_DB_SIZE_MB, SCHEMA_SQL_PATH, PROCESSED_DIR, get_database_url

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | [LOAD] %(message)s",
)
logger = logging.getLogger(__name__)


def estimate_payload_size_mb(tables: Dict[str, pd.DataFrame]) -> float:
    """Calcula a memória total estimada (em MB) das tabelas do Star Schema antes da carga."""
    total_bytes = sum(df.memory_usage(deep=True).sum() for df in tables.values())
    return round(total_bytes / (1024 * 1024), 2)


def export_star_schema_locally(tables: Dict[str, pd.DataFrame], out_dir: Path = PROCESSED_DIR) -> None:
    """
    Salva as tabelas do Esquema Estrela na pasta /data/processed para auditoria,
    reprodutibilidade rápida e cache do Dashboard no Streamlit Cloud.
    """
    out_dir.mkdir(parents=True, exist_ok=True)
    for table_name, df in tables.items():
        csv_path = out_dir / f"{table_name}.csv"
        df.to_csv(csv_path, index=False, encoding="utf-8")
        logger.info(f"Tabela exportada localmente: {csv_path} ({len(df):,} linhas)")


def load_to_aiven_postgres(
    tables: Dict[str, pd.DataFrame],
    database_url: str = "",
    chunk_size: int = CHUNK_SIZE,
) -> bool:
    """
    Conecta ao PostgreSQL na Aiven via SQLAlchemy, aplica o DDL (schema.sql) e
    carrega as dimensões e tabelas fato utilizando chunking (`chunksize=5000` e `method='multi'`).
    """
    db_url = database_url or get_database_url()
    est_mb = estimate_payload_size_mb(tables)
    logger.info(
        f"Volume estimado do Star Schema: {est_mb} MB "
        f"(Limite seguro configurado: {MAX_DB_SIZE_MB} MB / 1024 MB Aiven Free Tier)."
    )

    if est_mb > MAX_DB_SIZE_MB:
        raise ValueError(
            f"Volume estimado ({est_mb} MB) excede o limite de segurança ({MAX_DB_SIZE_MB} MB) para o banco Aiven 1 GB!"
        )

    if not db_url:
        logger.warning(
            "Variável DATABASE_URL / DB_HOST não configurada com instância ativa da Aiven no .env. "
            "As tabelas do Star Schema foram salvas localmente em /data/processed e estão prontas para carga assim que o .env for preenchido."
        )
        return False

    logger.info("Conectando ao banco PostgreSQL na Aiven (SSL habilitado)...")
    engine = create_engine(db_url, pool_pre_ping=True)

    with engine.begin() as conn:
        if SCHEMA_SQL_PATH.exists():
            logger.info(f"Executando script DDL do Star Schema: {SCHEMA_SQL_PATH}")
            sql_script = SCHEMA_SQL_PATH.read_text(encoding="utf-8")
            conn.execute(text(sql_script))

    load_order = [
        "dim_tempo",
        "dim_localidade",
        "dim_perfil_vitima",
        "dim_contexto_crime",
        "fato_violencia_sinesp",
        "fato_vitimas_fogo_cruzado",
    ]

    for table_name in load_order:
        if table_name not in tables:
            continue
        df = tables[table_name]
        logger.info(f"Carregando '{table_name}' ({len(df):,} linhas) em chunks de {chunk_size}...")
        df.to_sql(
            name=table_name,
            con=engine,
            if_exists="append",
            index=False,
            chunksize=chunk_size,
            method="multi",
        )
        logger.info(f"Carga de '{table_name}' concluída com sucesso.")

    with engine.connect() as conn:
        db_size = conn.execute(
            text("SELECT pg_size_pretty(pg_database_size(current_database()));")
        ).scalar()
        logger.info(f"Tamanho total ocupado no PostgreSQL Aiven após a carga: {db_size} (Limite: 1 GB).")

    return True
