"""
FASE 3: Módulo de Ingestão e Extração de Dados (extract.py)
Lê as duas bases públicas atribuídas ao projeto:
  1. fonte original.xlsx (SINESP / MJSP - Segurança Pública Nacional)
  2. complemento fogo cruzado.xlsx (Instituto Fogo Cruzado - Microdados Interseccionais)
"""
import logging
from pathlib import Path
import pandas as pd

from etl.config import BASE_DIR, RAW_SINESP_PATH, RAW_FOGO_CRUZADO_PATH, PROCESSED_DIR

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | [EXTRACT] %(message)s",
)
logger = logging.getLogger(__name__)


def extract_sinesp_dataset(filepath: Path = RAW_SINESP_PATH, use_cached_csv: bool = True) -> pd.DataFrame:
    """
    Extrai os dados da base nacional SINESP/MJSP ('fonte original.xlsx', aba '2025').
    Caso o CSV pré-extraído de alta performance exista e use_cached_csv=True,
    utiliza-o para leitura instantânea; caso contrário, lê diretamente do Excel bruto.
    """
    for candidate in [PROCESSED_DIR / "sinesp_tratado.csv", BASE_DIR / "sinesp_tratado.csv"]:
        if use_cached_csv and candidate.exists() and candidate.stat().st_size > 100:
            logger.info(f"Carregando extração otimizada SINESP via CSV: {candidate}")
            df = pd.read_csv(candidate)
            logger.info(f"SINESP carregado com sucesso: {len(df):,} registros ativos.")
            return df

    if not filepath.exists():
        raise FileNotFoundError(f"Arquivo bruto SINESP não encontrado em: {filepath}")

    logger.info(f"Iniciando leitura da planilha bruta SINESP: {filepath} (aba '2025')...")
    df = pd.read_excel(filepath, sheet_name="2025", engine="openpyxl")
    logger.info(f"Leitura bruta concluída: {len(df):,} linhas e {len(df.columns)} colunas.")
    return df


def extract_fogo_cruzado_dataset(
    filepath: Path = RAW_FOGO_CRUZADO_PATH, use_cached_csv: bool = True
) -> pd.DataFrame:
    """
    Extrai os microdados de vítimas do Instituto Fogo Cruzado ('complemento fogo cruzado.xlsx').
    """
    for candidate in [PROCESSED_DIR / "fogo_cruzado_tratado.csv", BASE_DIR / "fogo_cruzado_tratado.csv"]:
        if use_cached_csv and candidate.exists() and candidate.stat().st_size > 100:
            logger.info(f"Carregando extração otimizada Fogo Cruzado via CSV: {candidate}")
            df = pd.read_csv(candidate)
            logger.info(f"Fogo Cruzado carregado com sucesso: {len(df):,} registros de vítimas.")
            return df

    if not filepath.exists():
        raise FileNotFoundError(f"Arquivo bruto Fogo Cruzado não encontrado em: {filepath}")

    logger.info(f"Iniciando leitura da planilha bruta Fogo Cruzado: {filepath}...")
    df = pd.read_excel(filepath, sheet_name=0, engine="openpyxl")
    logger.info(f"Leitura bruta concluída: {len(df):,} linhas e {len(df.columns)} colunas.")
    return df
