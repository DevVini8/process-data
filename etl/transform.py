"""
FASE 1 & FASE 3: Tratamento, Limpeza (Data Wrangling), Conformidade LGPD,
Aplicação Estrita dos Critérios de Inclusão e Exclusão da Equipe,
Otimização de Volume (< 1 GB Aiven) e Modelagem Dimensional (Star Schema).

CRITÉRIOS DE INCLUSÃO:
  - Dados entre janeiro e dezembro de 2025
  - Somente Capitais do Brasil
  - Eventos ocorridos por estupro, feminicídio e tentativa de feminicídio (Fonte Original)
  - Vítimas do sexo feminino na fonte original
  - Mulheres entre 18 e 45 anos na fonte de dados do Fogo Cruzado
  - Somente gênero de mulheres cis e mulheres trans e travesti na fonte de dados do Fogo Cruzado

CRITÉRIOS DE EXCLUSÃO:
  - Dados sem gêneros informados na fonte de dados do Fogo Cruzado
  - Dados sem município identificado
  - Dados sem idade informada na fonte de dados do Fogo Cruzado
"""
import hashlib
import logging
import unicodedata
from typing import Dict
import numpy as np
import pandas as pd

from etl.config import (
    UF_METADATA,
    ESTADO_TO_UF,
    MESES_PT,
    CAPITAIS_POR_UF,
    ANO_REFERENCIA,
    EVENTOS_INCLUSAO_SINESP,
    GENEROS_INCLUSAO_FC,
    IDADE_MIN_FC,
    IDADE_MAX_FC,
    LGPD_HASH_SALT,
)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | [TRANSFORM] %(message)s",
)
logger = logging.getLogger(__name__)


def standardize_column_names(df: pd.DataFrame) -> pd.DataFrame:
    """Padroniza nomes de colunas para snake_case sem acentos."""
    clean_cols = []
    for col in df.columns:
        nfkd = unicodedata.normalize("NFKD", str(col).strip().lower())
        ascii_col = "".join([c for c in nfkd if not unicodedata.combining(c)])
        ascii_col = ascii_col.replace(" ", "_").replace("-", "_").replace("/", "_")
        clean_cols.append(ascii_col)
    df.columns = clean_cols
    return df


def strip_accents_upper(text: str) -> str:
    """Auxiliar para comparação normalizada de nomes de capitais."""
    if pd.isna(text):
        return ""
    nfkd = unicodedata.normalize("NFKD", str(text).strip().upper())
    return "".join([c for c in nfkd if not unicodedata.combining(c)])


CAPITAIS_POR_UF_NORM = {uf: strip_accents_upper(cap) for uf, cap in CAPITAIS_POR_UF.items()}
CAPITAIS_SET_NORM = set(CAPITAIS_POR_UF_NORM.values())


def pseudonymize_id_lgpd(raw_id: str) -> str:
    """
    Conformidade LGPD (Art. 12 e Art. 13 - Pseudonimização para estudos e pesquisa):
    Aplica hash criptográfico SHA-256 com salt sobre identificadores originais,
    retornando 16 caracteres hexadecimais irreversíveis.
    """
    if pd.isna(raw_id) or not str(raw_id).strip():
        return "0000000000000000"
    payload = f"{LGPD_HASH_SALT}:{str(raw_id).strip()}".encode("utf-8")
    return hashlib.sha256(payload).hexdigest()[:16]


def normalize_coordinate_lgpd(val, is_lat: bool = False) -> float:
    """
    Corrige coordenadas com erro de ponto decimal na fonte bruta (ex: -129255147 -> -12.93)
    e trunca para 2 casas decimais (~1.1 km de raio) aplicando k-anonimato espacial (LGPD).
    """
    if pd.isna(val) or str(val).strip() == "":
        return np.nan
    try:
        v = float(val)
        if v == 0:
            return np.nan
        limit = 35.0 if is_lat else 75.0
        while abs(v) > limit:
            v /= 10.0
        return round(v, 2)
    except (ValueError, TypeError):
        return np.nan


def classify_macro_genero(genero: str) -> str:
    """Classifica identidade de gênero para análise interseccional ODS 5."""
    g = str(genero).strip().lower()
    if "trans" in g or "travesti" in g:
        return "Mulher Trans/Travesti"
    if "mulher" in g or "feminino" in g:
        return "Mulher Cis"
    return "Outro"


def clean_and_optimize_sinesp(
    df_raw: pd.DataFrame,
    strict_exclude_unidentified_municipality: bool = True,
) -> pd.DataFrame:
    """
    Aplica Data Wrangling e os Critérios de Inclusão e Exclusão da Equipe na Fonte Original (SINESP):
      1. Período: Janeiro a Dezembro de 2025 (ano == 2025 e mes 1..12)
      2. Eventos de Inclusão: Somente 'Estupro', 'Feminicídio' e 'Tentativa de feminicídio'
      3. Critério de Exclusão: Remove dados sem município identificado (municipio nulo, vazio ou 'NÃO INFORMADO')
      4. Recorte Geográfico: Somente Capitais do Brasil (validadas pelo par UF -> Capital Oficial)
      5. Sexo das Vítimas: Somente vítimas do sexo feminino (feminino > 0; colunas masculino e nao_informado zeradas)
    """
    df = standardize_column_names(df_raw.copy())
    initial_rows = len(df)

    # Garante colunas numéricas
    num_cols = ["feminino", "masculino", "nao_informado", "total_vitima", "total", "total_peso"]
    for c in num_cols:
        if c in df.columns:
            df[c] = pd.to_numeric(df[c], errors="coerce").fillna(0.0)

    # Tratamento de strings
    df["uf"] = df["uf"].fillna("").astype(str).str.strip().str.upper()
    df["municipio"] = df["municipio"].fillna("").astype(str).str.strip().str.upper()
    df["evento"] = df["evento"].fillna("").astype(str).str.strip()
    df["agente"] = df.get("agente", pd.Series(["Não informado"] * len(df))).fillna("Não informado").astype(str).str.strip()
    df["arma"] = df.get("arma", pd.Series(["Não informada"] * len(df))).fillna("Não informada").astype(str).str.strip()
    df["faixa_etaria"] = df.get("faixa_etaria", pd.Series(["Não informada"] * len(df))).fillna("Não informada").astype(str).str.strip()
    df["abrangencia"] = df.get("abrangencia", pd.Series(["Estadual"] * len(df))).fillna("Estadual").astype(str).str.strip()

    # Conversão temporal e filtro: Janeiro a Dezembro de 2025
    df["data_referencia"] = pd.to_datetime(df["data_referencia"], errors="coerce")
    df = df.dropna(subset=["data_referencia"]).copy()
    df["ano"] = df["data_referencia"].dt.year.astype(int)
    df["mes"] = df["data_referencia"].dt.month.astype(int)
    df = df[(df["ano"] == ANO_REFERENCIA) & (df["mes"].between(1, 12))].copy()

    # Critério de Inclusão: Somente eventos ocorridos por Estupro, Feminicídio e Tentativa de feminicídio
    df = df[df["evento"].isin(EVENTOS_INCLUSAO_SINESP)].copy()

    # Critério de Exclusão: Dados sem município identificado
    invalid_mun_values = {"", "NAO INFORMADO", "NÃO INFORMADO", "SEM IDENTIFICACAO", "SEM IDENTIFICAÇÃO", "NAN", "NONE"}
    df["mun_norm"] = df["municipio"].apply(strip_accents_upper)
    if strict_exclude_unidentified_municipality:
        df = df[~df["mun_norm"].isin(invalid_mun_values)].copy()

    # Critério de Inclusão: Somente Capitais do Brasil (par UF -> Capital)
    df["capital_esperada_norm"] = df["uf"].map(CAPITAIS_POR_UF_NORM)
    if strict_exclude_unidentified_municipality:
        df = df[df["mun_norm"] == df["capital_esperada_norm"]].copy()
    else:
        # Modo comparativo opcional: mantém capitais identificadas ou aloca registro estadual sem município à capital da UF
        is_cap = df["mun_norm"] == df["capital_esperada_norm"]
        is_unid = df["mun_norm"].isin(invalid_mun_values)
        df = df[is_cap | is_unid].copy()
        df.loc[is_unid, "municipio"] = df.loc[is_unid, "uf"].map(CAPITAIS_POR_UF)

    # Padroniza o nome da Capital com acentuação oficial
    df["municipio"] = df["uf"].map(CAPITAIS_POR_UF)

    # Critério de Inclusão: Somente vítimas do sexo feminino na fonte original
    df["feminino"] = df["feminino"].round().astype(int)
    df = df[df["feminino"] > 0].copy()
    df["masculino"] = 0
    df["nao_informado"] = 0
    df["total_vitima"] = df["feminino"]

    # Consolidação mensal por Capital e Evento (preservando 100% da soma de vítimas femininas)
    df = df.drop(columns=["mun_norm", "capital_esperada_norm"], errors="ignore")
    group_cols = [
        "uf", "municipio", "evento", "data_referencia", "ano", "mes",
        "agente", "arma", "faixa_etaria", "abrangencia",
    ]
    df = (
        df.groupby(group_cols, as_index=False)
        .agg(
            feminino=("feminino", "sum"),
            masculino=("masculino", "sum"),
            nao_informado=("nao_informado", "sum"),
            total_vitima=("total_vitima", "sum"),
            total=("total", "sum"),
            total_peso=("total_peso", "sum"),
        )
    )

    # Atributos temporais e geográficos derivados
    df["nome_mes"] = df["mes"].map(MESES_PT)
    df["trimestre"] = df["data_referencia"].dt.quarter.astype(int)
    df["semestre"] = np.where(df["mes"] <= 6, 1, 2)
    df["id_tempo"] = df["data_referencia"].dt.strftime("%Y%m%d").astype(int)
    df["estado"] = df["uf"].map(lambda u: UF_METADATA.get(u, ("Nacional", "Nacional"))[0])
    df["regiao"] = df["uf"].map(lambda u: UF_METADATA.get(u, ("Nacional", "Nacional"))[1])

    logger.info(
        f"SINESP filtrado pelos Critérios da Equipe: {initial_rows:,} linhas brutas -> "
        f"{len(df):,} registros nas Capitais em 2025 ({df['feminino'].sum():,} vítimas do sexo feminino)."
    )
    return df.reset_index(drop=True)


def clean_and_anonymize_fogo_cruzado(df_raw: pd.DataFrame) -> pd.DataFrame:
    """
    Aplica Data Wrangling, Conformidade LGPD e os Critérios de Inclusão e Exclusão da Equipe
    na fonte de dados do Instituto Fogo Cruzado:
      1. Conformidade LGPD: Remove endereço exato e dados funcionais individualizados;
         pseudonimiza IDs (SHA-256) e trunca coordenadas para 2 casas decimais.
      2. Período de Inclusão: Janeiro a Dezembro de 2025 (ano == 2025 e mes 1..12)
      3. Critério de Exclusão (Município) + Inclusão (Somente Capitais do Brasil):
         Exclui dados sem município identificado e mantém apenas capitais (Salvador, Recife, Belém, Rio de Janeiro)
      4. Critério de Exclusão (Gênero) + Inclusão (Somente Mulher cis e Mulher trans e travesti):
         Exclui dados sem gênero informado e homens
      5. Critério de Exclusão (Idade) + Inclusão (Mulheres entre 18 e 45 anos):
         Exclui dados sem idade informada (nulos/vazios) e mantém apenas 18 <= idade <= 45
    """
    df = standardize_column_names(df_raw.copy())
    initial_rows = len(df)

    # 1. Conformidade LGPD: Remoção de colunas sensíveis / PII indireto
    cols_to_drop_lgpd = [
        "endereco",
        "ocorrencia_numero",
        "corporacao",
        "cargo_agente",
        "status_agente",
        "unidade",
        "status_servico",
        "qualificacoes",
        "recortes",
        "motivos_complementares",
        "data_morte",
    ]
    existing_drop = [c for c in cols_to_drop_lgpd if c in df.columns]
    if existing_drop:
        df = df.drop(columns=existing_drop)

    # Pseudonimização SHA-256 de IDs (LGPD Art. 12/13)
    if "vitima_id" in df.columns and "vitima_hash" not in df.columns:
        df["vitima_hash"] = df["vitima_id"].apply(pseudonymize_id_lgpd)
        df = df.drop(columns=["vitima_id"])
    elif "vitima_hash" in df.columns:
        df["vitima_hash"] = df["vitima_hash"].fillna("00000000").astype(str).str.slice(0, 16)

    if "ocorrencia_id" in df.columns and "ocorrencia_hash" not in df.columns:
        df["ocorrencia_hash"] = df["ocorrencia_id"].apply(pseudonymize_id_lgpd)
        df = df.drop(columns=["ocorrencia_id"])
    elif "ocorrencia_hash" in df.columns:
        df["ocorrencia_hash"] = df["ocorrencia_hash"].fillna("00000000").astype(str).str.slice(0, 16)

    # Coordenadas aproximadas (LGPD k-anonimato)
    if "latitude" in df.columns and "lat_aprox" not in df.columns:
        df["lat_aprox"] = df["latitude"].apply(lambda x: normalize_coordinate_lgpd(x, is_lat=True))
        df = df.drop(columns=["latitude"])
    if "longitude" in df.columns and "lon_aprox" not in df.columns:
        df["lon_aprox"] = df["longitude"].apply(lambda x: normalize_coordinate_lgpd(x, is_lat=False))
        df = df.drop(columns=["longitude"])

    # 2. Conversão temporal e Critério de Inclusão: Janeiro a Dezembro de 2025
    df["data"] = pd.to_datetime(df["data"], errors="coerce")
    df = df.dropna(subset=["data"]).copy()
    df["ano"] = df["data"].dt.year.astype(int)
    df["mes"] = df["data"].dt.month.astype(int)
    df = df[(df["ano"] == ANO_REFERENCIA) & (df["mes"].between(1, 12))].copy()

    # 3. Critério de Exclusão (Sem município identificado) + Inclusão (Somente Capitais do Brasil)
    df["estado"] = df["estado"].fillna("").astype(str).str.strip()
    if "uf" not in df.columns:
        df["uf"] = df["estado"].str.lower().map(lambda e: ESTADO_TO_UF.get(e, "BR"))
    if "regiao" not in df.columns:
        df["regiao"] = df["uf"].map(lambda u: UF_METADATA.get(u, ("Nacional", "Nacional"))[1])

    df["cidade"] = df["cidade"].fillna("").astype(str).str.strip()
    invalid_city_vals = {"", "NAO INFORMADO", "NÃO INFORMADO", "NAO INFORMADA", "NÃO INFORMADA", "SEM IDENTIFICACAO", "SEM IDENTIFICAÇÃO"}
    df["cidade_norm"] = df["cidade"].apply(strip_accents_upper)
    df = df[~df["cidade_norm"].isin(invalid_city_vals)].copy()
    df = df[df["cidade_norm"].isin(CAPITAIS_SET_NORM)].copy()
    df["cidade"] = df["uf"].map(CAPITAIS_POR_UF)

    df["bairro"] = df["bairro"].fillna("NÃO INFORMADO").astype(str).str.strip().str.upper()
    df.loc[df["bairro"] == "", "bairro"] = "NÃO INFORMADO"

    # 4. Critério de Exclusão (Sem gênero informado) + Inclusão (Somente Mulher cis e Mulher trans e travesti)
    df["genero"] = df["genero"].fillna("").astype(str).str.strip()
    df = df[df["genero"].isin(GENEROS_INCLUSAO_FC)].copy()
    df["macro_genero"] = df["genero"].apply(classify_macro_genero)

    # 5. Critério de Exclusão (Sem idade informada) + Inclusão (Mulheres entre 18 e 45 anos)
    df["idade"] = pd.to_numeric(df["idade"], errors="coerce")
    df = df.dropna(subset=["idade"]).copy()
    df["idade"] = df["idade"].round().astype(int)
    df = df[df["idade"].between(IDADE_MIN_FC, IDADE_MAX_FC)].copy()

    # Variáveis adicionais de análise interseccional (ODS 5 e ODS 18)
    df["raca"] = df["raca"].fillna("Não identificado").astype(str).str.strip()
    df.loc[df["raca"] == "", "raca"] = "Não identificado"
    df["faixa_etaria"] = np.where(df["idade"] <= 24, "Jovem Adulta (18-24)",
                         np.where(df["idade"] <= 34, "Adulta (25-34)", "Adulta (35-45)"))

    sit_map = {"Dead": "Morto", "Wounded": "Ferido", "Morto": "Morto", "Ferido": "Ferido"}
    df["situacao"] = df["situacao"].map(lambda s: sit_map.get(str(s).strip(), "Ferido"))
    df["is_fatal"] = np.where(df["situacao"] == "Morto", 1, 0)

    tp_map = {
        "Civilian": "Civil",
        "Agent": "Agente de Segurança",
        "Civil": "Civil",
        "Agente de Segurança": "Agente de Segurança",
    }
    df["tipo_pessoa"] = df["tipo_pessoa"].map(lambda t: tp_map.get(str(t).strip(), "Civil"))

    df["grupo_interseccional"] = df["genero"] + " - " + df["raca"]

    df["acao_policial"] = pd.to_numeric(df["acao_policial"], errors="coerce").fillna(0).astype(int)
    df["presenca_agente"] = pd.to_numeric(df["presenca_agente"], errors="coerce").fillna(0).astype(int)
    df["chacina"] = pd.to_numeric(df["chacina"], errors="coerce").fillna(0).astype(int)
    df["motivo_principal"] = df["motivo_principal"].fillna("Não identificado").astype(str).str.strip()
    df["circunstancias"] = df.get("circunstancias", pd.Series(["Sem identificação"] * len(df))).fillna("Sem identificação").astype(str).str.strip()
    df["local"] = df.get("local", pd.Series(["Sem identificação"] * len(df))).fillna("Sem identificação").astype(str).str.strip()

    # Atributos temporais derivados
    df["nome_mes"] = df["mes"].map(MESES_PT)
    df["trimestre"] = df["data"].dt.quarter.astype(int)
    df["semestre"] = np.where(df["mes"] <= 6, 1, 2)
    df["id_tempo"] = df["data"].dt.strftime("%Y%m%d").astype(int)

    df = df.drop_duplicates().drop(columns=["cidade_norm"], errors="ignore")
    logger.info(
        f"Fogo Cruzado filtrado pelos Critérios da Equipe e LGPD: {initial_rows:,} -> "
        f"{len(df):,} mulheres (18-45 anos, Mulher cis / Mulher trans e travesti) nas Capitais em 2025."
    )
    return df.reset_index(drop=True)


def build_star_schema_tables(
    df_sinesp: pd.DataFrame, df_fc: pd.DataFrame
) -> Dict[str, pd.DataFrame]:
    """
    Constrói as 4 Tabelas de Dimensão e as 2 Tabelas Fato do Esquema Estrela (Star Schema):
      - dim_tempo
      - dim_localidade
      - dim_perfil_vitima
      - dim_contexto_crime
      - fato_violencia_sinesp
      - fato_vitimas_fogo_cruzado
    """
    logger.info("Construindo tabelas dimensionais e fatos (Star Schema)...")

    # 1. dim_tempo
    t_sinesp = df_sinesp[["id_tempo", "data_referencia", "ano", "mes", "nome_mes", "trimestre", "semestre"]].rename(
        columns={"data_referencia": "data_completa"}
    )
    t_fc = df_fc[["id_tempo", "data", "ano", "mes", "nome_mes", "trimestre", "semestre"]].rename(
        columns={"data": "data_completa"}
    )
    dim_tempo = (
        pd.concat([t_sinesp, t_fc], ignore_index=True)
        .drop_duplicates(subset=["id_tempo"])
        .sort_values("id_tempo")
        .reset_index(drop=True)
    )
    dim_tempo["data_completa"] = pd.to_datetime(dim_tempo["data_completa"]).dt.strftime("%Y-%m-%d")

    # 2. dim_localidade (Somente Capitais do Brasil)
    loc_sinesp = pd.DataFrame({
        "regiao": df_sinesp["regiao"],
        "uf": df_sinesp["uf"],
        "estado": df_sinesp["estado"],
        "municipio": df_sinesp["municipio"],
        "bairro": "NÃO ESPECIFICADO",
    })
    loc_fc = pd.DataFrame({
        "regiao": df_fc["regiao"],
        "uf": df_fc["uf"],
        "estado": df_fc["estado"],
        "municipio": df_fc["cidade"],
        "bairro": df_fc["bairro"],
    })
    dim_localidade = (
        pd.concat([loc_sinesp, loc_fc], ignore_index=True)
        .drop_duplicates(subset=["uf", "municipio", "bairro"])
        .sort_values(["regiao", "uf", "municipio", "bairro"])
        .reset_index(drop=True)
    )
    dim_localidade["id_localidade"] = dim_localidade.index + 1

    # 3. dim_perfil_vitima (Mulheres 18-45 anos: Mulher cis & Mulher trans e travesti x Raça/Cor)
    dim_perfil_vitima = (
        df_fc[["genero", "macro_genero", "raca", "faixa_etaria", "tipo_pessoa", "grupo_interseccional"]]
        .rename(columns={"raca": "raca_cor"})
        .drop_duplicates(subset=["genero", "raca_cor", "faixa_etaria", "tipo_pessoa"])
        .sort_values(["genero", "raca_cor", "faixa_etaria"])
        .reset_index(drop=True)
    )
    dim_perfil_vitima["id_perfil"] = dim_perfil_vitima.index + 1

    # 4. dim_contexto_crime
    ctx_sinesp = pd.DataFrame({
        "fonte_dados": "SINESP_MJSP",
        "tipo_evento": df_sinesp["evento"],
        "arma": df_sinesp["arma"],
        "agente_seguranca": df_sinesp["agente"],
        "acao_policial": False,
        "presenca_agente": False,
        "chacina": False,
        "circunstancia": "Registro Oficial SINESP (Capitais 2025)",
        "local_ocorrencia": df_sinesp["abrangencia"],
    })
    ctx_fc = pd.DataFrame({
        "fonte_dados": "FOGO_CRUZADO",
        "tipo_evento": df_fc["motivo_principal"],
        "arma": "Arma de fogo",
        "agente_seguranca": np.where(df_fc["presenca_agente"] == 1, "Agente de Segurança Presente", "Sem Agente Identificado"),
        "acao_policial": df_fc["acao_policial"] == 1,
        "presenca_agente": df_fc["presenca_agente"] == 1,
        "chacina": df_fc["chacina"] == 1,
        "circunstancia": df_fc["circunstancias"],
        "local_ocorrencia": df_fc["local"],
    })
    ctx_cols = [
        "fonte_dados", "tipo_evento", "arma", "agente_seguranca",
        "acao_policial", "presenca_agente", "chacina", "circunstancia", "local_ocorrencia",
    ]
    dim_contexto_crime = (
        pd.concat([ctx_sinesp, ctx_fc], ignore_index=True)
        .drop_duplicates(subset=ctx_cols)
        .reset_index(drop=True)
    )
    dim_contexto_crime["id_contexto"] = dim_contexto_crime.index + 1

    # 5. fato_violencia_sinesp (Apenas Vítimas do Sexo Feminino nas Capitais em 2025)
    sinesp_loc = dim_localidade[dim_localidade["bairro"] == "NÃO ESPECIFICADO"][
        ["uf", "municipio", "id_localidade"]
    ]
    f_sinesp = df_sinesp.merge(sinesp_loc, on=["uf", "municipio"], how="left")
    sinesp_ctx = dim_contexto_crime[dim_contexto_crime["fonte_dados"] == "SINESP_MJSP"][
        ["tipo_evento", "arma", "agente_seguranca", "local_ocorrencia", "id_contexto"]
    ]
    f_sinesp = f_sinesp.merge(
        sinesp_ctx,
        left_on=["evento", "arma", "agente", "abrangencia"],
        right_on=["tipo_evento", "arma", "agente_seguranca", "local_ocorrencia"],
        how="left",
    )
    fato_violencia_sinesp = pd.DataFrame({
        "id_fato_sinesp": range(1, len(f_sinesp) + 1),
        "id_tempo": f_sinesp["id_tempo"],
        "id_localidade": f_sinesp["id_localidade"],
        "id_contexto": f_sinesp["id_contexto"],
        "vitimas_feminino": f_sinesp["feminino"],
        "vitimas_masculino": 0,
        "vitimas_nao_inf": 0,
        "total_vitimas": f_sinesp["feminino"],
        "total_ocorrencias": f_sinesp["total"],
        "total_peso": f_sinesp["total_peso"],
    })

    # 6. fato_vitimas_fogo_cruzado (Mulheres 18-45 anos, Cis e Trans/Travesti, Capitais 2025)
    f_fc = df_fc.merge(
        dim_localidade[["uf", "municipio", "bairro", "id_localidade"]],
        left_on=["uf", "cidade", "bairro"],
        right_on=["uf", "municipio", "bairro"],
        how="left",
    )
    f_fc = f_fc.merge(
        dim_perfil_vitima[["genero", "raca_cor", "faixa_etaria", "tipo_pessoa", "id_perfil"]],
        left_on=["genero", "raca", "faixa_etaria", "tipo_pessoa"],
        right_on=["genero", "raca_cor", "faixa_etaria", "tipo_pessoa"],
        how="left",
    )
    f_fc["acao_bool"] = f_fc["acao_policial"] == 1
    f_fc["pres_bool"] = f_fc["presenca_agente"] == 1
    f_fc["chac_bool"] = f_fc["chacina"] == 1
    fc_ctx = dim_contexto_crime[dim_contexto_crime["fonte_dados"] == "FOGO_CRUZADO"][
        ["tipo_evento", "acao_policial", "presenca_agente", "chacina", "circunstancia", "local_ocorrencia", "id_contexto"]
    ]
    f_fc = f_fc.merge(
        fc_ctx,
        left_on=["motivo_principal", "acao_bool", "pres_bool", "chac_bool", "circunstancias", "local"],
        right_on=["tipo_evento", "acao_policial", "presenca_agente", "chacina", "circunstancia", "local_ocorrencia"],
        how="left",
    )
    fato_vitimas_fogo_cruzado = pd.DataFrame({
        "id_fato_fc": range(1, len(f_fc) + 1),
        "vitima_hash_lgpd": f_fc["vitima_hash"].astype(str).str.slice(0, 16),
        "ocorrencia_hash": f_fc["ocorrencia_hash"].astype(str).str.slice(0, 16),
        "id_tempo": f_fc["id_tempo"],
        "id_localidade": f_fc["id_localidade"],
        "id_perfil": f_fc["id_perfil"],
        "id_contexto": f_fc["id_contexto"],
        "situacao_vitima": f_fc["situacao"],
        "is_fatal": f_fc["is_fatal"],
        "idade_vitima": f_fc["idade"],
        "lat_aprox": f_fc["lat_aprox"],
        "lon_aprox": f_fc["lon_aprox"],
    })

    tables = {
        "dim_tempo": dim_tempo,
        "dim_localidade": dim_localidade,
        "dim_perfil_vitima": dim_perfil_vitima,
        "dim_contexto_crime": dim_contexto_crime,
        "fato_violencia_sinesp": fato_violencia_sinesp,
        "fato_vitimas_fogo_cruzado": fato_vitimas_fogo_cruzado,
    }
    for name, tbl in tables.items():
        logger.info(f"  -> Tabela '{name}': {len(tbl):,} registros gerados.")
    return tables
