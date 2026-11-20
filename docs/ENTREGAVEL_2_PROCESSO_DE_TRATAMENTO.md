# ⚙️ Entregável 2: Processo de Tratamento dos Dados (Pipeline ETL)

**Projeto:** Observatório Interseccional da Violência contra a Mulher nas Capitais do Brasil (2025)  
**Alinhamento Agenda 2030:** **ODS 5** (Igualdade de Gênero) e **ODS 18** (Igualdade Étnico-Racial)  
**Módulos de Código no Repositório:** `/etl/config.py`, `/etl/extract.py`, `/etl/transform.py`, `/etl/load.py`, `/etl/schema.sql`, `/etl/pipeline.py`

---

## 1. Visão Geral da Arquitetura do Processo de Tratamento

O processo de transformação das duas bases de dados brutas (`fonte original.xlsx` — SINESP/MJSP e `complemento fogo cruzado.xlsx` — Instituto Fogo Cruzado) na base final tratada e dimensionalizada foi projetado sob o paradigma de **Pipeline ETL Modular (Extract, Transform, Load)** em **Python**.

A arquitetura do código no diretório `/etl` separa responsabilidades em cinco módulos independentes e auditáveis:

1. **`etl/config.py` (Configuração, Segurança e Metadados Oficiais):** Centraliza a leitura segura de variáveis de ambiente (`.env`), parâmetros de chunking (`ETL_CHUNK_SIZE=5000`), constantes dos **Critérios de Inclusão e Exclusão da Equipe** (`ANO_REFERENCIA = 2025`, `EVENTOS_INCLUSAO_SINESP`, `GENEROS_INCLUSAO_FC`, `IDADE_MIN_FC = 18`, `IDADE_MAX_FC = 45`) e o dicionário oficial das **27 Capitais do Brasil (`CAPITAIS_POR_UF`)** e Macrorregiões do IBGE (`UF_METADATA`).
2. **`etl/extract.py` (Ingestão Resiliente):** Realiza a leitura estruturada das planilhas originais em formato OpenXML (`.xlsx`) com suporte a cache otimizado em `.csv` e registro detalhado de logs de execução.
3. **`etl/transform.py` (Motor de Data Wrangling, Filtros da Equipe, LGPD e Star Schema):** Executa todas as regras de limpeza, conversão tipada, filtragem pelos critérios de inclusão e exclusão da equipe, anonimização em conformidade com a LGPD (Lei nº 13.709/2018) e construção das tabelas dimensionais e tabelas fato.
4. **`etl/load.py` e `etl/schema.sql` (Persistência Relacional e Controle de Volume < 1 GB):** Valida o consumo de memória antes da carga, aplica o DDL do Esquema Estrela (`schema.sql`) e insere os dados em lotes (`chunksize=5000`, `method='multi'`) no PostgreSQL gerenciado na Aiven Cloud (com SSL obrigatório) e em arquivos CSV tratados na pasta `/data/processed`.
5. **`etl/pipeline.py` (Orquestrador CLI):** Ponto de entrada único via linha de comando (`python -m etl.pipeline`) que encadeia Extração → Transformação → Carga e contabiliza o tempo total de execução.

---

## 2. Ferramentas e Tecnologias Utilizadas (e Justificativas)

| Tecnologia / Biblioteca | Papel no Processo de Tratamento | Justificativa Técnica para Utilização |
| :--- | :--- | :--- |
| **Python 3.11+** | Linguagem principal de engenharia de dados | Padrão de mercado para pipelines de ciência de dados e ETL, com tipagem clara, portabilidade multiplataforma e integração nativa com bancos relacionais em nuvem. |
| **Pandas & NumPy** | Manipulação vetorial, limpeza, filtragem e agregação em memória | Operações vetorizadas em C/Cython permitem filtrar as `832.293` linhas do SINESP e `45.025` linhas do Fogo Cruzado em poucos segundos, além de suportar `groupby`, `pivot_table` e joins relacionais (`merge`) para montagem do Star Schema. |
| **OpenPyXL** | Leitura de planilhas Microsoft Excel (`.xlsx`) | Suporte completo ao padrão OpenXML para leitura direta das abas `2025` (`fonte original.xlsx`) e `vitimas (2)` (`complemento fogo cruzado.xlsx`). |
| **Hashlib (SHA-256) & Unicodedata** | Pseudonimização criptográfica (LGPD) e normalização textual Unicode | `hashlib.sha256` garante anonimização irreversível com *salt* dos identificadores de vítimas (Art. 12 e 13 da LGPD), enquanto `unicodedata.normalize('NFKD')` elimina falhas de comparação causadas por acentuação nos nomes de capitais (ex: `BELÉM` vs. `BELEM`, `SÃO PAULO` vs. `SAO PAULO`). |
| **SQLAlchemy & Psycopg2** | Camada de abstração SQL e driver nativo PostgreSQL | Permite conexão segura com `sslmode=require` na plataforma Aiven e escrita otimizada em lotes (`method='multi'`, `chunksize=5000`), evitando sobrecarga de memória e timeouts de rede. |
| **Python-dotenv** | Gestão segura de segredos e variáveis de ambiente | Isola credenciais sensíveis (`DATABASE_URL`, `DB_PASSWORD`, `LGPD_HASH_SALT`) em arquivo `.env` ignorado pelo Git (`.gitignore`), cumprindo as boas práticas de segurança de DevSecOps. |

---

## 3. Detalhamento Técnico do Processo de Transformação (`etl/transform.py`)

### 3.1. Padronização Estrutural e Normalização Unicode
Logo na entrada do pipeline, duas funções utilitárias garantem consistência sintática entre as fontes:
- **`standardize_column_names(df)`:** Decompõe caracteres acentuados via Unicode `NFKD`, converte todos os nomes de colunas para letras minúsculas e substitui espaços e barras por sublinhados (`snake_case`).
- **`strip_accents_upper(text)`:** Normaliza nomes de municípios e capitais em caixa alta sem diacríticos, criando o dicionário `CAPITAIS_POR_UF_NORM` para validação exata do par `(UF, Capital)` sem risco de falsos negativos por diferenças de codificação.

### 3.2. Fluxo de Tratamento da Fonte Original — SINESP (`clean_and_optimize_sinesp`)
O método `clean_and_optimize_sinesp` transforma os `832.293` registros brutos na base consolidada das capitais seguindo seis passos sequenciais:

1. **Coerção Numérica e Tratamento de Nulos:** As colunas métricas (`feminino`, `masculino`, `nao_informado`, `total_vitima`, `total`, `total_peso`) são convertidas via `pd.to_numeric(..., errors="coerce").fillna(0.0)`, prevenindo erros de tipo provocados por células vazias na planilha original.
2. **Conversão Temporal e Critério de Inclusão 1 (Jan–Dez 2025):** A coluna `data_referencia` é convertida para `datetime`, extraindo-se `ano` e `mes` e aplicando o filtro estrito `(df["ano"] == 2025) & (df["mes"].between(1, 12))`.
3. **Critério de Inclusão 3 (Eventos de Violência de Gênero):** Aplica-se `df[df["evento"].isin(["Estupro", "Feminicídio", "Tentativa de feminicídio"])]`, descartando imediatamente todas as outras 26 categorias criminais ou administrativas fora do escopo da equipe.
4. **Critério de Exclusão 2 (Sem Município Identificado) + Inclusão 2 (Somente Capitais do Brasil):**
   - Remove-se qualquer registro cujo município normalizado pertença ao conjunto de valores inválidos (`{"", "NAO INFORMADO", "NÃO INFORMADO", "SEM IDENTIFICACAO"}`).
   - Valida-se cada linha comparando o município com a capital oficial daquela UF (`df["mun_norm"] == df["uf"].map(CAPITAIS_POR_UF_NORM)`). Essa validação cruzada por `UF + Capital` impede que municípios homônimos do interior (como *Belém/AL*, *Belém/PB* ou *Campo Grande/RN*) sejam incluídos indevidamente como se fossem as capitais *Belém/PA* ou *Campo Grande/MS*.
5. **Critério de Inclusão 4 (Somente Vítimas do Sexo Feminino):** Filtra-se estritamente `df[df["feminino"] > 0]`, zerando as colunas `masculino = 0` e `nao_informado = 0` e igualando `total_vitima = feminino`. Essa etapa também elimina todas as linhas cartesianas zeradas da planilha original, otimizando o volume para o limite de 1 GB da Aiven.
6. **Consolidação Mensal por Capital e Evento:** Executa-se `df.groupby(group_cols, as_index=False).agg(...)` somando `feminino`. Essa decisão técnica resolve duplicidades de lançamento dentro do mesmo mês/capital na fonte original (como múltiplas linhas mensais registradas nas regiões administrativas de Brasília/DF), preservando 100% das **1.233 vítimas femininas** nas 27 Capitais.

### 3.3. Fluxo de Tratamento e LGPD — Instituto Fogo Cruzado (`clean_and_anonymize_fogo_cruzado`)
O método `clean_and_anonymize_fogo_cruzado` processa os `45.025` registros brutos de vítimas baleadas aplicando governança LGPD e o funil metodológico da equipe:

1. **Minimização de Dados Sensíveis e Quase-Identificadores (LGPD Art. 6º, III):**
   Remove explicitamente 11 colunas que não são utilizadas nas métricas interseccionais ou que oferecem risco de reidentificação física/funcional:
   ```python
   cols_to_drop_lgpd = [
       "endereco", "ocorrencia_numero", "corporacao", "cargo_agente",
       "status_agente", "unidade", "status_servico", "qualificacoes",
       "recortes", "motivos_complementares", "data_morte",
   ]
   ```
2. **Pseudonimização Irreversível com SHA-256 (LGPD Arts. 12 e 13):**
   Substitui os UUIDs originais (`vitima_id` e `ocorrencia_id`) por hashes criptográficos de 16 caracteres gerados com *salt* secreto (`pseudonymize_id_lgpd`), preservando a capacidade de contagem distinta (`COUNT(DISTINCT vitima_hash_lgpd)`) sem permitir rastreamento reverso.
3. **Saneamento Algorítmico de Coordenadas e *k*-Anonimato Espacial (`normalize_coordinate_lgpd`):**
   Na planilha bruta, diversas coordenadas apresentavam perda de separador decimal (ex: latitude `-129586607` ou `-8054123` em vez de `-12.96` ou `-8.05`). O algoritmo divide iterativamente o valor por `10.0` até que a latitude entre no intervalo territorial brasileiro (`|lat| <= 35.0`) e a longitude em `|lon| <= 75.0`, arredondando em seguida para **2 casas decimais** (`round(v, 2)`), o que cria células espaciais de ~1,1 km de raio para proteção de privacidade geográfica.
4. **Aplicação Sequencial dos Critérios de Inclusão e Exclusão da Equipe:**
   - **Período (Jan–Dez 2025):** `(df["ano"] == 2025) & (df["mes"].between(1, 12))` (reduz de `45.025` para `5.466` registros).
   - **Município Identificado e Somente Capitais:** Remove cidades vazias/não informadas e filtra `df["cidade_norm"].isin(CAPITAIS_SET_NORM)` (mantendo `Salvador`, `Recife`, `Rio de Janeiro` e `Belém`, reduzindo de `5.466` para `3.026` registros).
   - **Gênero Informado e Somente Mulheres Cis e Trans/Travestis:** Remove registros com gênero `'Não identificado'` e homens, aplicando `df[df["genero"].isin(["Mulher cis", "Mulher trans e travesti"])]` (reduzindo de `3.026` para `223` registros).
   - **Idade Informada e Faixa Etária de 18 a 45 Anos:** Converte `idade` para numérico, aplica `df.dropna(subset=["idade"])` (excluindo os `96` registros sem idade informada) e filtra `df[df["idade"].between(18, 45)]` (excluindo `50` registros fora da faixa etária), resultando na coorte final exata de **`77` mulheres**.
5. **Engenharia de Atributos Interseccionais (ODS 5 × ODS 18):**
   Cria as colunas derivadas `is_fatal` (`1` para `Morto`, `0` para `Ferido`), `faixa_etaria` segmentada (`Jovem Adulta (18-24)`, `Adulta (25-34)`, `Adulta (35-45)`), `macro_genero` (`Mulher Cis` vs. `Mulher Trans/Travesti`) e `grupo_interseccional` (`genero + " - " + raca`).

### 3.4. Construção do Esquema Estrela (`build_star_schema_tables`) e Carga em Lotes (`load.py`)
A função `build_star_schema_tables(df_sinesp, df_fc)` normaliza os dois DataFrames tratados em **4 Tabelas de Dimensão** e **2 Tabelas Fato**:
- `dim_tempo`: Calendário único com chave substituta inteira `id_tempo` no formato `YYYYMMDD`.
- `dim_localidade`: Cadastro unificado das Capitais do Brasil e bairros com chave `id_localidade`, enriquecido com `estado`, `uf` e `regiao`.
- `dim_perfil_vitima`: Combinações únicas de `genero`, `macro_genero`, `raca_cor`, `faixa_etaria`, `tipo_pessoa` e `grupo_interseccional` (`id_perfil`).
- `dim_contexto_crime`: Tipificações criminais, presença/ação policial, chacina, circunstância e local (`id_contexto`).
- `fato_violencia_sinesp` e `fato_vitimas_fogo_cruzado`: Tabelas fato contendo apenas chaves estrangeiras inteiras e métricas numéricas, carregadas no PostgreSQL Aiven via `df.to_sql(..., chunksize=5000, method="multi")`.

---

## 4. Resumo das Principais Decisões Técnicas e Justificativas

| Decisão Técnica Adotada | Problema Técnico Resolvido | Justificativa de Engenharia de Dados |
| :--- | :--- | :--- |
| **Validação Geográfica pelo Par `(UF, Capital)` em vez de apenas Nome da Cidade** | No Brasil existem municípios do interior com o mesmo nome de capitais de outros estados (ex: *Belém* em AL e PB; *Campo Grande* em AL e RN; *Palmas* no PR; *Rio Branco* no MT). | Filtrar apenas pelo nome da cidade incluiria municípios do interior por engano. A validação pelo dicionário `CAPITAIS_POR_UF[uf] == municipio` garante 100% de precisão geográfica nas 27 capitais. |
| **Agrupamento Mensal (`groupby().agg(sum)`) em vez de `drop_duplicates()` Simples no SINESP** | Na planilha original do SINESP, Brasília (DF) possui múltiplas linhas no mesmo mês/evento com valor `feminino = 1` referentes a diferentes regiões administrativas. | Um `drop_duplicates()` simples descartaria ocorrências reais idênticas. O `groupby` com soma consolida corretamente o total mensal de cada capital sem perder nenhuma vítima (`1.233` vítimas preservadas). |
| **Descarte de Linhas Zeradas e Pré-Filtragem no ETL antes da Carga no Banco** | A planilha bruta `fonte original.xlsx` possui `728.846` linhas zeradas (~310 MB em XML), o que consumiria espaço e I/O desnecessários no banco em nuvem. | Ao aplicar os critérios da equipe e descartar linhas zeradas na camada de transformação em memória, o volume enviado ao PostgreSQL da Aiven cai para menos de **5 MB** (< 0,5% do limite gratuito de 1 GB). |
| **Algoritmo Iterativo de Correção de Escala de Coordenadas GPS** | As coordenadas no arquivo bruto do Fogo Cruzado misturam valores decimais (`-12.95`) com inteiros fora de escala (`-129586607` e `-8054123`). | A divisão iterativa por 10 com limite territorial específico para latitude (`<= 35°`) e longitude (`<= 75°`) recupera automaticamente a posição geográfica correta antes de aplicar o truncamento de 2 casas da LGPD. |
| **Persistência Dual (PostgreSQL Aiven + Cache CSV em `/data/processed`)** | Instâncias gratuitas de banco em nuvem podem entrar em hibernação após inatividade, o que poderia causar falha de conexão no Dashboard publicado. | A persistência dual garante que o pipeline carregue o Esquema Estrela no PostgreSQL da Aiven e, simultaneamente, mantenha os CSVs tratados sincronizados no repositório para leitura instantânea (< 0,2s) e alta disponibilidade. |

---

## 5. Como Reproduzir e Auditar o Processo de Tratamento

```bash
# 1. Executar o pipeline completo de tratamento (Extract -> Transform -> Load)
python -m etl.pipeline

# 2. Executar apenas o processamento local das planilhas brutas sem conexão remota ao PostgreSQL
python -m etl.pipeline --skip-db --force-excel

# 3. Executar a suíte de testes unitários que valida todas as regras de transformação e LGPD
python -m unittest discover -s tests -p "test_*.py" -v
```
