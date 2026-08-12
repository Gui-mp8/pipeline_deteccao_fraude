# Pipeline de Deteccao de Fraude

Projeto desenvolvido para o case tecnico de Engenharia de Dados, Observabilidade e Deteccao de Fraudes. A solucao ingere arquivos CSV/JSON, converte para Parquet, disponibiliza tabelas Bronze no BigQuery, transforma os dados com dbt em Silver/Gold e orquestra tudo com Airflow/Astronomer.

## Sumario

- [Arquitetura](#arquitetura)
- [Stack](#stack)
- [Estrutura do Projeto](#estrutura-do-projeto)
- [Configuracao GCP e GitHub Actions](#configuracao-gcp-e-github-actions)
- [Free Tier da GCP](#free-tier-da-gcp)
- [Subir o Airflow Local com Astronomer](#subir-o-airflow-local-com-astronomer)
- [Rodar dbt Local](#rodar-dbt-local)
- [Rodar o Job Python Local](#rodar-o-job-python-local)
- [BigQuery DDL](#bigquery-ddl)
- [Orquestracao Airflow](#orquestracao-airflow)
- [Modelagem dbt](#modelagem-dbt)
- [Definicao de Cargas](#definicao-de-cargas)
- [Observabilidade](#observabilidade)
- [Sinais de Fraude](#sinais-de-fraude)
- [Dashboard Power BI](#dashboard-power-bi)
- [Respostas ao Desafio](#respostas-ao-desafio)
- [Avaliacao Tecnica da Entrega](#avaliacao-tecnica-da-entrega)
- [Descricao dos Dados](#descricao-dos-dados)
- [Validacao das Fraudes Identificadas](#validacao-das-fraudes-identificadas)
- [Roteiro para Entrevista](#roteiro-para-entrevista)
- [Troubleshooting](#troubleshooting)
- [Validacao Local](#validacao-local)

## Arquitetura

![Arquitetura da pipeline](image.png)

Fluxo implementado:

```text
GCS Landing CSV/JSON
  -> Cloud Run Job Python
  -> GCS Staging Parquet particionado
  -> BigQuery Bronze external tables
  -> dbt Silver
  -> dbt Gold
  -> Power BI
```

Camadas:

| Camada | Responsabilidade |
|---|---|
| Landing | Arquivos brutos em `gs://case-grupo-otg1/landing`. |
| Staging | Arquivos Parquet particionados em `gs://case-grupo-otg1/staging`. |
| Bronze | Tabelas externas BigQuery sobre os Parquets, declaradas como `source()` no dbt. |
| Silver | Limpeza, casting, normalizacao, deduplicacao, incremental e testes de qualidade. |
| Gold | Marts analiticos para fraude, afiliados e sinais financeiros. |

## Stack

| Componente | Uso |
|---|---|
| Airflow 3 / Astronomer | Orquestracao das DAGs por camada e por tabela. |
| Cloud Run Jobs | Execucao do conversor Python e do dbt em containers. |
| Cloud Storage | Landing de arquivos brutos e staging em Parquet. |
| BigQuery | Bronze, Silver e Gold. |
| dbt | Transformacao, testes e documentacao logica dos modelos. |
| GitHub Actions | CI/CD para DDL, dbt e jobs Python. |
| Workload Identity Federation | Autenticacao GitHub Actions -> GCP sem chave JSON. |

## Estrutura do Projeto

```text
airflow/                    # Projeto Astro Airflow
airflow/dags/case           # Factories de DAGs bronze, silver e gold
airflow/include             # Config, Assets e TaskGroups reutilizaveis
dbt/                        # Projeto dbt BigQuery
dbt/models/bronze/case      # Sources bronze e testes simples
dbt/models/silver/case      # Modelos Silver
dbt/models/gold/case        # Modelos Gold
ddl/datasets                # YAMLs de datasets BigQuery
ddl/tables                  # DDLs das tabelas externas Bronze
jobs/case/parquet_converter # Cloud Run Job Python CSV/JSON -> Parquet
.github/workflows           # CI/CD
tests/jobs                  # Testes unitarios do job Python
```

## Configuracao GCP e GitHub Actions

Este projeto usa Workload Identity Federation para permitir que o GitHub Actions acesse a GCP sem armazenar chave JSON.

Valores usados:

```bash
PROJECT_ID="case-grupo-otg1"
PROJECT_NUMBER="712304487497"
REGION="us-central1"
POOL_ID="github-pool"
PROVIDER_ID="github-provider"
SA_EMAIL="case-594@case-grupo-otg1.iam.gserviceaccount.com"
GITHUB_ORG="Gui-mp8"
GITHUB_REPO="pipeline_deteccao_fraude"
ARTIFACT_REGISTRY_REPOSITORY="gar-imagens"
```

### 1. Definir variaveis no Cloud Shell

```bash
export PROJECT_ID="case-grupo-otg1"
export PROJECT_NUMBER="$(gcloud projects describe $PROJECT_ID --format='value(projectNumber)')"
export REGION="us-central1"
export POOL_ID="github-pool"
export PROVIDER_ID="github-provider"
export SA_EMAIL="case-594@case-grupo-otg1.iam.gserviceaccount.com"
export GITHUB_ORG="Gui-mp8"
export GITHUB_REPO="pipeline_deteccao_fraude"

gcloud config set project $PROJECT_ID
```

### 2. Habilitar APIs

```bash
gcloud services enable \
  iamcredentials.googleapis.com \
  cloudresourcemanager.googleapis.com \
  iam.googleapis.com \
  serviceusage.googleapis.com \
  artifactregistry.googleapis.com \
  cloudbuild.googleapis.com \
  run.googleapis.com \
  bigquery.googleapis.com \
  storage.googleapis.com \
  --project=$PROJECT_ID
```

### 3. Criar Workload Identity Pool

```bash
gcloud iam workload-identity-pools create $POOL_ID \
  --project=$PROJECT_ID \
  --location="global" \
  --display-name="GitHub Actions Pool"
```

Se retornar `ALREADY_EXISTS`, o pool ja existe.

### 4. Criar Provider OIDC do GitHub

```bash
gcloud iam workload-identity-pools providers create-oidc $PROVIDER_ID \
  --project=$PROJECT_ID \
  --location="global" \
  --workload-identity-pool=$POOL_ID \
  --display-name="GitHub Provider" \
  --attribute-mapping="google.subject=assertion.sub,attribute.actor=assertion.actor,attribute.repository=assertion.repository,attribute.repository_owner=assertion.repository_owner" \
  --attribute-condition="assertion.repository == '${GITHUB_ORG}/${GITHUB_REPO}'" \
  --issuer-uri="https://token.actions.githubusercontent.com"
```

Validar:

```bash
gcloud iam workload-identity-pools providers describe $PROVIDER_ID \
  --project=$PROJECT_ID \
  --location="global" \
  --workload-identity-pool=$POOL_ID
```

O provider usado nos workflows deve ser:

```text
projects/712304487497/locations/global/workloadIdentityPools/github-pool/providers/github-provider
```

### 5. Permitir impersonacao da Service Account

```bash
gcloud iam service-accounts add-iam-policy-binding "$SA_EMAIL" \
  --project=$PROJECT_ID \
  --role="roles/iam.workloadIdentityUser" \
  --member="principalSet://iam.googleapis.com/projects/$PROJECT_NUMBER/locations/global/workloadIdentityPools/$POOL_ID/attribute.repository/$GITHUB_ORG/$GITHUB_REPO"
```

### 6. Permissoes da Service Account

Permissoes minimas usadas no case:

```bash
gcloud projects add-iam-policy-binding $PROJECT_ID \
  --member="serviceAccount:$SA_EMAIL" \
  --role="roles/viewer"

gcloud projects add-iam-policy-binding $PROJECT_ID \
  --member="serviceAccount:$SA_EMAIL" \
  --role="roles/run.admin"

gcloud projects add-iam-policy-binding $PROJECT_ID \
  --member="serviceAccount:$SA_EMAIL" \
  --role="roles/bigquery.admin"

gcloud projects add-iam-policy-binding $PROJECT_ID \
  --member="serviceAccount:$SA_EMAIL" \
  --role="roles/storage.admin"

gcloud iam service-accounts add-iam-policy-binding "$SA_EMAIL" \
  --project=$PROJECT_ID \
  --member="serviceAccount:$SA_EMAIL" \
  --role="roles/iam.serviceAccountUser"
```

Para Artifact Registry:

```bash
gcloud artifacts repositories create gar-imagens \
  --repository-format=docker \
  --location=$REGION \
  --description="Imagens Docker do case de fraude" \
  --project=$PROJECT_ID

gcloud artifacts repositories add-iam-policy-binding gar-imagens \
  --location=$REGION \
  --project=$PROJECT_ID \
  --member="serviceAccount:$SA_EMAIL" \
  --role="roles/artifactregistry.writer"
```

### 7. Configurar GitHub

Em `Settings -> Secrets and variables -> Actions`, configure:

Secrets ou variables:

```text
GCP_WORKLOAD_IDENTITY_PROVIDER=projects/712304487497/locations/global/workloadIdentityPools/github-pool/providers/github-provider
GCP_GITHUB_ACTIONS_SERVICE_ACCOUNT=case-594@case-grupo-otg1.iam.gserviceaccount.com
```

Repository variables opcionais:

```text
GCP_PROJECT_ID=case-grupo-otg1
GCP_REGION=us-central1
DBT_ARTIFACT_REGISTRY_REPOSITORY=gar-imagens
JOBS_ARTIFACT_REGISTRY_REPOSITORY=gar-imagens
DBT_IMAGE_NAME=fraud-dbt
DBT_CLOUD_RUN_JOB=fraud-dbt
DBT_RUNTIME_SERVICE_ACCOUNT=case-594@case-grupo-otg1.iam.gserviceaccount.com
```

Permissao obrigatoria nos workflows:

```yaml
permissions:
  contents: read
  id-token: write
```

### 8. Workflows disponiveis

| Workflow | Quando roda | O que faz |
|---|---|---|
| `deploy-bigquery-ddl.yml` | `push` na `main` com alteracao em `ddl/**` ou manual | Cria datasets e aplica DDLs BigQuery. |
| `deploy-dbt-cloud-run.yml` | `push` na `main` com alteracao em `dbt/**`, Dockerfile ou entrypoint | Builda imagem dbt, roda `dbt parse`, publica no Artifact Registry e deploya Cloud Run Job `fraud-dbt`. |
| `deploy-jobs-cloud-run.yml` | `push` na `main` com alteracao em `jobs/**` ou manual | Roda pytest, builda jobs Python alterados e deploya Cloud Run Jobs. |

## Free Tier da GCP

O projeto foi desenhado para ficar leve e reduzir custo em ambiente de teste. Ainda assim, os limites gratuitos podem mudar com o tempo, entao antes de executar em uma conta real e importante conferir a pagina oficial de precos da GCP.

Servicos usados e cuidados de custo:

| Servico | Uso no projeto | Estrategia para baixo custo |
|---|---|---|
| Cloud Run Jobs | Executa o conversor Python e o dbt em containers sob demanda. | Jobs so rodam quando chamados pela DAG/CI, com `512Mi`, `1 CPU`, `1 task` e timeout controlado. |
| Artifact Registry | Armazena imagens Docker dos jobs e do dbt. | Usa um unico repositorio `gar-imagens`; imagens pequenas e tags por commit. |
| Cloud Storage | Guarda landing CSV/JSON e staging Parquet. | Arquivos pequenos, Parquet comprimido com Snappy e limpeza do prefixo staging antes de nova carga. |
| BigQuery | Bronze externa, Silver e Gold. | Bronze como external table evita carga fisica inicial; Silver incremental reduz reprocessamento; queries Bronze usam filtro de particao. |
| GitHub Actions | CI/CD fora da GCP. | Executa apenas em alteracoes de caminho especifico e workflow manual quando necessario. |
| Airflow local | Orquestracao em ambiente local Astronomer. | Roda localmente via Docker, sem custo GCP para scheduler/webserver. |

Boas praticas aplicadas:

- Usar Cloud Run Jobs em vez de servicos sempre ligados.
- Ler arquivos linha a linha e gravar Parquet em batches para reduzir memoria.
- Manter `BATCH_SIZE` configuravel.
- Usar tabelas externas Bronze sobre Parquet particionado.
- Exigir filtro de particao Hive na Bronze.
- Usar incremental na Silver para evitar processar todo o historico.
- Limpar a staging da tabela antes de escrever nova carga para evitar duplicidade e crescimento desnecessario.
- Separar workflows por escopo: DDL, dbt e jobs Python.

## Subir o Airflow Local com Astronomer

Instale a Astro CLI e Docker. Dentro da pasta do projeto:

```bash
cd airflow
astro dev start
```

Servicos locais:

```text
Airflow UI: http://localhost:8080
Usuario: admin
Senha: admin
Postgres: localhost:5432/postgres
```

Comandos uteis:

```bash
astro dev ps
astro dev logs --webserver
astro dev logs --scheduler
astro dev restart
astro dev stop
```

Se a porta `8080` ou `5432` ja estiver ocupada, pare o container conflitante ou ajuste a porta do Astro.

### Conexao GCP no Airflow local

O `conn_id` esperado pelas DAGs e:

```text
google_cloud_default
```

Na UI do Airflow:

```text
Admin -> Connections -> Add/Edit
Connection Id: google_cloud_default
Connection Type: Google Cloud
Project Id: case-grupo-otg1
Keyfile JSON: conteudo do JSON da Service Account, se estiver usando chave local
```

Tambem e possivel usar ADC local:

```bash
gcloud auth application-default login
gcloud config set project case-grupo-otg1
```

Para testar a conexao, use a DAG `test_gcp_connection`.

## Rodar dbt Local

Instale dependencias do dbt em um ambiente Python:

```bash
python -m venv .venv
source .venv/bin/activate
pip install dbt-bigquery dbt-core
```

Rodar do diretorio raiz:

```bash
dbt deps --project-dir dbt --profiles-dir dbt
dbt parse --project-dir dbt --profiles-dir dbt --target dev
dbt build --project-dir dbt --profiles-dir dbt --target dev --select slv_players
dbt build --project-dir dbt --profiles-dir dbt --target dev --select tag:silver
dbt build --project-dir dbt --profiles-dir dbt --target dev --select tag:gold
```

Para reconstruir uma incremental com estado antigo:

```bash
dbt build --project-dir dbt --profiles-dir dbt --target dev --select slv_players --full-refresh
```

O perfil local esta em `dbt/profiles.yml` e usa:

```text
project: case-grupo-otg1
dataset: case_silver
location: us-central1
```

## Rodar o Job Python Local

Instale dependencias:

```bash
python -m venv .venv
source .venv/bin/activate
pip install -r requirements-dev.txt
```

Exemplo usando GCS:

```bash
TABLE_NAME=transactions \
SOURCE_URI=gs://case-grupo-otg1/landing/transactions.csv \
DESTINATION_URI=gs://case-grupo-otg1/staging/transactions \
BATCH_SIZE=1000 \
PYTHONPATH=jobs/case/parquet_converter \
python jobs/case/parquet_converter/main.py
```

No Cloud Run, a DAG passa a tabela e o job assume os caminhos:

```text
Landing: gs://case-grupo-otg1/landing/<arquivo>
Staging: gs://case-grupo-otg1/staging/<tabela>
```

O repository limpa o prefixo da tabela na staging antes de salvar os novos Parquets, tornando a carga idempotente para evitar duplicidade em tabelas externas.

## BigQuery DDL

Datasets:

```text
case_bronze
case_silver
case_gold
```

Arquivos:

```text
ddl/datasets/*.yaml
ddl/tables/*.sql
```

A Bronze usa external tables sobre Parquet com Hive partition e `REQUIRE_HIVE_PARTITION_FILTER = TRUE`.

Para aplicar manualmente:

```bash
bq --project_id=case-grupo-otg1 query --use_legacy_sql=false < ddl/tables/case_bronze_players.sql
```

## Orquestracao Airflow

As DAGs sao geradas por tabela/modelo:

| Camada | Padrao | Responsabilidade |
|---|---|---|
| Bronze | `case_bronze_<tabela>` | Executa Cloud Run Job Python para converter landing em Parquet. |
| Silver | `case_silver_<tabela>` | Primeiro roda checks da source bronze, depois executa `dbt build --select slv_<tabela>`. |
| Gold | `case_gold_<modelo>` | Executa `dbt build --select gold_<modelo>` apos Assets Silver necessarios. |

Dependencias usam Airflow Assets:

```text
Bronze Asset -> Silver DAG -> Silver Asset -> Gold DAG
```

TaskGroups reutilizaveis:

| TaskGroup | Uso |
|---|---|
| `TaskStrategyLandingToBronzeCloudRunTG` | Invoca Cloud Run Job Python da Bronze. |
| `TaskStrategyDbtChecksCloudRunTG` | Executa testes dbt antes da transformacao. |
| `TaskStrategyDbtCloudRunTG` | Executa dbt no Cloud Run Job. |

## Modelagem dbt

| Camada | Objetos |
|---|---|
| Bronze | Sources `case_bronze.players`, `sessions`, `transactions`, `affiliate_cpa_ftd`. |
| Silver | `slv_players`, `slv_sessions`, `slv_transactions`, `slv_affiliate_cpa_ftd`. |
| Gold | `gold_fraud_overview`, `gold_affiliate_metrics`, `gold_financial_signals`. |

Silver:

| Modelo | Grain | Incremental |
|---|---|---|
| `slv_players` | 1 linha por `player_id` | `created_at` |
| `slv_sessions` | 1 linha por `session_id` | `timestamp` |
| `slv_transactions` | 1 linha por `transaction_id` | `timestamp` |
| `slv_affiliate_cpa_ftd` | afiliado + player + pais | tabela agregada/full tecnico |

Testes:

| Camada | Testes |
|---|---|
| Bronze/source | `not_null` em IDs e particoes. |
| Silver | `not_null`, `unique`, `relationships`, `accepted_values`. |
| Gold | Unicidade, combinacao unica e testes singulares de metricas/flags. |

## Definicao de Cargas

| Dataset | Frequencia | Tipo | Campo de controle | Justificativa |
|---|---|---|---|---|
| `players.json` | Diaria | Incremental | `created_at` | Cadastro muda menos; carga diaria reduz custo e atende analises cadastrais. |
| `sessions.json` | Horaria | Incremental | `timestamp` | Sessoes sao importantes para fraude comportamental, IP e device. |
| `transactions.csv` | Horaria | Incremental | `timestamp` | Movimentacoes financeiras precisam de atualizacao frequente. |
| `affiliate_cpa_ftd.csv` | Diaria | Full tecnico na staging e agregacao na Silver | `ingest_date` | A fonte nao possui data de evento; a particao tecnica controla reprocessamento. |

## Observabilidade

Pontos de observabilidade:

| Area | O que observar |
|---|---|
| Airflow | Status das DAGs, duracao, retries, falhas por camada, eventos de Assets. |
| Cloud Run Jobs | Exit code, memoria, CPU, tempo de execucao, logs estruturados. |
| BigQuery | Jobs executados, bytes processados, erros de particao, volume de linhas. |
| dbt | Falhas de testes, `run_results.json`, modelos quebrados, lineage e docs. |
| Dados | Contagem de linhas por camada, unicidade na Silver, nao nulos, relacionamentos. |

Queries uteis:

```sql
SELECT COUNT(*) AS total_linhas
FROM `case-grupo-otg1.case_bronze.players`
WHERE dt >= DATE('1900-01-01');
```

```sql
SELECT player_id, COUNT(*) AS qtd
FROM `case-grupo-otg1.case_silver.slv_players`
GROUP BY player_id
HAVING COUNT(*) > 1;
```

## Sinais de Fraude

A camada Gold permite detectar pelo menos estes sinais:

| Sinal | Regra |
|---|---|
| IP compartilhado | Muitos players usando o mesmo IP. |
| Muitos devices | Mesmo player acessando por muitos dispositivos. |
| Saque elevado | `withdraw_amount` muito maior que `deposit_amount`. |
| Aposta elevada | `bet_amount` muito alto em relacao ao deposito. |
| Funil de afiliado anomalo | `registrations > clicks` ou `ftd > registrations`. |

Modelos Gold:

| Modelo | Uso |
|---|---|
| `gold_fraud_overview` | Visao por player com flags e score de risco. |
| `gold_affiliate_metrics` | Performance de afiliados, conversao, FTD e custo CPA. |
| `gold_financial_signals` | Indicadores financeiros por player. |

## Dashboard Power BI

Paginas sugeridas:

| Pagina | Indicadores |
|---|---|
| Fraud Overview | Players por score de risco, flags ativas, IP compartilhado, devices por player, cidade. |
| Affiliate Metrics | Clicks, registrations, FTD, taxa de conversao, CPA estimado, anomalias de funil. |
| Financial Signals | Depositos, saques, apostas, razao saque/deposito, razao aposta/deposito. |

Conexao recomendada: Power BI conectado diretamente nas tabelas `case_gold`.

## Respostas ao Desafio

| Item pedido | Resposta no projeto |
|---|---|
| Arquitetura Medallion | Landing, Staging, Bronze, Silver e Gold descritas neste README e implementadas no repo. |
| Airflow | DAGs por camada/tabela em `airflow/dags/case`, com Assets e TaskGroups. |
| dbt | Sources Bronze, modelos Silver e Gold organizados por pasta `case`. |
| BigQuery | Datasets e DDLs versionados em `ddl/`. |
| Definicao de cargas | Tabela de frequencia, tipo e justificativa na secao "Definicao de Cargas". |
| Observabilidade | Airflow, Cloud Run, BigQuery, dbt e qualidade de dados descritos na secao propria. |
| Fraude | Gold possui sinais de IP compartilhado, multiplos devices, saque/aposta anomala e funil de afiliado. |
| Dashboard | Estrutura recomendada para Power BI usando as tabelas Gold. |
| CI/CD | GitHub Actions com Workload Identity Federation para DDL, dbt e jobs Python. |

## Avaliacao Tecnica da Entrega

Conclusao da revisao: a entrega cobre os requisitos centrais do desafio.

| Area avaliada | Status | Evidencia no projeto |
|---|---|---|
| Arquitetura | Atendido | README com desenho medallion, `image.png`, GCS Landing/Staging, BigQuery Bronze/Silver/Gold. |
| Ingestao | Atendido | Job Python em `jobs/case/parquet_converter` converte CSV/JSON em Parquet particionado. |
| Orquestracao | Atendido | Factories de DAGs em `airflow/dags/case` e TaskGroups reutilizaveis em `airflow/include/task_groups`. |
| BigQuery | Atendido | Datasets em `ddl/datasets` e DDLs de external tables Bronze em `ddl/tables`. |
| dbt Bronze/Silver/Gold | Atendido | Sources Bronze, modelos Silver incrementais e modelos Gold analiticos. |
| Incremental loads | Atendido com ressalva | `players`, `sessions` e `transactions` usam incremental por data/timestamp; `affiliate_cpa_ftd` usa full tecnico por nao ter data de evento na fonte. |
| Qualidade de dados | Atendido | Testes de `not_null`, `unique`, `relationships`, `accepted_values` e testes singulares para metricas/flags. |
| Observabilidade | Atendido | Secao dedicada a Airflow, Cloud Run, BigQuery, dbt e qualidade de dados. |
| Fraude | Atendido | Gold traz mais de 2 sinais: financeiro, funil de afiliado, IP compartilhado e multi-device. |
| Dashboard | Atendido como especificacao | README descreve paginas e indicadores esperados; as tabelas Gold estao prontas para consumo no Power BI. |

Pontos fortes:

- A separacao medallion esta clara e coerente com o case.
- O job Python e configuravel por tabela e valida colunas obrigatorias antes de gravar Parquet.
- A Silver faz normalizacao, casting, deduplicacao e relacionamento entre entidades.
- A Gold entrega tabelas diretamente alinhadas aos tres temas pedidos: Fraud Overview, Affiliate Metrics e Financial Signals.
- As descricoes dbt de Silver e Gold estao habilitadas para persistir no BigQuery com `persist_docs`.

Pontos de atencao para falar com transparencia:

- A Bronze foi desenhada como external table sobre Parquet; isso e economico e pratico para o case, mas em producao pode ser avaliado carregar para tabelas nativas se houver maior necessidade de performance.
- `affiliate_cpa_ftd` nao possui timestamp de evento, entao a estrategia mais honesta e carga diaria com full tecnico/idempotente ou particionamento por `ingest_date`.
- Os limiares de fraude sao regras heuristicas para triagem. Em producao, eles devem ser calibrados com historico, taxa de falso positivo e feedback do time de risco.

## Descricao dos Dados

Os dados representam quatro visoes complementares de uma operacao de iGaming: cadastro, comportamento de acesso, movimentacao financeira e aquisicao por afiliados.

| Dataset | Grao da fonte | Papel analitico | Campos principais | Transformacao principal |
|---|---|---|---|---|
| `players.json` | Um registro por jogador | Dimensao cadastral para enriquecer risco por cidade, data de cadastro e dominio de email. | `player_id`, `email`, `city`, `created_at` | Normaliza email, extrai dominio, padroniza cidade e deduplica por `player_id`. |
| `sessions.json` | Um registro por sessao | Base comportamental para investigar IP compartilhado, recencia e diversidade de devices. | `session_id`, `player_id`, `ip`, `device`, `timestamp` | Converte timestamp, normaliza device, cria `session_date` e deduplica por `session_id`. |
| `transactions.csv` | Um registro por transacao | Base financeira para medir deposito, saque, aposta e padroes anormais. | `transaction_id`, `player_id`, `type`, `amount`, `timestamp` | Normaliza tipo, converte valor para NUMERIC, separa medidas de deposito/saque/aposta e deduplica por `transaction_id`. |
| `affiliate_cpa_ftd.csv` | Afiliado, player e pais | Base de performance e fraude de aquisicao por CPA/FTD. | `affiliate_id`, `player_id`, `country`, `clicks`, `registrations`, `ftd`, `cpa_value` | Agrega por afiliado-player-pais, calcula custo CPA estimado, taxas de conversao e flags de funil impossivel. |

Tabelas Silver recomendadas para BigQuery:

| Tabela | Descricao curta para catalogo |
|---|---|
| `case_silver.slv_players` | Jogadores tratados e deduplicados, com email normalizado, dominio de email, cidade padronizada e data de cadastro. |
| `case_silver.slv_sessions` | Sessoes tratadas e deduplicadas, com IP, device normalizado e timestamp de acesso para analise comportamental. |
| `case_silver.slv_transactions` | Transacoes financeiras tratadas, com valores numericos e colunas separadas para depositos, saques e apostas. |
| `case_silver.slv_affiliate_cpa_ftd` | Atribuicao de afiliados consolidada por afiliado, player e pais, com funil CPA/FTD e flags de inconsistencia. |

Tabelas Gold recomendadas para BigQuery:

| Tabela | Descricao curta para catalogo |
|---|---|
| `case_gold.gold_fraud_overview` | Visao consolidada por player para triagem de fraude, combinando sinais comportamentais, financeiros e de afiliado. |
| `case_gold.gold_affiliate_metrics` | Mart de performance por afiliado e pais, com clicks, cadastros, FTD, CPA estimado e anomalias de funil. |
| `case_gold.gold_financial_signals` | Mart financeiro por player, com volumes de deposito, saque, aposta, ratios e flags de comportamento anomalo. |

## Validacao das Fraudes Identificadas

O desafio pede que a Gold permita sugerir pelo menos 2 sinais de fraude. Esta solucao implementa 5 sinais:

| Sinal | Onde fica | Regra | Interpretacao |
|---|---|---|---|
| Saque elevado | `gold_financial_signals.has_high_withdraw_signal` | `total_withdraw_amount > total_deposit_amount * 1.5` e `total_withdraw_amount >= 500` | Player saca muito mais do que deposita. Pode indicar abuso, comportamento financeiro atipico ou necessidade de investigacao. |
| Aposta desproporcional | `gold_financial_signals.has_high_bet_velocity_signal` | `total_bet_amount >= total_deposit_amount * 5` e `total_bet_amount >= 1000` | Player gira/aposta muito mais do que depositou. Pode indicar uso intenso de credito, bonus, alavancagem ou comportamento fora da curva. |
| Funil de afiliado anomalo | `slv_affiliate_cpa_ftd` e `gold_fraud_overview.has_affiliate_funnel_anomaly` | `registrations > clicks` ou `ftd > registrations` | O funil fica logicamente impossivel: nao deveria haver mais cadastros que clicks, nem mais FTDs que cadastros. |
| IP compartilhado | `gold_fraud_overview.has_shared_ip_signal` | Algum IP do player aparece para pelo menos 5 players distintos | Pode indicar multi-conta, trafego coordenado, uso de rede compartilhada ou automacao. |
| Muitos devices | `gold_fraud_overview.has_many_devices_signal` | Player usa pelo menos 4 tipos de device distintos | Pode indicar compartilhamento de conta, tentativa de mascarar origem ou comportamento incomum. |

Validacao feita sobre os arquivos locais do case com a mesma logica dos modelos:

| Medida | Resultado observado |
|---|---:|
| Players na base | 600 |
| Linhas de transacao | 1.800 |
| Linhas de afiliado | 2.000 |
| Linhas com `registrations > clicks` | 139 |
| Linhas com `ftd > registrations` | 344 |
| Linhas com alguma anomalia de funil | 480 |
| Players com sinal de saque elevado | 228 |
| Players com sinal de aposta desproporcional | 135 |
| Players com sinal de funil de afiliado | 325 |
| Players com pelo menos 1 sinal | 465 |
| Players com pelo menos 2 sinais | 176 |

Observacao importante: na amostra local, os sinais de IP compartilhado e muitos devices nao dispararam com os limiares atuais. Mesmo assim, eles estao modelados e documentados para funcionar quando o dado apresentar esse comportamento. Os dois sinais minimos pedidos pelo desafio foram atendidos e validados por dados: financeiro e afiliado.

## Roteiro para Entrevista

Use este roteiro para explicar o projeto de forma simples e segura.

1. Comece pelo problema: "O desafio era pegar dados heterogeneos de iGaming, tratar em uma arquitetura medallion e disponibilizar analises de risco, afiliados e comportamento financeiro."

2. Explique a arquitetura: "Eu criei uma camada Landing para arquivos brutos, uma Staging em Parquet particionado, uma Bronze no BigQuery como external table, uma Silver limpa/deduplicada/incremental e uma Gold pronta para dashboard."

3. Explique a ingestao: "O job Python recebe a tabela por variavel de ambiente, le CSV ou JSON, valida colunas obrigatorias, transforma em batches Parquet e grava no GCS. Isso deixa a carga idempotente e barata para o case."

4. Explique o dbt: "Na Silver eu tratei tipos, padronizei campos, removi duplicidades e criei testes. Na Gold eu modelei as perguntas de negocio: overview de fraude por player, metricas de afiliado por pais e sinais financeiros por player."

5. Explique incrementalidade: "Para players uso `created_at`, para sessions e transactions uso `timestamp`, porque sao datas naturais do evento. Para affiliate CPA/FTD, como a fonte nao tem data de evento, usei carga diaria com controle tecnico por ingestao."

6. Explique fraude sem prometer decisao final: "Eu nao classifico automaticamente alguem como fraudador. Eu gero sinais de risco para priorizar investigacao. Isso reduz falso positivo e deixa a decisao final para o time de risco."

7. Explique os sinais financeiros: "Criei dois sinais: saque muito maior que deposito e aposta muito maior que deposito. A ideia e capturar players com movimento financeiro desproporcional ao dinheiro que entrou."

8. Explique o sinal de afiliado: "No funil de aquisicao, clicks vem antes de cadastro, e cadastro vem antes de FTD. Quando aparece `registrations > clicks` ou `ftd > registrations`, existe uma inconsistencia logica que pode indicar erro de tracking, inflacao de numeros ou fraude de afiliado."

9. Explique os sinais comportamentais: "Tambem modelei IP compartilhado e muitos devices por player. Na amostra esses sinais nao dispararam, mas eles sao importantes em producao para detectar multi-conta e comportamento coordenado."

10. Feche com o dashboard: "As tabelas Gold ja estao no formato de consumo: `gold_fraud_overview` para ranking de players suspeitos, `gold_affiliate_metrics` para performance e anomalias de afiliados, e `gold_financial_signals` para ratios e volumes financeiros."

Frase curta para defender os limiares:

```text
Os thresholds sao heuristicas iniciais para o case. Eu os deixei simples e explicaveis, porque em um ambiente real eu calibraria esses cortes com historico, investigacoes confirmadas, custo de falso positivo e feedback do time de risco.
```

Frase curta sobre resultado:

```text
Com a base fornecida, a solucao identifica mais do que os dois sinais minimos pedidos: existem sinais financeiros e de afiliado materializados na Gold, e tambem sinais comportamentais modelados para producao.
```

## Troubleshooting

Erro de provider deletado:

```text
invalid_target ... pool or provider is disabled or deleted
```

Recrie o provider OIDC e confira se o `state` nao esta `DELETED`.

Erro `iam.serviceaccounts.actAs`:

```text
Permission 'iam.serviceaccounts.actAs' denied
```

Conceda:

```bash
gcloud iam service-accounts add-iam-policy-binding "$SA_EMAIL" \
  --project=$PROJECT_ID \
  --member="serviceAccount:$SA_EMAIL" \
  --role="roles/iam.serviceAccountUser"
```

Erro de particao na Bronze:

```text
Cannot query over table ... without a filter over column(s) 'dt'
```

As queries sobre Bronze precisam filtrar a particao Hive:

```sql
WHERE dt >= DATE('1900-01-01')
```

Erro de unique na Silver:

```text
Got N results, configured to fail if != 0
```

O teste dbt retorna as linhas invalidas. Para teste passar, deve retornar zero. Se a tabela incremental ficou com estado antigo, rode:

```bash
dbt build --project-dir dbt --profiles-dir dbt --target dev --select slv_players --full-refresh
```

## Validacao Local

```bash
python -m py_compile \
  airflow/dags/case/dag_factory_silver.py \
  jobs/case/parquet_converter/main.py \
  jobs/case/parquet_converter/src/repository/gcs_repository.py

dbt parse --project-dir dbt --profiles-dir dbt --target dev --quiet
```

Testes Python:

```bash
pip install -r requirements-dev.txt
pytest -q
```
