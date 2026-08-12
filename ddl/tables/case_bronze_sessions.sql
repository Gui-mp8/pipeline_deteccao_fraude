CREATE OR REPLACE EXTERNAL TABLE `case-grupo-otg1.case_bronze.sessions`
(
  session_id  STRING OPTIONS(DESCRIPTION="Identificador unico da sessao"),
  player_id   STRING OPTIONS(DESCRIPTION="Identificador do jogador associado a sessao"),
  ip          STRING OPTIONS(DESCRIPTION="Endereco IP utilizado na sessao"),
  device      STRING OPTIONS(DESCRIPTION="Dispositivo utilizado na sessao"),
  timestamp   STRING OPTIONS(DESCRIPTION="Timestamp do evento de sessao")
)
WITH PARTITION COLUMNS (
  dt DATE
)
OPTIONS (
  FORMAT = 'PARQUET',
  URIS = ['gs://case-grupo-otg1/staging/sessions/*'],
  HIVE_PARTITION_URI_PREFIX = 'gs://case-grupo-otg1/staging/sessions',
  REQUIRE_HIVE_PARTITION_FILTER = TRUE,
  DESCRIPTION = "Tabela bronze externa de sessoes em Parquet no Cloud Storage"
);
