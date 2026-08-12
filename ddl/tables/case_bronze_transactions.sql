CREATE OR REPLACE EXTERNAL TABLE `case-grupo-otg1.case_bronze.transactions`
(
  transaction_id  STRING OPTIONS(DESCRIPTION="Identificador unico da transacao"),
  player_id       STRING OPTIONS(DESCRIPTION="Identificador do jogador associado a transacao"),
  type            STRING OPTIONS(DESCRIPTION="Tipo da transacao"),
  amount          STRING OPTIONS(DESCRIPTION="Valor monetario da transacao no formato bruto"),
  timestamp       STRING OPTIONS(DESCRIPTION="Timestamp do evento de transacao")
)
WITH PARTITION COLUMNS (
  dt DATE
)
OPTIONS (
  FORMAT = 'PARQUET',
  URIS = ['gs://case-grupo-otg1/staging/transactions/*'],
  HIVE_PARTITION_URI_PREFIX = 'gs://case-grupo-otg1/staging/transactions',
  REQUIRE_HIVE_PARTITION_FILTER = TRUE,
  DESCRIPTION = "Tabela bronze externa de transacoes em Parquet no Cloud Storage"
);
