CREATE OR REPLACE EXTERNAL TABLE `case-grupo-otg1.case_bronze.players`
(
  player_id   STRING OPTIONS(DESCRIPTION="Identificador unico do jogador")
  ,email      STRING OPTIONS(DESCRIPTION="Email informado no cadastro do jogador")
  ,city       STRING OPTIONS(DESCRIPTION="Cidade informada no cadastro do jogador")
  ,created_at STRING OPTIONS(DESCRIPTION="Timestamp de criacao do cadastro do jogador")
)
WITH PARTITION COLUMNS (
  dt DATE
)
OPTIONS (
  FORMAT = 'PARQUET',
  URIS = ['gs://case-grupo-otg1/staging/players/dt=*/*.parquet'],
  HIVE_PARTITION_URI_PREFIX = 'gs://case-grupo-otg1/staging/players',
  REQUIRE_HIVE_PARTITION_FILTER = FALSE,
  DESCRIPTION = "Tabela bronze externa de jogadores em Parquet no Cloud Storage"
);
