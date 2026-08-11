CREATE OR REPLACE EXTERNAL TABLE `case-grupo-otg1.case_bronze.affiliate_cpa_ftd`
(
  affiliate_id   STRING OPTIONS(DESCRIPTION="Identificador do afiliado")
  ,player_id     STRING OPTIONS(DESCRIPTION="Identificador do jogador vinculado ao afiliado")
  ,country       STRING OPTIONS(DESCRIPTION="Pais do jogador")
  ,clicks        STRING OPTIONS(DESCRIPTION="Quantidade de cliques gerados pelo afiliado")
  ,registrations STRING OPTIONS(DESCRIPTION="Quantidade de registros gerados pelo afiliado")
  ,ftd           STRING OPTIONS(DESCRIPTION="Quantidade de primeiros depositos")
  ,cpa_value     STRING OPTIONS(DESCRIPTION="Valor de CPA associado ao afiliado")
)
WITH PARTITION COLUMNS (
  ingest_date DATE
)
OPTIONS (
  FORMAT = 'PARQUET',
  URIS = ['gs://case-grupo-otg1/staging/affiliate_cpa_ftd/*'],
  HIVE_PARTITION_URI_PREFIX = 'gs://case-grupo-otg1/staging/affiliate_cpa_ftd',
  REQUIRE_HIVE_PARTITION_FILTER = TRUE,
  DESCRIPTION = "Tabela bronze externa de afiliados CPA/FTD em Parquet no Cloud Storage"
);
