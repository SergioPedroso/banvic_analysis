WITH source_deduplicate AS (

  {{ dbt_utils.deduplicate(
      relation=source('banvic','cotacao_USD'),
      partition_by="date(datetime(timestamp(data_e_hora_da_cotacao), 'America/Sao_Paulo'))",
      order_by="timestamp(data_e_hora_da_cotacao) desc"
  ) }}

),

cotacao AS (

  SELECT
    CAST(data_e_hora_da_cotacao AS date) AS data_cotacao,
    CAST(cotacao_compra AS numeric) AS cotacao_compra,
    CAST(cotacao_venda AS numeric) AS cotacao_venda

  FROM source_deduplicate

)

SELECT * FROM cotacao

