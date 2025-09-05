WITH cotacao AS (

    SELECT
        REPLACE(CAST(data_cotacao AS string), '-', '') AS date_id,
        cotacao_compra,
        cotacao_venda
        
    
    FROM {{ ref('stg_cotacao_dolar') }}
  
)

SELECT * FROM cotacao

