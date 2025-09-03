WITH transacoes AS (

    SELECT
        transacao_id,
        conta_id,
        REPLACE(CAST(data_transacao AS string), '-', '') AS date_id,
        tipo_transacao,
        {{ centavos_to_reais('valor_transacao_1e4', 10000, 4) }} AS valor_transacao
    
    FROM {{ ref('stg_transacoes') }}
  
)

SELECT * FROM transacoes