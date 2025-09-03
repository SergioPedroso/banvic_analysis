WITH transacoes AS (

    SELECT
        transacao_id,
        conta_id,
        date_id,
        tipo_transacao,
        valor_transacao
    
    FROM {{ ref('int_transacoes') }}
  
)

SELECT * FROM transacoes