WITH cotacao AS (

    SELECT
        date_id,
        cotacao_compra,
        cotacao_venda

    FROM {{ ref('int_cotacao_dolar') }}

)

SELECT * FROM cotacao
