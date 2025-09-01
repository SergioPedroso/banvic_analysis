WITH source AS (

    SELECT * FROM {{ source('banvic', 'transacoes') }}

),

transacoes as (

    SELECT
        CAST(cod_transacao AS int) AS transacao_id,
        CAST(num_conta AS int) AS conta_id,
        data_transacao,
        nome_transacao AS tipo_transacao,
        {{ reais_to_centavos('valor_transacao', 10000) }} AS valor_transacao_1e4

    FROM source
    
)

SELECT * FROM transacoes
