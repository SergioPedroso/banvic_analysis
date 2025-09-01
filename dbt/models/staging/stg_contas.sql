WITH source AS (

    SELECT * FROM {{ source('banvic', 'contas') }}

),

contas as (

    SELECT
        CAST(num_conta AS int) AS conta_id,
        CAST(cod_cliente AS int) AS cliente_id,
        CAST(cod_agencia AS int) AS agencia_id,
        CAST(cod_colaborador AS int) AS colaborador_id,
        tipo_conta,
        data_abertura,
        {{ reais_to_centavos('saldo_total', 10000) }} AS saldo_total_1e4,
        {{ reais_to_centavos('saldo_disponivel', 10000) }} AS saldo_disponivel_1e4,
        data_ultimo_lancamento
        
    FROM source
    
)

SELECT * FROM contas
