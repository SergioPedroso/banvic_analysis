WITH source AS (

    SELECT * FROM {{ source('banvic', 'propostas_credito') }}

),

propostas_credito as (

    SELECT
        CAST(cod_proposta AS int) AS proposta_id,
        CAST(cod_cliente AS int) AS cliente_id,
        CAST(cod_colaborador AS int) AS colaborador_id,
        data_entrada_proposta,
        taxa_juros_mensal,
        {{ to_centavos('valor_proposta', 4) }} AS valor_proposta_1e4,
        {{ to_centavos('valor_financiamento', 4) }} AS valor_financiamento_1e4,
        {{ to_centavos('valor_entrada', 4) }} AS valor_entrada_1e4,
        {{ to_centavos('valor_prestacao', 4) }} AS valor_prestacao_1e4,
        quantidade_parcelas,
        carencia,
        status_proposta
        
    FROM source
    
)

SELECT * FROM propostas_credito
