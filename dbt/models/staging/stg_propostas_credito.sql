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
        valor_proposta,
        valor_financiamento,
        valor_entrada,
        valor_prestacao,
        quantidade_parcelas,
        carencia,
        status_proposta
        
    FROM source
    
)

SELECT * FROM propostas_credito
