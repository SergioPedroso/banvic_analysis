WITH propostas AS (

    SELECT
        proposta_id,
        cliente_id,
        colaborador_id,
        data_entrada_proposta,
        taxa_juros_mensal,
        valor_proposta,
        valor_financiamento,
        valor_entrada,
        valor_prestacao,
        quantidade_parcelas,
        carencia,
        status_proposta
    
    FROM {{ ref('int_propostas_credito') }}
  
)

SELECT * FROM propostas