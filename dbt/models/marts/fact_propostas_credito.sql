WITH propostas AS (

    SELECT
        proposta_id,
        cliente_id,
        colaborador_id,
        data_entrada_proposta,
        taxa_juros_mensal,
        {{ centavos_to_reais('valor_proposta_1e4', 10000, 4) }} AS valor_proposta,
        {{ centavos_to_reais('valor_financiamento_1e4', 10000, 4) }} AS valor_financiamento,
        {{ centavos_to_reais('valor_entrada_1e4', 10000, 4) }} AS valor_entrada,
        {{ centavos_to_reais('valor_prestacao_1e4', 10000, 4) }} AS valor_prestacao,
        quantidade_parcelas,
        carencia,
        status_proposta
    
    FROM {{ ref('stg_propostas_credito') }}
  
)

SELECT * FROM propostas