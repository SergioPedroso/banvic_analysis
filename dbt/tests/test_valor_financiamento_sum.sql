-- Singular test: ensures that valor_financiamento equals to valor_entrada + valor_proposta

SELECT
    valor_proposta_1e4,
    valor_financiamento_1e4,
    valor_entrada_1e4

FROM {{ ref('stg_propostas_credito') }}
WHERE valor_financiamento_1e4 <> valor_entrada_1e4 + valor_proposta_1e4
