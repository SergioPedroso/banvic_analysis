WITH contas AS (

    SELECT
        conta_id,
        cliente_id,
        agencia_id,
        colaborador_id,
        tipo_conta,
        data_abertura,
        {{ centavos_to_reais('saldo_total_1e4', 10000, 2) }} AS saldo_total,
        {{ centavos_to_reais('saldo_disponivel_1e4', 10000, 2) }} AS saldo_disponivel,
        data_ultimo_lancamento
    
    FROM {{ ref('stg_contas') }}
  
)

SELECT * FROM contas