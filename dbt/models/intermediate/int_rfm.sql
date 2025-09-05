WITH clientes AS (

    SELECT
        cliente_id, 
        nome_cliente

    FROM {{ ref('stg_clientes') }}
),

contas AS (

    SELECT
        conta_id,
        cliente_id,
        agencia_id

    FROM {{ ref('stg_contas')}}
),

agencias AS (

    SELECT
        agencia_id,
        nome AS nome_agencia

    FROM {{ ref('stg_agencias')}}
),


transacoes AS (
    SELECT
        transacoes.transacao_id,              
        contas.cliente_id, 
        transacoes.conta_id,
        CAST(transacoes.data_transacao AS DATE) AS data_transacao,  
        ABS({{ centavos_to_reais('valor_transacao_1e4', 10000, 4) }}) AS valor_transacao 

    FROM {{ ref('stg_transacoes') }} transacoes
    LEFT JOIN contas USING (conta_id)
),

transacoes_agg AS (

    SELECT
        transacao_id,
        cliente_id,
        data_transacao,
        SUM(valor_transacao) AS valor_transacao

    FROM transacoes
    GROUP BY transacao_id, cliente_id, data_transacao

),

max_date AS (

    SELECT 
        MAX(data_transacao) AS snapshot_date

    FROM transacoes_agg

),


cliente_agencia AS (

    SELECT DISTINCT
        contas.cliente_id,
        agencias.nome_agencia

    FROM contas 
    LEFT JOIN agencias USING (agencia_id)
),

rfm_base AS (

    SELECT
        transacoes_agg.cliente_id,
        DATE_DIFF((SELECT snapshot_date FROM max_date), MAX(transacoes_agg.data_transacao), DAY) AS recency_days,
        COUNT(DISTINCT transacoes_agg.transacao_id) AS frequency_orders,
        COALESCE(SUM(transacoes_agg.valor_transacao), 0) AS monetary_value

    FROM transacoes_agg
    GROUP BY transacoes_agg.cliente_id
),

rfm_scores AS (

    SELECT
        rb.cliente_id,
        cli.nome_cliente,
        ca.nome_agencia,
        rb.recency_days,
        rb.frequency_orders,
        ROUND(rb.monetary_value, 2) AS monetary_value,
        6 - NTILE(5) OVER (ORDER BY rb.recency_days ASC) AS r_score, -- Recency invertido, mais recente = 5
        NTILE(5) OVER (ORDER BY rb.frequency_orders ASC) AS f_score,
        NTILE(5) OVER (ORDER BY rb.monetary_value ASC) AS m_score

    FROM rfm_base rb
    LEFT JOIN clientes cli USING (cliente_id)
    LEFT JOIN cliente_agencia ca USING (cliente_id)
),

rfm AS (

    SELECT
        cliente_id,
        nome_cliente,
        nome_agencia,
        recency_days,
        frequency_orders,
        monetary_value,
        r_score,
        f_score,
        m_score,
        {# CASE
            WHEN r_score >= 3 AND f_score >= 4 AND m_score >= 4 THEN 'Campeões'
            WHEN r_score <= 2 AND f_score >= 4 AND m_score >= 4 THEN 'Não posso perdê-los'
            WHEN r_score <= 2 AND f_score BETWEEN 2 AND 3 AND m_score BETWEEN 2 AND 3 THEN 'Em risco'
            WHEN r_score >= 4 AND f_score BETWEEN 3 AND 4 AND m_score BETWEEN 3 AND 4 THEN 'Leais'
            WHEN r_score = 5 AND f_score <= 2 AND m_score <= 2 THEN 'Promissores'
            WHEN r_score BETWEEN 3 AND 4 AND f_score BETWEEN 2 AND 3 AND m_score BETWEEN 2 AND 3 THEN 'Precisam de Atenção'
            WHEN r_score <= 2 AND f_score <= 2 AND m_score <= 2 THEN 'Perdidos'
        ELSE 'Outros' #}

        CASE
            WHEN r_score >= 4 AND f_score >= 4 AND m_score >= 4 THEN 'Campeões'
            WHEN r_score >= 3 AND f_score >= 3 AND m_score >= 3 THEN 'Leais'
            WHEN r_score = 5 AND f_score <= 2 AND m_score <= 2 THEN 'Promissores'
            WHEN r_score <= 2 AND f_score >= 4 AND m_score >= 4 THEN 'Não posso perdê-los'
            WHEN r_score <= 2 AND f_score BETWEEN 2 AND 3 AND m_score BETWEEN 2 AND 3 THEN 'Em risco'
            WHEN r_score BETWEEN 3 AND 4 AND f_score BETWEEN 2 AND 3 AND m_score BETWEEN 2 AND 3 THEN 'Precisam de Atenção'
            WHEN r_score <= 2 AND f_score <= 2 AND m_score <= 2 THEN 'Perdidos'
        ELSE 'Regulares'
        END AS rfm_segment

    FROM rfm_scores
)

SELECT * FROM rfm
