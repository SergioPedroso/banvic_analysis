WITH rfm AS (

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
        rfm_segment

    FROM rfm_scores
    
    FROM {{ ref('int_rfm') }}
  
)

SELECT * FROM rfm