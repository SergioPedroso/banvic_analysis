WITH clientes AS (

    SELECT
        cliente_id,
        nome_cliente,
        email,
        tipo_cliente,
        data_inclusao,
        data_nascimento
    
    FROM {{ ref('stg_clientes') }}
  
)

SELECT * FROM clientes