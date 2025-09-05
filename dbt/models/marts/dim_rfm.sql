WITH agencias AS (

    SELECT
        agencia_id,
        nome,
        cidade,
        data_abertura
    
    FROM {{ ref('stg_agencias') }}
  
)

SELECT * FROM agencias