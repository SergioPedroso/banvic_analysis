WITH colaborador_agencia AS (

    SELECT
        colaborador_id,
        agencia_id

    FROM {{ ref('stg_colaborador_agencia') }}
  
)

SELECT * FROM colaborador_agencia