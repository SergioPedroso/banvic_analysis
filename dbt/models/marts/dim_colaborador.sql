WITH colaborador AS (

    SELECT
        colaborador_id,
        nome_colaborador,
        data_nascimento
    
    FROM {{ ref('stg_colaboradores') }}
  
)

SELECT * FROM colaborador