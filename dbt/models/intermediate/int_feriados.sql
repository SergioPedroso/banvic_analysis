WITH feriados AS (

    SELECT
        REPLACE(CAST(data_feriado AS string), '-', '') AS date_id,
        nome_feriado,
        tipo
    
    FROM {{ ref('stg_feriados') }}
  
)

SELECT * FROM feriados

