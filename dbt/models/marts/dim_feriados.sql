WITH feriados AS (

    SELECT
        date_id,
        nome_feriado,
        tipo

    FROM {{ ref('int_feriados') }}

)

SELECT * FROM feriados