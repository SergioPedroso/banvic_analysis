WITH source AS (

    SELECT * FROM {{ source('banvic', 'feriados_pontes_nacionais') }}

),

feriados AS (

    SELECT
        REPLACE(CAST(date AS string), '-', '') AS date_id,
        CAST(date AS date) AS data_feriado,
        name AS nome_feriado,
        type AS tipo

    FROM source

)

SELECT * FROM feriados
