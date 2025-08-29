WITH source AS (

    SELECT * FROM {{ source('banvic', 'colaborador_agencia') }}

),

colaborador_agencia as (

    SELECT
        CAST(cod_colaborador AS int) AS colaborador_id,
        CAST(cod_agencia AS int) AS agencia_id
        
    FROM source
    
)

SELECT * FROM colaborador_agencia
