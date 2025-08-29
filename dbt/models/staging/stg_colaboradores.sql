WITH source AS (

    SELECT * FROM {{ source('banvic', 'colaboradores') }}

),

colaboradores as (

    SELECT
        CAST(cod_colaborador AS int) AS colaborador_id,
        primeiro_nome,
        ultimo_nome,
        data_nascimento
        
    FROM source
    
)

SELECT * FROM colaboradores
