WITH source AS (

    SELECT * FROM {{ source('banvic', 'colaboradores') }}

),

colaboradores as (

    SELECT
        CAST(cod_colaborador AS int) AS colaborador_id,
        CONCAT(primeiro_nome,' ',ultimo_nome) AS nome_colaborador,
        data_nascimento
        
    FROM source
    
)

SELECT * FROM colaboradores
