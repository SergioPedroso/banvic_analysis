WITH colaborador_agencia AS (

    SELECT
        colaborador_id,
        nome_colaborador,
        data_nascimento,
        agencia_id,
        nome_agencia,
        cidade,
        data_abertura
    
    FROM {{ ref('int_colaborador_agencia') }}
  
)

SELECT * FROM colaborador_agencia