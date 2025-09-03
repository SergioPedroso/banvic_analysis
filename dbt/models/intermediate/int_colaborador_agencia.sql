WITH colaborador AS (

    SELECT
        colaborador_id,
        nome_colaborador,
        data_nascimento
    
    FROM {{ ref('stg_colaboradores') }}
  
),

agencias AS (

    SELECT
        agencia_id,
        nome AS nome_agencia,
        cidade,
        data_abertura
    
    FROM {{ ref('stg_agencias') }}
  
),

bridge AS (

    SELECT
        colaborador_id,
        agencia_id

    FROM {{ ref('stg_colaborador_agencia') }}
  
),

merged AS (

    SELECT
        colaborador.colaborador_id,
        colaborador.nome_colaborador,
        colaborador.data_nascimento,
        agencias.agencia_id,
        agencias.nome_agencia,
        agencias.cidade,
        agencias.data_abertura

    FROM colaborador
    LEFT JOIN bridge ON colaborador.colaborador_id = bridge.colaborador_id
    LEFT JOIN agencias ON bridge.agencia_id = agencias.agencia_id

)

SELECT * FROM merged
