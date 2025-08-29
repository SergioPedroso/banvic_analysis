WITH source AS (

  SELECT * FROM {{ source('banvic', 'clientes') }}

),

clientes AS(

    SELECT
        CAST(cod_cliente AS int) AS cliente_id,
        primeiro_nome,
        ultimo_nome,
        email,
        tipo_cliente,
        data_inclusao,
        data_nascimento
        
    FROM source

)

SELECT * FROM clientes