WITH source AS (

  SELECT * FROM {{ source('banvic', 'agencias') }}

),

agencias AS(

    SELECT     
        CAST(cod_agencia AS int) AS agencia_id,
        nome,
        cidade,
        data_abertura
        
    FROM source

)

SELECT * FROM agencias

