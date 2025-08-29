WITH agencias AS (
  SELECT * FROM {{ source('banvic', 'agencias') }}
)

SELECT     
   
    CAST(cod_agencia AS int) AS agencia_id,
    nome,
    cidade,
    data_abertura,
    tipo_agencia
        
FROM agencias