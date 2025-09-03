WITH transacoes AS (

    SELECT
        transacao_id,
        conta_id,
        date_id,
        tipo_transacao,
        valor_transacao
    
    FROM {{ ref('int_transacoes') }}
  
),

contas AS (

    SELECT
        conta_id,
        cliente_id,
        agencia_id,
        colaborador_id,
        tipo_conta,
        data_abertura,
        saldo_total,
        saldo_disponivel,
        data_ultimo_lancamento
    
    FROM {{ ref('int_contas') }}
  
),

clientes AS (

    SELECT
        cliente_id,
        nome_cliente,
        email,
        tipo_cliente,
        data_inclusao,
        data_nascimento
    
    FROM {{ ref('stg_clientes') }}
  
),

colaborador_agencia AS (

    SELECT
        colaborador_id,
        nome_colaborador,
        data_nascimento,
        agencia_id,
        nome_agencia,
        cidade,
        data_abertura
    
    FROM {{ ref('int_colaborador_agencia') }}
  
),

dates AS (

    SELECT

        date_id,
        date_day,
        month_name_short,
        month_year,
        day_of_week_num,
        week_day

    FROM {{ ref('int_dates') }}

),

merged AS (

    SELECT
        transacoes.transacao_id,
        transacoes.conta_id,
        contas.cliente_id,
        contas.agencia_id,
        contas.colaborador_id,
        transacoes.date_id,
        dates.date_day,
        dates.month_name_short,
        dates.month_year,
        dates.day_of_week_num,
        dates.week_day,
        transacoes.tipo_transacao,
        transacoes.valor_transacao,
        contas.tipo_conta,
        contas.data_abertura AS conta_data_abertura,
        contas.saldo_total,
        contas.saldo_disponivel,
        contas.data_ultimo_lancamento,
        clientes.nome_cliente,
        clientes.email,
        clientes.data_inclusao AS cliente_data_inclusao,
        clientes.data_nascimento AS cliente_data_nascimento,
        colaborador_agencia.nome_colaborador,
        colaborador_agencia.data_nascimento AS colaborador_data_nascimento,
        colaborador_agencia.nome_agencia,
        colaborador_agencia.cidade AS agencia_cidade,
        colaborador_agencia.data_abertura AS agencia_data_abertura

    FROM transacoes
    LEFT JOIN contas ON transacoes.conta_id = contas.conta_id
    LEFT JOIN clientes ON contas.cliente_id = clientes.cliente_id
    LEFT JOIN colaborador_agencia ON contas.colaborador_id = colaborador_agencia.colaborador_id 
                                  AND contas.agencia_id = colaborador_agencia.agencia_id
    LEFT JOIN dates ON transacoes.date_id = dates.date_id
)

SELECT * FROM merged