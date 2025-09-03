WITH dates AS (

    SELECT

        date_id,
        date_day,
        date_start_month,
        month_name_short,
        month_year ,
        date_start_year,
        year_number,
        day_of_week_num,
        week_day

    FROM {{ ref('int_dates') }}

)

SELECT * FROM dates