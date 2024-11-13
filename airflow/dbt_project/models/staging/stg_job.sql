{{
    config(
        materialized='incremental',
        unique_key=['company_name', 'report_date', 'job_category', 'tools']
    )
}}

with raw_data as (
    select *,
        GENERATE_UUID() as generated_uuid
    from {{ source('final_data_source', 'final_data') }}
    {% if is_incremental() %}
    where report_date > (select max(report_date) from {{ this }})
    {% endif %}
)

select 
    *,
    generated_uuid as uuid,
    case 
        when salary is null then true
        else false
    end as is_salary_negotiable
from raw_data
