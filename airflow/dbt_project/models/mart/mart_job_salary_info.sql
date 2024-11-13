with stg_data as (
    select *
    from {{ ref('stg_job') }}
)

select 
    source,
    report_date,
    jobtitle as job_title,
    location_region,
    company_name,
    education,
    job_category,
    salary,
    experience,
    industry,
    tools,
    is_salary_negotiable,
    uuid

from stg_data
