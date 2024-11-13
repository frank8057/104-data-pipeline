

with raw_data as (
    select *,
        GENERATE_UUID() as generated_uuid
    from `tir103-job-analysis`.`final_data`.`final_data`
    
    where report_date > (select max(report_date) from `tir103-job-analysis`.`final_data`.`stg_job`)
    
)

select 
    *,
    generated_uuid as uuid,
    case 
        when salary is null then true
        else false
    end as is_salary_negotiable
from raw_data