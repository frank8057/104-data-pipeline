
  
    

    create or replace table `tir103-job-analysis`.`final_data`.`mart_job_salary_info`
      
    
    

    OPTIONS()
    as (
      with stg_data as (
    select *
    from `tir103-job-analysis`.`final_data`.`stg_job`
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
    );
  