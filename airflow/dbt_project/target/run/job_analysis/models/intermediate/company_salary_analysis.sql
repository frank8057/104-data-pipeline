
  
    

    create or replace table `tir103-job-analysis`.`final_data`.`company_salary_analysis`
      
    
    

    OPTIONS()
    as (
      with stg_jobs as (
    select *
    from `tir103-job-analysis`.`final_data`.`stg_job`
)

select
    company_name,
    date_trunc(report_date, month) as report_month,
    report_date,
    count(jobtitle) as job_count,
    avg(case when is_salary_negotiable = false then salary end) as avg_salary,
    max(case when is_salary_negotiable = false then salary end) as max_salary,
    min(case when is_salary_negotiable = false then salary end) as min_salary
from stg_jobs
group by company_name, report_month, report_date
order by company_name, report_month, report_date
    );
  