
  
    

    create or replace table `tir103-job-analysis`.`final_data`.`mart_salary_overview`
      
    
    

    OPTIONS()
    as (
      with company_salary as (
    select *
    from `tir103-job-analysis`.`final_data`.`company_salary_analysis`
),
industry_salary as (
    select *
    from `tir103-job-analysis`.`final_data`.`industry_salary_analysis`
)

select
    cs.company_name,
    cs.report_month,
    cs.report_date,
    cs.job_count as company_job_count,
    cs.avg_salary as company_avg_salary,
    cs.max_salary as company_max_salary,
    cs.min_salary as company_min_salary,
    ind.job_count as industry_job_count,
    ind.avg_salary as industry_avg_salary,
    ind.max_salary as industry_max_salary,
    ind.min_salary as industry_min_salary
from company_salary cs
left join industry_salary ind
on cs.report_month = ind.report_month and cs.company_name = ind.industry
order by cs.report_month, cs.company_name, cs.report_date
    );
  