with stg_jobs as (
    select *
    from `tir103-job-analysis`.`final_data`.`stg_job`
)

select
    date_trunc(report_date, month) as report_month,
    report_date,
    count(jobtitle) as total_job_count,
    sum(case when is_salary_negotiable = false then 1 else 0 end) as salaried_job_count,
    sum(case when is_salary_negotiable = true then 1 else 0 end) as negotiable_job_count
from stg_jobs
group by report_month, report_date
order by report_month, report_date