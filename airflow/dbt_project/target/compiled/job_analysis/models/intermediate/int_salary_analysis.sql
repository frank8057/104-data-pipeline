WITH stg_jobs AS (
    SELECT *
    FROM `tir103-job-analysis`.`project_104_project_104`.`stg_104_jobs`
)

SELECT 
    location_region,
    job_category,
    COUNT(*) as job_count,
    AVG(salary) as avg_salary,
    MIN(salary) as min_salary,
    MAX(salary) as max_salary,
    COUNT(DISTINCT company_name) as company_count
FROM stg_jobs
WHERE salary > 0
GROUP BY location_region, job_category