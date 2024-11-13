WITH source AS (
    SELECT *
    FROM `tir103-job-analysis`.`project_104`.`job_104`
)

SELECT 
    CAST(report_date AS DATE) as report_date,
    job_title,
    company_name,
    main_category,
    sub_category,
    job_category,
    CAST(salary AS INTEGER) as salary,
    location_region,
    experience,
    industry,
    job_url,
    job_skills,
    tools,
    source
FROM source