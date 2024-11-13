with stg_data as (
    select *
    from {{ ref('stg_job') }}
    where location_region LIKE '台北%'  -- 只保留台北地區的資料
)

select
    -- 手動列出所有欄位，排除 location_region
    source,
    report_date,
    jobtitle as job_title,
    company_name,
    education,
    job_category,
    salary,
    experience,
    industry,
    tools,
    is_salary_negotiable,
    uuid,
    
    -- location_region_name: 組合「縣市名稱」加上「市」或「縣」
    CASE
        WHEN location_region LIKE '%市%' THEN CONCAT(REGEXP_EXTRACT(location_region, r'^(.*)市'), '市')
        WHEN location_region LIKE '%縣%' THEN CONCAT(REGEXP_EXTRACT(location_region, r'^(.*)縣'), '縣')
        ELSE location_region
    END AS location_region_name,

    -- location_district: 組合「區」名稱加上「區」
    CASE
        WHEN location_region LIKE '%市%' THEN CONCAT(REGEXP_EXTRACT(location_region, r'市(.*)區'), '區')
        WHEN location_region LIKE '%縣%' THEN CONCAT(REGEXP_EXTRACT(location_region, r'縣(.*)區'), '區')
        ELSE NULL
    END AS location_district,

    -- -- is_taipei_region: 標記是否為台北地區
    -- CASE
    --     WHEN location_region LIKE '台北%' THEN true
    --     ELSE false
    -- END AS is_taipei_region

from stg_data
