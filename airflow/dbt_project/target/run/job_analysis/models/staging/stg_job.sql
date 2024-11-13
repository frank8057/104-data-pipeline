-- back compat for old kwarg name
  
  
        
            
                
                
            
                
                
            
                
                
            
                
                
            
        
    

    

    merge into `tir103-job-analysis`.`final_data`.`stg_job` as DBT_INTERNAL_DEST
        using (

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
        ) as DBT_INTERNAL_SOURCE
        on (
                    DBT_INTERNAL_SOURCE.company_name = DBT_INTERNAL_DEST.company_name
                ) and (
                    DBT_INTERNAL_SOURCE.report_date = DBT_INTERNAL_DEST.report_date
                ) and (
                    DBT_INTERNAL_SOURCE.job_category = DBT_INTERNAL_DEST.job_category
                ) and (
                    DBT_INTERNAL_SOURCE.tools = DBT_INTERNAL_DEST.tools
                )

    
    when matched then update set
        `source` = DBT_INTERNAL_SOURCE.`source`,`report_date` = DBT_INTERNAL_SOURCE.`report_date`,`jobtitle` = DBT_INTERNAL_SOURCE.`jobtitle`,`company_name` = DBT_INTERNAL_SOURCE.`company_name`,`education` = DBT_INTERNAL_SOURCE.`education`,`job_category` = DBT_INTERNAL_SOURCE.`job_category`,`salary` = DBT_INTERNAL_SOURCE.`salary`,`location_region` = DBT_INTERNAL_SOURCE.`location_region`,`experience` = DBT_INTERNAL_SOURCE.`experience`,`industry` = DBT_INTERNAL_SOURCE.`industry`,`tools` = DBT_INTERNAL_SOURCE.`tools`,`generated_uuid` = DBT_INTERNAL_SOURCE.`generated_uuid`,`uuid` = DBT_INTERNAL_SOURCE.`uuid`,`is_salary_negotiable` = DBT_INTERNAL_SOURCE.`is_salary_negotiable`
    

    when not matched then insert
        (`source`, `report_date`, `jobtitle`, `company_name`, `education`, `job_category`, `salary`, `location_region`, `experience`, `industry`, `tools`, `generated_uuid`, `uuid`, `is_salary_negotiable`)
    values
        (`source`, `report_date`, `jobtitle`, `company_name`, `education`, `job_category`, `salary`, `location_region`, `experience`, `industry`, `tools`, `generated_uuid`, `uuid`, `is_salary_negotiable`)


    