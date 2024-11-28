{{ config(
    materialized='incremental',
    unique_key='id',
    incremental_strategy='delete+insert',
) }}

WITH type_list AS (
    SELECT  
        NOW() as created,
        NOW() as modified,
        'linear' as source,
        '{}'::jsonb as last_raw_data, 
        false as is_sub_task,
        types.key as external_id,
        types.name as name,
        NULL as description,
        'project' as scope,
        NULL as url,
        NULL as icon
    FROM (
        (SELECT 'task' as key, 'Task' as name)
    ) as types
)

SELECT
    md5(
        project.id ||
        type_list.external_id ||
        'issuetypelinear'
    ) as id,
    type_list.*,
    '{{ var("timestamp", run_started_at) }}' as sync_timestamp,
    project.id as project_id
FROM {{ ref('linear_projects') }} AS project
CROSS JOIN type_list
