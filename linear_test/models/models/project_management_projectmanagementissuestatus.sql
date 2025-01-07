{{ config(
    materialized='incremental',
    unique_key='id',
    incremental_strategy='delete+insert',
) }}

WITH status_list AS (
    SELECT  
        NOW() as created,
        NOW() as modified,
        'linear' as source,
        '{}'::jsonb as last_raw_data,
        false::boolean as default,
        status.color as color,
        status.order as order,
        status.key as external_id,
        status.name as name
    FROM (
        (SELECT 'done' as key, 'Done' as name, 0 as order, 'green' as color) UNION
        (SELECT 'in_progress' as key, 'In Progress' as name, 1 as order, 'blue' as color) UNION
        (SELECT 'todo' as key, 'To Do' as name, 2 as order, 'gray' as color)
    ) as status
)

SELECT
    md5(
        project.id ||
        status_list.external_id ||
        'issuestatuslinear'
    ) as id,
    status_list.name as meta_status,
    status_list.*,
    '{{ var("timestamp", run_started_at) }}' AS sync_timestamp,
    project.id as project_id
FROM {{ ref('project_management_projectmanagementproject') }} AS project
CROSS JOIN status_list
