{{ config(
    materialized='incremental',
    unique_key='id',
    incremental_strategy='delete+insert',
) }}

WITH raw_data AS (
    SELECT 
        data::jsonb -> 'projects' -> 'nodes' AS nodes,
        _airbyte_raw_id,
        _airbyte_extracted_at,
        _airbyte_meta
    FROM 
        public.projects
)

SELECT 
    DISTINCT 
    md5(
        '{{ var("integration_id") }}' ||
        (node->>'id') ||
        'project' ||
        'linear'
    ) AS id,
    NOW() AS created,
    NOW() AS modified,
    '{{ var("timestamp") }}' AS sync_timestamp,
    (node->>'id') AS external_id,
    'linear' AS source,
    (node->>'name') AS name,
    group_table.id AS group_id,
    user_table.id AS owner_id,
    (node->>'description') AS description,
    COALESCE((node->>'startDate')::date, NULL) AS begin_date,
    COALESCE((node->>'targetDate')::date, NULL) AS end_date,
    (node->>'state') AS status,
    '{{ var("integration_id") }}'::uuid AS integration_id,
    _airbyte_raw_id AS last_raw_data
FROM 
    raw_data,
    jsonb_array_elements(nodes) AS node
LEFT JOIN {{ ref('project_management_projectmanagementgroup') }} AS group_table
    ON group_table.external_id = (node->>'teamId')
    AND group_table.integration_id = '{{ var("integration_id") }}'
LEFT JOIN {{ ref('project_management_projectmanagementuser') }} AS user_table
    ON user_table.external_id = (node->>'ownerId')
    AND user_table.integration_id = '{{ var("integration_id") }}'
WHERE 
    node->>'id' IS NOT NULL
