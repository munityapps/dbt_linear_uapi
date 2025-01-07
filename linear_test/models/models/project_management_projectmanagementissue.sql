{{ config(
    materialized='incremental',
    unique_key='id',
    incremental_strategy='delete+insert',
) }}

WITH raw_data AS (
    SELECT 
        data::jsonb -> 'issues' -> 'nodes' AS nodes,
        _airbyte_raw_id,
        _airbyte_extracted_at,
        _airbyte_meta
    FROM 
        public.issues
)

SELECT 
    DISTINCT 
    md5(
        '{{ var("integration_id") }}' ||
        (node->>'id') ||
        'issue' ||
        'linear'
    ) AS id,
    NOW() AS created,
    NOW() AS modified,
    '{{ var("timestamp") }}' AS sync_timestamp,
    (node->>'id') AS external_id,
    'linear' AS source,
    (node->>'title') AS name,
    (node->>'description') AS description,
    project_table.id AS project_id,
    user_table.id AS assignee_id,
    status_table.id AS status_id,
    type_table.id AS type_id,
    (node->>'priority')::integer AS priority,
    parent_issue.id AS parent_id,
    '{{ var("integration_id") }}'::uuid AS integration_id,
    _airbyte_raw_id AS last_raw_data
FROM 
    raw_data,
    jsonb_array_elements(nodes) AS node
LEFT JOIN {{ ref('project_management_projectmanagementproject') }} AS project_table
    ON project_table.external_id = (node->>'projectId')
    AND project_table.integration_id = '{{ var("integration_id") }}'
LEFT JOIN {{ ref('project_management_projectmanagementuser') }} AS user_table
    ON user_table.external_id = (node->>'assigneeId')
    AND user_table.integration_id = '{{ var("integration_id") }}'
LEFT JOIN {{ ref('project_management_projectmanagementissuestatus') }} AS status_table
    ON status_table.external_id = (node->>'stateId')
    AND status_table.integration_id = '{{ var("integration_id") }}'
LEFT JOIN {{ ref('project_management_projectmanagementissuetype') }} AS type_table
    ON type_table.external_id = (node->>'typeId')
    AND type_table.integration_id = '{{ var("integration_id") }}'
LEFT JOIN {{ ref('project_management_projectmanagementissue') }} AS parent_issue
    ON parent_issue.external_id = (node->>'parentId')
    AND parent_issue.integration_id = '{{ var("integration_id") }}'
WHERE 
    node->>'id' IS NOT NULL
