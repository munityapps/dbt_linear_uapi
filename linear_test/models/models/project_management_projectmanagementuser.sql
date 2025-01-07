{{ config(
    materialized='incremental',
    unique_key='id',
    incremental_strategy='delete+insert',
) }}

WITH raw_data AS (
    SELECT 
        data::jsonb -> 'users' -> 'nodes' AS nodes,
        _airbyte_raw_id,
        _airbyte_extracted_at,
        _airbyte_meta
    FROM 
        public.users
)

SELECT 
    DISTINCT 
    md5(
        '{{ var("integration_id") }}' ||
        (node->>'id') ||
        'user' ||
        'linear'
    ) AS id,
    NOW() AS created,
    NOW() AS modified,
    '{{ var("timestamp") }}' AS sync_timestamp,
    (node->>'id') AS external_id,
    'linear' AS source,
    (node->>'name') AS name,
    (node->>'email') AS email,
    group_table.id AS group_id,
    (node->>'avatarUrl') AS avatar,
    NULL AS firstname,
    NULL AS lastname,
    NULL AS title,
    NULL AS timezone,
    '{{ var("integration_id") }}'::uuid AS integration_id,
    _airbyte_raw_id AS last_raw_data
FROM 
    raw_data,
    jsonb_array_elements(nodes) AS node
LEFT JOIN {{ ref('project_management_projectmanagementgroup') }} AS group_table
    ON group_table.external_id = ANY (
        SELECT jsonb_array_elements_text(node->'teams'->'nodes'->'id')
    )
    AND group_table.integration_id = '{{ var("integration_id") }}'
WHERE 
    node->>'id' IS NOT NULL
