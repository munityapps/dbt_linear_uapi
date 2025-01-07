{{ config(
    materialized='incremental',
    unique_key='id',
    incremental_strategy='delete+insert',
) }}

WITH raw_data AS (
    SELECT 
        data::jsonb -> 'issueStatuses' -> 'nodes' AS nodes,
        _airbyte_raw_id,
        _airbyte_extracted_at,
        _airbyte_meta
    FROM 
        public.issuestatuses
)

SELECT 
    DISTINCT 
    md5(
        '{{ var("integration_id") }}' ||
        (node->>'id') || 
        'issuestatus' ||
        'linear' ||
        '{{ var("integration_id") }}'
    ) AS id,
    (node->>'id') AS external_id,
    'linear' AS source,
    NOW() AS created,
    NOW() AS modified,
    '{{ var("timestamp", run_started_at) }}' AS sync_timestamp,
    _airbyte_raw_id AS last_raw_data,
    (node->>'name') AS name,
    (node->>'color') AS color,
    (node->>'order')::integer AS order,
    '{{ var("integration_id") }}'::uuid AS integration_id
FROM 
    raw_data,
    jsonb_array_elements(nodes) AS node
WHERE 
    node->>'id' IS NOT NULL
