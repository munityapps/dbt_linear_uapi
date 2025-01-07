{{ config(
    materialized='incremental',
    unique_key='id',
    incremental_strategy='delete+insert',
) }}

WITH raw_data AS (
    SELECT 
        data::jsonb -> 'teams' -> 'nodes' AS nodes,
        _airbyte_raw_id,
        _airbyte_extracted_at,
        _airbyte_meta
    FROM 
        public.teams
)

SELECT 
    DISTINCT 
    md5(
        '{{ var("integration_id") }}' ||
        (node->>'id') ||
        'group' ||
        'linear'
    ) AS id,
    NOW() AS created,
    NOW() AS modified,
    '{{ var("timestamp") }}' AS sync_timestamp,
    (node->>'id') AS external_id,
    'linear' AS source,
    (node->>'name') AS name,
    (node->>'description') AS description,
    '{{ var("integration_id") }}'::uuid AS integration_id,
    _airbyte_raw_id AS last_raw_data
FROM 
    raw_data,
    jsonb_array_elements(nodes) AS node
WHERE 
    node->>'id' IS NOT NULL
