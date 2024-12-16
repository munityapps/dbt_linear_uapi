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
        (node->>'id')::text ||
        'linear'::text
    ) AS id,
    (node->>'id') AS external_id,
    'linear' AS source,
    NOW() AS created,
    NOW() AS modified,
    (SELECT _airbyte_raw_id FROM public.teams LIMIT 1) AS last_raw_data,
    (node->>'name') AS name,
    COALESCE((node->>'createdAt')::timestamp, CURRENT_TIMESTAMP) AS created_at,
    COALESCE((node->>'updatedAt')::timestamp, CURRENT_TIMESTAMP) AS updated_at,
    (node->>'description') AS description,
    0::boolean AS deleted,
    0::boolean AS archived,
    '{{ var("timestamp", run_started_at) }}' AS sync_timestamp
FROM 
    raw_data,
    jsonb_array_elements(nodes) AS node
WHERE 
    node->>'id' IS NOT NULL
