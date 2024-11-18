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
    (node->>'id') AS external_id,
    NOW() AS created,
    NOW() AS modified,
    '{{ var("timestamp", run_started_at) }}' AS sync_timestamp,
    md5(
        (node->>'id') || 'project' || 'linear'
    ) AS id,
    'linear' AS source,
    (node->>'name') AS name,
    NULL AS folder,
    NULL AS url,
    (node->>'state') AS state,
    NULL AS private,
    (node->>'description') AS description,
    COALESCE((node->>'createdAt')::timestamp, CURRENT_TIMESTAMP) AS created_at,
    COALESCE((node->>'updatedAt')::timestamp, CURRENT_TIMESTAMP) AS updated_at,
    COALESCE((node->>'priority')::integer, 0) AS priority,
    NULL::date AS begin_date,
    NULL::date AS end_date,
    NULL AS owner_id,
    _airbyte_raw_id,
    _airbyte_extracted_at,
    _airbyte_meta
FROM 
    raw_data,
    jsonb_array_elements(nodes) AS node
WHERE 
    node->>'id' IS NOT NULL
