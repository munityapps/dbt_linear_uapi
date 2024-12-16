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
    (node->>'id') AS external_id,
    NOW() AS created,
    NOW() AS modified,
    '{{ var("timestamp", run_started_at) }}' AS sync_timestamp,
    md5(
      (node->>'id') ||
      'user' ||
      'linear'
    ) AS id,
    'linear' AS source,
    _airbyte_raw_id AS last_raw_data,
    (node->>'name') AS name,
    (node->>'email') AS email,
    NULL AS url,
    NULL AS status,
    NULL AS firstname,
    NULL AS lastname,
    NULL AS title,
    NULL AS roles,
    NULL AS company_name,
    NULL AS phone,
    NULL AS timezone,
    TRUE AS active,
    (node->>'avatarUrl') AS avatar_url,
    COALESCE((node->>'createdAt')::timestamp, CURRENT_TIMESTAMP) AS created_at,
    COALESCE((node->>'updatedAt')::timestamp, CURRENT_TIMESTAMP) AS updated_at,
    team_node->>'name' AS team_name
FROM 
    raw_data,
    jsonb_array_elements(nodes) AS node,
    jsonb_array_elements(node->'teams'->'nodes') AS team_node
