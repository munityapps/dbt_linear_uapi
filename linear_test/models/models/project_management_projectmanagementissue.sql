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
    WHERE data IS NOT NULL
)

SELECT 
    DISTINCT 
    md5((node->>'id') || 'issue' || 'linear') AS id,
    (node->>'id') AS external_id,
    NOW() AS created,
    NOW() AS modified,
    '{{ var("timestamp", run_started_at) }}' AS sync_timestamp,
    'linear' AS source,
    _airbyte_raw_id AS last_raw_data,
    NULL AS url,
    COALESCE((node->>'priority')::integer, NULL) AS priority,
    NULL AS severity,
    (node->>'title') AS title,
    (node->>'description') AS description,
    COALESCE((node->>'createdAt')::timestamp, CURRENT_TIMESTAMP) AS created_at,
    COALESCE((node->>'updatedAt')::timestamp, CURRENT_TIMESTAMP) AS updated_at,
    NULL AS due_date,
    (node->'state'->>'name') = 'completed' AS complete,
    COALESCE(
        ARRAY(
            SELECT label->>'name'
            FROM jsonb_array_elements(node->'labels'->'nodes') AS label
        ), 
        '{}'
    ) AS tags,
    NULL AS group_id,
    (node->'assignee'->>'id') AS assignee_id,
    (node->'assignee'->>'name') AS assignee_name,
    NULL AS creator_id,
    NULL AS project_id,
    NULL AS project_name,
    (node->'state'->>'name') AS status_name,
    (node->'state'->>'color') AS state_color,
    NULL AS type_id,
    FALSE AS is_milestone
FROM 
    raw_data,
    jsonb_array_elements(nodes) AS node
WHERE 
    node->>'id' IS NOT NULL
