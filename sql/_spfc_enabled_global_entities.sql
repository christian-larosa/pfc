CREATE OR REPLACE TABLE `{{ params.project_id }}.{{ params.dataset.cl }}._spfc_enabled_global_entities` AS
SELECT global_entity AS global_entity_id
FROM UNNEST({{ params.param_global_entity_id }}) AS global_entity
