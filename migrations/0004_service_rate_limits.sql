UPDATE flow_service_instances
SET desired_spec = jsonb_set(
    desired_spec,
    '{rate_limit}',
    '{"requests_per_second":20,"burst":40}'::jsonb,
    true
)
WHERE jsonb_typeof(desired_spec) = 'object'
  AND NOT desired_spec ? 'rate_limit';

UPDATE flow_reconcile_operations
SET spec = jsonb_set(
    spec,
    '{rate_limit}',
    '{"requests_per_second":20,"burst":40}'::jsonb,
    true
)
WHERE jsonb_typeof(spec) = 'object'
  AND NOT spec ? 'rate_limit';

UPDATE flow_provider_token_receipts
SET spec = jsonb_set(
    spec,
    '{rate_limit}',
    '{"requests_per_second":20,"burst":40}'::jsonb,
    true
)
WHERE jsonb_typeof(spec) = 'object'
  AND NOT spec ? 'rate_limit';
