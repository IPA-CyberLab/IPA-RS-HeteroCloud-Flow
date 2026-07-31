ALTER TABLE audit_events
    ADD COLUMN principal_context_id UUID;

CREATE INDEX audit_events_principal_context_idx
    ON audit_events (service_instance_id, principal_context_id)
    WHERE principal_context_id IS NOT NULL;
