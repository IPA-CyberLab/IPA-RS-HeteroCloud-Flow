CREATE TABLE flow_principal_context_revocations (
    context_id UUID PRIMARY KEY,
    organization_id UUID NOT NULL,
    project_id UUID NOT NULL,
    service_instance_id UUID NOT NULL,
    expires_at TIMESTAMPTZ NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    FOREIGN KEY (service_instance_id, organization_id, project_id)
        REFERENCES flow_service_instances (id, organization_id, project_id)
        ON DELETE CASCADE
);

CREATE INDEX flow_principal_context_revocations_scope_idx
    ON flow_principal_context_revocations (
        service_instance_id,
        organization_id,
        project_id
    );

CREATE INDEX flow_principal_context_revocations_expires_idx
    ON flow_principal_context_revocations (expires_at);
