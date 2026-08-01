CREATE TABLE flow_signaling_connections (
    connection_id UUID PRIMARY KEY,
    organization_id UUID NOT NULL,
    project_id UUID NOT NULL,
    service_instance_id UUID NOT NULL,
    room_id UUID NOT NULL REFERENCES flow_rooms(id) ON DELETE CASCADE,
    principal_id UUID NOT NULL,
    opened_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    last_seen_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    closed_at TIMESTAMPTZ,
    FOREIGN KEY (service_instance_id, organization_id, project_id)
        REFERENCES flow_service_instances (id, organization_id, project_id)
        ON DELETE CASCADE
);

CREATE INDEX flow_signaling_connections_active_scope_idx
    ON flow_signaling_connections (
        organization_id,
        project_id,
        service_instance_id,
        last_seen_at DESC
    )
    WHERE closed_at IS NULL;

CREATE TABLE flow_delete_operations (
    id UUID PRIMARY KEY,
    service_instance_id UUID NOT NULL UNIQUE,
    organization_id UUID NOT NULL,
    project_id UUID NOT NULL,
    principal_id UUID NOT NULL,
    generation BIGINT NOT NULL CHECK (generation > 0),
    state TEXT NOT NULL CHECK (state IN ('deleting', 'succeeded')),
    status JSONB NOT NULL CHECK (jsonb_typeof(status) = 'object'),
    provider_room_names JSONB NOT NULL DEFAULT '[]'::jsonb
        CHECK (jsonb_typeof(provider_room_names) = 'array'),
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    UNIQUE (service_instance_id, generation)
);

CREATE TABLE flow_delete_token_receipts (
    jwt_id UUID PRIMARY KEY,
    service_instance_id UUID NOT NULL,
    organization_id UUID NOT NULL,
    project_id UUID NOT NULL,
    principal_id UUID NOT NULL,
    generation BIGINT NOT NULL CHECK (generation > 0),
    operation_id UUID NOT NULL REFERENCES flow_delete_operations(id),
    created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX flow_delete_token_receipts_instance_idx
    ON flow_delete_token_receipts (service_instance_id, generation);
