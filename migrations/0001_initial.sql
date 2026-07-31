CREATE TABLE flow_service_instances (
    id UUID PRIMARY KEY,
    organization_id UUID NOT NULL,
    project_id UUID NOT NULL,
    name TEXT NOT NULL CHECK (char_length(name) BETWEEN 1 AND 120),
    desired_generation BIGINT NOT NULL CHECK (desired_generation > 0),
    desired_spec JSONB NOT NULL CHECK (jsonb_typeof(desired_spec) = 'object'),
    observed_generation BIGINT NOT NULL CHECK (observed_generation >= 0),
    status JSONB NOT NULL CHECK (jsonb_typeof(status) = 'object'),
    current_operation_id UUID NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    UNIQUE (id, organization_id, project_id),
    UNIQUE (project_id, name)
);

CREATE INDEX flow_service_instances_scope_idx
    ON flow_service_instances (organization_id, project_id, id);

CREATE TABLE flow_reconcile_operations (
    id UUID PRIMARY KEY,
    service_instance_id UUID NOT NULL REFERENCES flow_service_instances(id) ON DELETE CASCADE,
    organization_id UUID NOT NULL,
    project_id UUID NOT NULL,
    principal_id UUID NOT NULL,
    generation BIGINT NOT NULL CHECK (generation > 0),
    name TEXT NOT NULL,
    spec JSONB NOT NULL CHECK (jsonb_typeof(spec) = 'object'),
    state TEXT NOT NULL CHECK (state IN ('accepted', 'succeeded', 'failed')),
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    UNIQUE (service_instance_id, generation),
    FOREIGN KEY (service_instance_id, organization_id, project_id)
        REFERENCES flow_service_instances (id, organization_id, project_id)
        ON DELETE CASCADE
);

CREATE TABLE flow_provider_token_receipts (
    jwt_id UUID PRIMARY KEY,
    service_instance_id UUID NOT NULL REFERENCES flow_service_instances(id) ON DELETE CASCADE,
    organization_id UUID NOT NULL,
    project_id UUID NOT NULL,
    generation BIGINT NOT NULL CHECK (generation > 0),
    name TEXT NOT NULL,
    spec JSONB NOT NULL CHECK (jsonb_typeof(spec) = 'object'),
    operation_id UUID NOT NULL REFERENCES flow_reconcile_operations(id) ON DELETE CASCADE,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    FOREIGN KEY (service_instance_id, organization_id, project_id)
        REFERENCES flow_service_instances (id, organization_id, project_id)
        ON DELETE CASCADE
);

CREATE INDEX flow_provider_token_receipts_instance_idx
    ON flow_provider_token_receipts (service_instance_id, generation);

CREATE TABLE flow_rooms (
    id UUID PRIMARY KEY,
    organization_id UUID NOT NULL,
    project_id UUID NOT NULL,
    service_instance_id UUID NOT NULL,
    name TEXT NOT NULL,
    provider_room_name TEXT,
    mode TEXT NOT NULL CHECK (mode IN ('p2p', 'sfu')),
    state TEXT NOT NULL CHECK (state IN ('provisioning', 'ready', 'failed', 'closed')),
    max_participants INTEGER NOT NULL CHECK (max_participants BETWEEN 2 AND 1000),
    metadata JSONB NOT NULL DEFAULT '{}'::jsonb CHECK (jsonb_typeof(metadata) = 'object'),
    failure_reason TEXT,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    UNIQUE (service_instance_id, name),
    UNIQUE (provider_room_name),
    FOREIGN KEY (service_instance_id, organization_id, project_id)
        REFERENCES flow_service_instances (id, organization_id, project_id)
        ON DELETE CASCADE
);

CREATE INDEX flow_rooms_scope_created_idx
    ON flow_rooms (
        organization_id,
        project_id,
        service_instance_id,
        created_at DESC
    );

CREATE TABLE matchmaking_tickets (
    id UUID PRIMARY KEY,
    organization_id UUID NOT NULL,
    project_id UUID NOT NULL,
    service_instance_id UUID NOT NULL,
    principal_id UUID NOT NULL,
    queue_name TEXT NOT NULL,
    mode TEXT NOT NULL CHECK (mode IN ('p2p', 'sfu')),
    match_size INTEGER NOT NULL CHECK (match_size BETWEEN 2 AND 100),
    state TEXT NOT NULL CHECK (state IN ('queued', 'matching', 'assigned', 'cancelled', 'expired')),
    attributes JSONB NOT NULL DEFAULT '{}'::jsonb CHECK (jsonb_typeof(attributes) = 'object'),
    reservation_id UUID,
    reservation_expires_at TIMESTAMPTZ,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    expires_at TIMESTAMPTZ NOT NULL,
    CHECK (
        (
            state = 'matching'
            AND reservation_id IS NOT NULL
            AND reservation_expires_at IS NOT NULL
        )
        OR (
            state <> 'matching'
            AND reservation_id IS NULL
            AND reservation_expires_at IS NULL
        )
    ),
    FOREIGN KEY (service_instance_id, organization_id, project_id)
        REFERENCES flow_service_instances (id, organization_id, project_id)
        ON DELETE CASCADE
);

CREATE INDEX matchmaking_tickets_match_idx
    ON matchmaking_tickets (
        organization_id,
        project_id,
        service_instance_id,
        queue_name,
        mode,
        match_size,
        created_at
    )
    WHERE state = 'queued';

CREATE INDEX matchmaking_tickets_reservation_idx
    ON matchmaking_tickets (reservation_id)
    WHERE reservation_id IS NOT NULL;

CREATE UNIQUE INDEX matchmaking_tickets_active_principal_idx
    ON matchmaking_tickets (
        service_instance_id,
        queue_name,
        principal_id
    )
    WHERE state IN ('queued', 'matching');

CREATE TABLE match_assignments (
    id UUID PRIMARY KEY,
    ticket_id UUID NOT NULL UNIQUE REFERENCES matchmaking_tickets(id) ON DELETE CASCADE,
    room_id UUID NOT NULL REFERENCES flow_rooms(id) ON DELETE CASCADE,
    peer_principal_ids JSONB NOT NULL CHECK (jsonb_typeof(peer_principal_ids) = 'array'),
    created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX match_assignments_room_idx ON match_assignments (room_id);

CREATE TABLE audit_events (
    id UUID PRIMARY KEY,
    organization_id UUID NOT NULL,
    project_id UUID NOT NULL,
    service_instance_id UUID NOT NULL,
    principal_id UUID NOT NULL,
    request_id TEXT NOT NULL,
    action TEXT NOT NULL,
    resource_type TEXT NOT NULL,
    resource_id TEXT,
    outcome TEXT NOT NULL CHECK (outcome IN ('allowed', 'denied', 'failed')),
    details JSONB NOT NULL DEFAULT '{}'::jsonb CHECK (jsonb_typeof(details) = 'object'),
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    FOREIGN KEY (service_instance_id, organization_id, project_id)
        REFERENCES flow_service_instances (id, organization_id, project_id)
        ON DELETE CASCADE
);

CREATE INDEX audit_events_scope_created_idx
    ON audit_events (
        organization_id,
        project_id,
        service_instance_id,
        created_at DESC
    );

CREATE TABLE usage_events (
    id UUID PRIMARY KEY,
    organization_id UUID NOT NULL,
    project_id UUID NOT NULL,
    service_instance_id UUID NOT NULL,
    principal_id UUID,
    event_type TEXT NOT NULL,
    resource_id TEXT,
    quantity BIGINT NOT NULL CHECK (quantity >= 0),
    idempotency_key TEXT NOT NULL,
    dimensions JSONB NOT NULL DEFAULT '{}'::jsonb CHECK (jsonb_typeof(dimensions) = 'object'),
    occurred_at TIMESTAMPTZ NOT NULL,
    recorded_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    UNIQUE (service_instance_id, idempotency_key),
    FOREIGN KEY (service_instance_id, organization_id, project_id)
        REFERENCES flow_service_instances (id, organization_id, project_id)
        ON DELETE CASCADE
);

CREATE INDEX usage_events_scope_occurred_idx
    ON usage_events (
        organization_id,
        project_id,
        service_instance_id,
        occurred_at DESC
    );
