ALTER TABLE flow_rooms
    ADD COLUMN created_by_principal_id UUID,
    ADD COLUMN empty_since TIMESTAMPTZ DEFAULT now(),
    ADD COLUMN join_grace_until TIMESTAMPTZ,
    ADD COLUMN activity_checked_at TIMESTAMPTZ,
    ADD COLUMN activity_check_token UUID;

UPDATE flow_rooms AS room
SET created_by_principal_id = COALESCE(
    (
        SELECT event.principal_id
        FROM audit_events AS event
        WHERE event.service_instance_id = room.service_instance_id
          AND event.action = 'flow.room.create'
          AND event.resource_type = 'room'
          AND event.resource_id = room.id::text
        ORDER BY event.created_at
        LIMIT 1
    ),
    (
        SELECT ticket.principal_id
        FROM match_assignments AS assignment
        JOIN matchmaking_tickets AS ticket ON ticket.id = assignment.ticket_id
        WHERE assignment.room_id = room.id
        ORDER BY ticket.created_at
        LIMIT 1
    ),
    '00000000-0000-0000-0000-000000000000'::uuid
);

ALTER TABLE flow_rooms
    ALTER COLUMN created_by_principal_id SET NOT NULL;

CREATE INDEX flow_rooms_principal_active_idx
    ON flow_rooms (service_instance_id, created_by_principal_id)
    WHERE state IN ('provisioning', 'ready');

CREATE INDEX flow_rooms_activity_check_idx
    ON flow_rooms (activity_checked_at NULLS FIRST, created_at)
    WHERE state = 'ready';

