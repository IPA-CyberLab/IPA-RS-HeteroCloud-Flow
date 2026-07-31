# ADR-0002: PostgreSQL-backed Matchmaking

- Status: Accepted
- Date: 2026-07-31

## Decision

Matchmaking uses PostgreSQL as the durable queue. Workers select a compatible
organization/project/service-instance/queue/mode/size group, serialize claims
for that group with a transaction-scoped advisory lock, lock the selected
tickets with `FOR UPDATE`, then move them to an expiring `matching`
reservation. Different groups can be processed independently, while competing
workers cannot split one match between them. This permits any number of
identical workers without an elected leader.

The worker provisions the mode-specific room outside the transaction. On
success it atomically creates assignments and marks the room ready. On error it
marks the room failed and returns unexpired tickets to the queue. A later
worker reclaims reservations whose worker died.

Compatibility in the initial version is exact equality of organization,
project, service instance, queue, mode, and requested match size. Ticket
attributes are stored for future policy evaluation but do not yet influence
matching.

## Consequences

- Queue and assignment state survives process and Redis failures.
- A provider room can outlive a failed database completion. The reservation
  reaper recovers tickets and marks the provisioning database room failed;
  empty LiveKit rooms expire, and a future reconciler should delete such
  provider orphans proactively.
- Geographic, skill, party, and backfill matching remain future policy layers
  over the same reservation protocol.
