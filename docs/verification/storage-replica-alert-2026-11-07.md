# Storage replica alert verification

The resource collector reads Longhorn volumes and engines with a namespace-scoped,
read-only role. For an attached volume it counts `RW` replicas only when one
running engine supplies a replica mode map. A `WO` rebuilding replica is not a
read/write copy. Missing, malformed, or ambiguous observations are reported as
unavailable instead of being converted to zero; a failed engine inventory has
its own warning. The Prometheus alert for at most one read/write copy is
critical, while the existing degraded-volume alert remains a warning.

The [resource monitoring workflow](https://github.com/IPA-CyberLab/IPA-RS-HeteroCloud-Flow/actions/runs/42360954684)
passed the collector tests and Prometheus expression cases, including zero,
one and two copies, duplicated collectors and unavailable observations. In
production, the collector reported one `RW` copy for
`pvc-704f080b-d4e1-4bdb-8cc3-579224c60a36`, and Prometheus reported
`HeteroCloudVolumeSingleReadWriteReplica` as firing at critical severity. This
is the persistent home volume for the Coder `CKB-VM` workspace.

This alert does not repair or back up the volume. At verification time the
second replica was repeatedly being rebuilt and removed. External alert
delivery is not configured. The evidence and investigation of the rebuild race
are recorded in the [HeteroNetwork incident report](https://github.com/IPA-CyberLab/IPA-RS-HeteroNetwork/blob/master/docs/incidents/2026-11-07-identity-postgres-latency.md).
