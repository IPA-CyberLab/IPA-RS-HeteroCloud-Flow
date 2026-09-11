# Release-Bound Helm Dependencies

The two archives under `deploy/helm/heterocloud-flow/charts/` are intentionally
tracked. This binds the templates consumed by Helm to the same source commit
as the parent chart. `Chart.lock` selects dependency versions but does not
replace verification of the downloaded archive bytes.

On 2026-09-11, fresh `helm pull` downloads from the repositories declared in
`Chart.yaml` matched the existing archives byte-for-byte:

| Chart | Version | OCI Manifest Digest |
| --- | --- | --- |
| redis | 23.1.1 | `sha256:f4a368f7a67f4f2bedee2426bfb063b960565ee38a91fdf07185a014c9e63406` |
| postgresql-ha | 16.3.2 | `sha256:78ae138a11c4f6f058fcb18c94e57b0bb52b1b557f92975f70c0dd74b4eb2d94` |

Both originate from `oci://registry-1.docker.io/bitnamicharts`.
Archive SHA-256 values, distinct from OCI manifest digests, are checked by:

```sh
sha256sum --strict --check scripts/chart-dependencies.sha256
```

Release CI verifies these committed bytes instead of resolving dependency
tags again. When updating a dependency, review its upstream source, update
Chart.yaml/Chart.lock and the vendored archive and checksum together, and issue
a new release. Do not alter archives for an already published source revision.
Upstream license files remain inside the unmodified archives.

This pins chart templates, not all container images emitted by enabled
dependencies. DEV/prod image digest validation remains a separate requirement.
The archives are included even when external database/Redis services disable
their templates so that Helm can resolve dependencies entirely offline.
