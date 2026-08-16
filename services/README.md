# Services Registry

This directory is the canonical registry for every deployable service and job.

## Rules

1. Each service or job must have its own folder under `services/`.
2. Each folder must include a `service.yaml` with ownership, runtime, and API metadata.
3. If an entrypoint changes in `apps/` or `jobs/`, update the matching `service.yaml` in the same PR.
4. If a new service is introduced, add it here and in `docs/architecture/microservices-catalog.md`.

## Current Entries

1. `services/broker`
2. `services/processor`
3. `services/processor-server`
4. `services/management-api`
5. `services/schema-registry`
6. `services/connector`
7. `services/region-gateway`
8. `services/backup-job`
9. `services/streamql-runner-job`
