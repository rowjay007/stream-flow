# Service Standards

This document defines baseline enterprise standards for all StreamFlow services.

## Required for Every Service

1. Dedicated service metadata under `services/<name>/service.yaml`.
2. Stable runtime entrypoint under `apps/` for long-running services and `jobs/` for batch tools.
3. Health endpoint for HTTP services.
4. Ownership declared in `.github/CODEOWNERS` and in service metadata.
5. Change validation through `go test ./... -count=1` and `make build`.

## API and Backward Compatibility

1. Existing endpoints must not change semantics without an explicit migration note.
2. New endpoints should be additive.
3. Breaking changes must include rollback guidance in PR description.

## Observability

1. Expose health checks for every HTTP service.
2. Ensure metrics and tracing changes are documented.
3. Keep dashboards under `observability/` updated for user-visible metrics changes.

## Security and Operations

1. Prefer environment-based configuration for secrets and runtime options.
2. Keep auth controls in management/control-plane APIs.
3. Include operational notes for any new service in architecture docs.

## Definition of Done for New Service

1. New app/job entrypoint exists and builds.
2. `services/<name>/service.yaml` exists.
3. Microservice catalog updated.
4. README updated if user-facing run command changed.
5. CI passes.
