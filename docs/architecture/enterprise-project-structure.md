# Enterprise Project Structure

This guide defines a practical enterprise structure for StreamFlow while keeping the current repository stable.

## Current Assessment

The project has strong functional coverage, but structure is mixed:

1. Service entry points are split between cmd and domain folders.
2. Operational jobs and long-running services are mixed in the same command space.
3. Architecture and service ownership are not documented in one place.

## Recommended Enterprise Layout

Use this as the target organization model:

1. apps/
   - apps/broker
   - apps/management-api
   - apps/processor
   - apps/schema-registry
   - apps/connector
   - apps/region-gateway

2. jobs/
   - jobs/backup
   - jobs/streamql-runner

3. internal/
   - internal/broker
   - internal/raft
   - internal/processor
   - internal/schema
   - internal/connectors

4. platform/
   - platform/helm
   - platform/k8s
   - platform/observability

5. docs/
   - docs/architecture
   - docs/adr
   - docs/runbooks

## Non-Disruptive Plan

To avoid breaking imports and deployment scripts, migrate in stages:

1. Stage 1: Document boundaries and owners (completed by this file and the microservice catalog).
2. Stage 2: Introduce apps and jobs folders with thin wrappers that call existing packages (completed; legacy command paths retained for compatibility).
3. Stage 3: Move shared code into internal modules and deprecate legacy command paths.
4. Stage 4: Update CI, Dockerfiles, Helm templates, and runbooks to new paths.

## Ownership Model

Suggested team boundaries:

1. Data Plane Team
   - broker, raft, processor runtime

2. Control Plane Team
   - management API, schema registry

3. Integration Team
   - connector framework, gateway

4. Platform Team
   - helm, k8s, observability, ADR governance

## Definition of Organized

The project can be considered enterprise-organized when:

1. Every deployable process is listed in docs/architecture/microservices-catalog.md.
2. Every deployable process has a dedicated app or job folder.
3. Platform assets are grouped under a single platform root.
4. Runbooks and ADRs reference stable service names and paths.
