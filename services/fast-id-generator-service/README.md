# Fast ID Generator Service

A lightweight service that generates unique PUIDs (Payment Unique IDs) and MUIDs (Message Unique IDs) for the Clear Path Gateway. Designed to be stateless, horizontally scalable, and safe for concurrent multi-instance operation.

## Features
- PUID: 16-char ID = prefix `G31` + 13-digit numeric (DB sequence-backed, block-allocated)
- MUID: 16-char ID = prefix `MSG` + 13-digit numeric (DB sequence-backed, block-allocated)
- In-memory cache via Ehcache (JCache) for lightweight performance (no per-ID DB writes)
- Batch/block PUID generation (prefetch 1000 IDs per allocation)
- Health endpoint
- Stateless; safe for multi-instance scaling

## Endpoints
- GET `/health` → service status
- GET `/api/ids/puid?channel=G3I` → `{ puid }`
- GET `/api/ids/muid?puid=<puid>` → `{ muid }`
- GET `/api/ids/puid-block?channel=G3I&size=50` → `{ channel, count, puids: [] }`

## Uniqueness & Scalability
- Cross-pod uniqueness via a shared DB-backed monotonically increasing sequence.
- Each instance allocates blocks of 1000 IDs atomically; IDs are then served from memory.
- No per-ID persistence; only the sequence value advances.

## Caching
- Uses Spring Cache with Ehcache (JCache API).
- Configure via `spring.cache.jcache.config` if a custom ehcache.xml is desired (defaults are sufficient here).

## Configuration
- Server port: set `SERVER_PORT` (e.g., 8091) or `server.port` property.

## Local Development
- Run: `SPRING_PROFILES_ACTIVE=local SERVER_PORT=8091 mvn spring-boot:run`
- Health: `curl http://localhost:8091/health`
- Sample: `curl http://localhost:8091/api/ids/puid?channel=G3I`

## Testing
- Unit tests: `mvn -q -DskipITs test`
- Playwright E2E: from `services/fast-id-generator-service/e2e/`, run `npm i` then `npm test`. Ensure the service is running on the configured port.

## Production Hardening
- Run multiple instances behind a load balancer; assign unique `id.shard` per instance.
- Configure cache TTL/size for `muidByPuid` to align with business retention policies.
- Set resource limits and liveness/readiness probes.
- Structured logging/tracing per platform requirements.

## Failure/Backpressure Behavior
- If more than 1000 IDs are requested within a single second per instance, the generator advances to the next second to maintain uniqueness. Caller-side retries/backoff recommended under extreme spikes.

## License
Internal use.
