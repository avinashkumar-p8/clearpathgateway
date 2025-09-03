# Fast ID Generator Service

A lightweight service that generates unique PUIDs (Payment Unique IDs) and MUIDs (Message Unique IDs) for the Clear Path Gateway. Designed to be stateless, horizontally scalable, and safe for concurrent multi-instance operation.

## Features
- PUID: 16-char ID = 3-char channel + 9-digit epoch seconds + 1-char shard + 3-digit sequence
- MUID: PUID-prefixed ID with a randomized base36 suffix
- MUID cache: `@Cacheable` memoizes MUID per PUID for idempotency and consistency
- Batch/block PUID generation
- Health endpoint
- Stateless; safe for multi-instance scaling

## Endpoints
- GET `/health` → service status
- GET `/api/ids/puid?channel=G3I` → `{ puid }`
- GET `/api/ids/muid?puid=<puid>` → `{ muid }`
- GET `/api/ids/puid-block?channel=G3I&size=50` → `{ channel, count, puids: [] }`

## Uniqueness & Scalability
- Within one instance, per-second sequence (000–999) is capped at 1000; if exceeded in the same second, the generator advances to the next second to avoid collisions.
- Cross-instance uniqueness is achieved using `id.shard` (single character) configured per instance (env `ID_SHARD`). Ensure different shard values per instance under extreme load.
- MUID adds randomness; memoized per PUID for idempotency via Spring Cache.

## Caching
- Uses Spring Cache with Caffeine.
- Cache name: `muidByPuid` (key = PUID, value = MUID).
- Configure via Spring properties (example):
  - `spring.cache.cache-names=muidByPuid`
  - `spring.cache.caffeine.spec=maximumSize=100000,expireAfterWrite=6h`

## Configuration
- `id.shard` (env: `ID_SHARD`): single character shard discriminator (default `0`).
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
