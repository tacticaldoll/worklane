# Development tasks. `just` (https://github.com/casey/just) is optional sugar —
# every recipe below is a plain command you can also run by hand.

# Local test-service endpoints, matching docker-compose.yml's host ports (shifted
# off the defaults to avoid colliding with a local Postgres/Redis). Single source
# of truth: if you change a port, change it in docker-compose.yml too.
pg_url := "postgres://worklane:worklane@localhost:55432/worklane"
redis_url := "redis://localhost:56379"

# List recipes.
default:
    @just --list

# Start the local Postgres + Redis test services and wait until healthy.
up:
    docker compose up -d --wait

# Stop and remove the test services (and their volumes).
down:
    docker compose down -v

# Run the full suite WITHOUT live services (the durable-broker tiers skip).
test:
    cargo test --workspace

# Run the full suite WITH live services, as CI does: bring the services up, point
# the tests at them, and run. The durable-broker tiers run instead of skipping.
test-live: up
    WORKLANE_POSTGRES_TEST_URL="{{pg_url}}" WORKLANE_REDIS_TEST_URL="{{redis_url}}" cargo test --workspace

# Format + lint gate (mirrors the CI `lint` job).
lint:
    cargo fmt --all --check
    cargo clippy --workspace --all-targets -- -D warnings

# Apply formatting.
fmt:
    cargo fmt --all

# Supply-chain gate (mirrors the CI `deny` job): RustSec advisories, licenses,
# banned/wildcard deps, source allow-listing. Needs `cargo install cargo-deny`.
audit:
    cargo deny check advisories bans licenses sources
