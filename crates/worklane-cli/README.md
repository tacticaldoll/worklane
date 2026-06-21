# worklane-cli

Operator CLI (`wl` binary) for inspecting and maintaining durable [worklane]
brokers — SQLite, Postgres, and Redis.

**Layer:** top-of-stack binary. Depends on `worklane-core` plus all three durable
brokers. Not a library; nothing depends on it.

## Install

```sh
cargo install worklane-cli   # installs the `wl` binary
```

## Commands

```sh
# Lane health: pending + dead-letter counts
wl --broker sqlite --db ./jobs.db stats default

# Inspect dead letters (jsonl default, or --format table)
wl --broker postgres --url $DATABASE_URL dead-letters list critical --limit 20

# Requeue one dead-lettered job (prompts unless -y)
wl --broker redis --url $REDIS_URL dead-letters requeue <uuid>

# Purge all dead letters on a lane (prompts unless -y)
wl --broker sqlite --db ./jobs.db dead-letters purge default
```

## Connection

`--broker` is `sqlite` (needs `--db`), `postgres`, or `redis`. For postgres/redis
the URL is `--url`, else `$WORKLANE_URL`, else `$DATABASE_URL` / `$REDIS_URL`. The
chosen source is printed to stderr; the URL itself is never printed.

[worklane]: https://docs.rs/worklane

## License

Licensed under either of MIT or Apache-2.0 at your option.
