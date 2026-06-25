/// Lua helpers, prepended to every script that touches a lane-ZSET member or the
/// per-lane active-priority index.
///
/// **`fifo_member` / `fifo_id`** — the lane-ZSET member is `<seq>:<id>`, where
/// `seq` is a per-broker monotonic counter (`INCR ns:seq`) zero-padded to a fixed
/// width. ZSET tie-breaking is lexicographic on equal scores, so the padded
/// sequence makes equal-visibility jobs sort by enqueue order — strict FIFO within
/// one priority and visibility time, which the broker contract requires. The id
/// alone could not: it is a random UUID, so its lexicographic order is arbitrary.
/// `INCR` counts up from 1, and `i64::MAX` is 19 digits, so width 20 holds every
/// sequence value the counter produces: the padding never truncates a non-negative
/// sequence into a narrower, mis-sorting field. (`fifo_member` assumes a
/// non-negative sequence — which `INCR` from a fresh namespace always yields.)
///
/// **`prio_add`** — records that a priority level is in use on a lane in the
/// `ns:lane:{lane}:prios` ZSET (member = priority, score = priority). `reserve`
/// sweeps only the priorities present there (highest first) instead of all 256,
/// and prunes a level once its ZSET empties. This bounds the sweep to the
/// priorities actually used on the lane.
const LUA_HELPERS: &str = r#"
local function fifo_member(seq, id)
    return string.format('%020d', tonumber(seq))..':'..id
end
local function fifo_id(member)
    return string.match(member, '^%d+:(.+)$')
end
local function prio_add(ns, lane, priority)
    redis.call('ZADD', ns..':lane:'..lane..':prios', tonumber(priority), priority)
end
-- Move a live job to the dead-letter store: release any unique key (retaining it
-- on the dead record for a later requeue), write the dead record, remove the
-- lane-ZSET member, drop the job hash and any receipt reverse-index it held, then
-- apply retention pruning. Shared by `fail` and the `max_deliveries` bound in
-- `reserve`, so both follow exactly the same dead-letter path. `deadAt` is the
-- now-nanos timestamp; `maxCount` is the count bound (0 = no bound), and
-- `hasAgeBound` says whether `ageCutoff` is active. A real cutoff may be 0 when
-- the injected clock is near epoch, so age pruning must not use 0 as a sentinel.
local function dead_letter_move(ns, id, job, lane, priority, lz, member, err, deadAt, maxCount, ageCutoff, hasAgeBound)
    local uk = redis.call('HGET', job, 'unique_key')
    if uk then redis.call('DEL', ns..':unique:'..uk) end
    local env = redis.call('HGET', job, 'envelope')
    local att = redis.call('HGET', job, 'attempts')
    local deadKey = ns..':dead:'..lane
    -- The dead store is a ZSET scored by dead_at, membered by fifo_member(seq,id)
    -- (the same member the lane ZSET used), so count is ZCARD, age-prune is
    -- ZREMRANGEBYSCORE, and count-prune is ZREMRANGEBYRANK; the seq in the member
    -- preserves FIFO order when several records share a dead_at. The member is
    -- stashed on the hash so `requeue` can address the ZSET by id alone.
    redis.call('ZADD', deadKey, deadAt, member)
    redis.call('HSET', ns..':dead:job:'..id, 'envelope', env, 'error', err, 'lane', lane, 'priority', priority, 'attempts', att, 'dead_at', deadAt, 'member', member)
    if uk then redis.call('HSET', ns..':dead:job:'..id, 'unique_key', uk) end
    redis.call('ZREM', lz, member)
    local rcpt = redis.call('HGET', job, 'receipt')
    if rcpt then redis.call('DEL', ns..':rcpt:'..rcpt) end
    redis.call('DEL', job)
    -- Retention pruning, scoped to this lane's dead ZSET (age first, then count),
    -- matching the bounds and order `fail` passes.
    if hasAgeBound == 1 then
        local aged = redis.call('ZRANGEBYSCORE', deadKey, '-inf', '('..ageCutoff)
        for _, m in ipairs(aged) do redis.call('DEL', ns..':dead:job:'..fifo_id(m)) end
        if #aged > 0 then redis.call('ZREMRANGEBYSCORE', deadKey, '-inf', '('..ageCutoff) end
    end
    if maxCount > 0 then
        local excess = redis.call('ZCARD', deadKey) - maxCount
        if excess > 0 then
            local old = redis.call('ZRANGE', deadKey, 0, excess - 1)
            for _, m in ipairs(old) do redis.call('DEL', ns..':dead:job:'..fifo_id(m)) end
            redis.call('ZREMRANGEBYRANK', deadKey, 0, excess - 1)
        end
    end
end
"#;

const ENQUEUE: &str = r#"
local ns, id, lane, availableAt, env, ukey, priority = ARGV[1], ARGV[2], ARGV[3], ARGV[4], ARGV[5], ARGV[6], tonumber(ARGV[7])
if redis.call('EXISTS', ns..':job:'..id) == 1 then
    return redis.call('HGET', ns..':job:'..id, 'envelope')
end
if ukey ~= '' then
    local existing = redis.call('GET', ns..':unique:'..ukey)
    if existing then
        return redis.call('HGET', ns..':job:'..existing, 'envelope')
    end
end
local job = ns..':job:'..id
local seq = redis.call('INCR', ns..':seq')
redis.call('HSET', job, 'envelope', env, 'lane', lane, 'priority', priority, 'available_at', availableAt, 'attempts', 0, 'deliveries', 0, 'seq', seq)
if ukey ~= '' then
    redis.call('HSET', job, 'unique_key', ukey)
    redis.call('SET', ns..':unique:'..ukey, id)
end
redis.call('ZADD', ns..':lane:'..lane..':p:'..priority, tonumber(availableAt), fifo_member(seq, id))
prio_add(ns, lane, priority)
return env
"#;

const ENQUEUE_BATCH: &str = r#"
local ns = ARGV[1]
local count = (#ARGV - 1) / 6
local results = {}
for i = 0, count - 1 do
    local offset = 2 + (i * 6)
    local id = ARGV[offset]
    local lane = ARGV[offset + 1]
    local availableAt = ARGV[offset + 2]
    local env = ARGV[offset + 3]
    local ukey = ARGV[offset + 4]
    local priority = ARGV[offset + 5]

    local skip = false
    if redis.call('EXISTS', ns..':job:'..id) == 1 then
        results[i + 1] = redis.call('HGET', ns..':job:'..id, 'envelope')
        skip = true
    end
    if not skip and ukey ~= '' then
        local existing = redis.call('GET', ns..':unique:'..ukey)
        if existing then
            results[i + 1] = redis.call('HGET', ns..':job:'..existing, 'envelope')
            skip = true
        end
    end

    if not skip then
        local job = ns..':job:'..id
        local seq = redis.call('INCR', ns..':seq')
        redis.call('HSET', job, 'envelope', env, 'lane', lane, 'priority', priority, 'available_at', availableAt, 'attempts', 0, 'deliveries', 0, 'seq', seq)
        if ukey ~= '' then
            redis.call('HSET', job, 'unique_key', ukey)
            redis.call('SET', ns..':unique:'..ukey, id)
        end
        redis.call('ZADD', ns..':lane:'..lane..':p:'..priority, tonumber(availableAt), fifo_member(seq, id))
        prio_add(ns, lane, priority)
        results[i + 1] = env
    end
end
return results
"#;

const RESERVE: &str = r#"
local ns, lane, now, leaseUntil, receipt = ARGV[1], ARGV[2], tonumber(ARGV[3]), tonumber(ARGV[4]), ARGV[5]
local max = tonumber(ARGV[6])
local maxCount, ageCutoff, hasAgeBound = tonumber(ARGV[7]), tonumber(ARGV[8]), tonumber(ARGV[9])
local sweep_cap = tonumber(ARGV[10])
local swept = 0
local prios = ns..':lane:'..lane..':prios'
for _, p in ipairs(redis.call('ZREVRANGE', prios, 0, -1)) do
    local lz = ns..':lane:'..lane..':p:'..p
    if redis.call('ZCARD', lz) == 0 then
        redis.call('ZREM', prios, p)
    else
        while true do
            local found = redis.call('ZRANGEBYSCORE', lz, '-inf', now, 'LIMIT', 0, 1)
            if #found == 0 then break end
            local member = found[1]
            local id = fifo_id(member)
            local job = ns..':job:'..id
            if max > 0 and tonumber(redis.call('HGET', job, 'deliveries') or 0) + 1 > max then
                dead_letter_move(ns, id, job, lane, p, lz, member, 'exceeded max deliveries ('..max..')', now, maxCount, ageCutoff, hasAgeBound)
                swept = swept + 1
                if swept >= sweep_cap then return false end
            else
                local old = redis.call('HGET', job, 'receipt')
                if old then redis.call('DEL', ns..':rcpt:'..old) end
                redis.call('HINCRBY', job, 'deliveries', 1)
                redis.call('HSET', job, 'leased_until', leaseUntil, 'receipt', receipt)
                redis.call('ZADD', lz, leaseUntil, member)
                redis.call('SET', ns..':rcpt:'..receipt, id)
                return { redis.call('HGET', job, 'envelope'), tonumber(redis.call('HGET', job, 'attempts')) }
            end
        end
    end
end
return false
"#;

const GUARD: &str = r#"
local ns, receipt, now = ARGV[1], ARGV[2], tonumber(ARGV[3])
local id = redis.call('GET', ns..':rcpt:'..receipt)
if not id then return 0 end
local job = ns..':job:'..id
local cur = redis.call('HGET', job, 'receipt')
local lu = redis.call('HGET', job, 'leased_until')
if cur ~= receipt or (not lu) or tonumber(lu) <= now then return 0 end
local lane = redis.call('HGET', job, 'lane')
local priority = redis.call('HGET', job, 'priority') or '0'
local lz = ns..':lane:'..lane..':p:'..priority
local member = fifo_member(redis.call('HGET', job, 'seq'), id)
"#;

const FREE_UNIQUE: &str = r#"
local uk = redis.call('HGET', job, 'unique_key')
if uk then redis.call('DEL', ns..':unique:'..uk) end
"#;

const REQUEUE: &str = r#"
local ns, id, now = ARGV[1], ARGV[2], tonumber(ARGV[3])
local dead = ns..':dead:job:'..id
if redis.call('EXISTS', dead) == 0 then return 0 end
if redis.call('EXISTS', ns..':job:'..id) == 1 then return 3 end
local env = redis.call('HGET', dead, 'envelope')
local lane = redis.call('HGET', dead, 'lane')
local priority = redis.call('HGET', dead, 'priority') or '0'
local att = redis.call('HGET', dead, 'attempts')
local uk = redis.call('HGET', dead, 'unique_key')
local dmember = redis.call('HGET', dead, 'member')
if uk and redis.call('GET', ns..':unique:'..uk) then return 2 end
local job = ns..':job:'..id
local seq = redis.call('INCR', ns..':seq')
redis.call('HSET', job, 'envelope', env, 'lane', lane, 'priority', priority, 'available_at', now, 'attempts', att, 'deliveries', 0, 'seq', seq)
if uk then
    redis.call('HSET', job, 'unique_key', uk)
    redis.call('SET', ns..':unique:'..uk, id)
end
redis.call('ZADD', ns..':lane:'..lane..':p:'..priority, now, fifo_member(seq, id))
prio_add(ns, lane, priority)
redis.call('ZREM', ns..':dead:'..lane, dmember)
redis.call('DEL', dead)
return 1
"#;

const PURGE_DEAD: &str = r#"
local ns, lane = ARGV[1], ARGV[2]
local deadKey = ns..':dead:'..lane
local members = redis.call('ZRANGE', deadKey, 0, -1)
for _, m in ipairs(members) do redis.call('DEL', ns..':dead:job:'..fifo_id(m)) end
local n = #members
redis.call('DEL', deadKey)
return n
"#;

const ENQUEUE_SCHEDULED: &str = r#"
local key, occ = KEYS[1], ARGV[1]
local current = redis.call('GET', key)
if current and current >= occ then
    return 0
end
redis.call('SET', key, occ)

local ns, id, lane, availableAt, env, ukey, priority = ARGV[2], ARGV[3], ARGV[4], ARGV[5], ARGV[6], ARGV[7], tonumber(ARGV[8])
if redis.call('EXISTS', ns..':job:'..id) == 1 then
    return 1
end
if ukey ~= '' then
    local existing = redis.call('GET', ns..':unique:'..ukey)
    if not existing then
        local job = ns..':job:'..id
        local seq = redis.call('INCR', ns..':seq')
        redis.call('HSET', job, 'envelope', env, 'lane', lane, 'priority', priority, 'available_at', availableAt, 'attempts', 0, 'deliveries', 0, 'seq', seq)
        redis.call('HSET', job, 'unique_key', ukey)
        redis.call('SET', ns..':unique:'..ukey, id)
        redis.call('ZADD', ns..':lane:'..lane..':p:'..priority, tonumber(availableAt), fifo_member(seq, id))
        prio_add(ns, lane, priority)
    end
else
    local job = ns..':job:'..id
    local seq = redis.call('INCR', ns..':seq')
    redis.call('HSET', job, 'envelope', env, 'lane', lane, 'priority', priority, 'available_at', availableAt, 'attempts', 0, 'deliveries', 0, 'seq', seq)
    redis.call('ZADD', ns..':lane:'..lane..':p:'..priority, tonumber(availableAt), fifo_member(seq, id))
    prio_add(ns, lane, priority)
end
return 1
"#;

pub(crate) const PENDING_COUNT: &str = r#"
local ns, lane = ARGV[1], ARGV[2]
local prios = redis.call('ZRANGE', ns..':lane:'..lane..':prios', 0, -1)
local total = 0
for _, p in ipairs(prios) do
    total = total + redis.call('ZCARD', ns..':lane:'..lane..':p:'..p)
end
return total
"#;

/// Classify a job by id: `1` = live (job hash exists), `2` = dead-lettered (dead
/// hash exists), `0` = completed/unknown. Both existence checks run in one script
/// so the by-id classification is atomic (no TOCTOU between the two `EXISTS`).
/// Keyed by `KEYS[1]` (the `ns:job:{id}` hash) and `KEYS[2]` (the
/// `ns:dead:job:{id}` hash); takes no `ARGV`.
const CLASSIFY: &str = "\
if redis.call('EXISTS', KEYS[1]) == 1 then return 1 \
elseif redis.call('EXISTS', KEYS[2]) == 1 then return 2 \
else return 0 end";

pub(crate) fn enqueue() -> String {
    format!("{LUA_HELPERS}{ENQUEUE}")
}

pub(crate) fn enqueue_batch() -> String {
    format!("{LUA_HELPERS}{ENQUEUE_BATCH}")
}

pub(crate) fn reserve() -> String {
    format!("{LUA_HELPERS}{RESERVE}")
}

pub(crate) fn ack() -> String {
    format!(
        "{LUA_HELPERS}\n\
         {GUARD}\n\
         {FREE_UNIQUE}\n\
         redis.call('ZREM', lz, member)\n\
         redis.call('DEL', job)\n\
         redis.call('DEL', ns..':rcpt:'..receipt)\n\
         return 1"
    )
}

pub(crate) fn retry() -> String {
    format!(
        "{LUA_HELPERS}\n\
         {GUARD}\n\
         redis.call('HINCRBY', job, 'attempts', 1)\n\
         redis.call('HSET', job, 'available_at', ARGV[4])\n\
         redis.call('HDEL', job, 'leased_until', 'receipt')\n\
         redis.call('ZADD', lz, tonumber(ARGV[4]), member)\n\
         redis.call('DEL', ns..':rcpt:'..receipt)\n\
         return 1"
    )
}

pub(crate) fn defer() -> String {
    format!(
        "{LUA_HELPERS}\n\
         {GUARD}\n\
         redis.call('HSET', job, 'available_at', ARGV[4])\n\
         redis.call('HDEL', job, 'leased_until', 'receipt')\n\
         redis.call('ZADD', lz, tonumber(ARGV[4]), member)\n\
         redis.call('DEL', ns..':rcpt:'..receipt)\n\
         return 1"
    )
}

pub(crate) fn fail() -> String {
    format!(
        "{LUA_HELPERS}\n\
         {GUARD}\n\
         dead_letter_move(ns, id, job, lane, priority, lz, member, ARGV[4], ARGV[3], tonumber(ARGV[5]), tonumber(ARGV[6]), tonumber(ARGV[7]))\n\
         return 1"
    )
}

pub(crate) fn extend() -> String {
    format!(
        "{LUA_HELPERS}\n\
         {GUARD}\n\
         redis.call('HSET', job, 'leased_until', ARGV[4])\n\
         redis.call('ZADD', lz, tonumber(ARGV[4]), member)\n\
         return 1"
    )
}

pub(crate) fn purge_dead() -> String {
    format!("{LUA_HELPERS}{PURGE_DEAD}")
}

pub(crate) fn requeue() -> String {
    format!("{LUA_HELPERS}{REQUEUE}")
}

pub(crate) fn enqueue_scheduled() -> String {
    format!("{LUA_HELPERS}{ENQUEUE_SCHEDULED}")
}

/// Every lifecycle Lua script, each built — and SHA1-hashed by
/// `redis::Script::new` — exactly once at broker construction and reused on every
/// call. This is the Redis analogue of the Postgres broker's precomputed
/// `Queries` struct: `Script::new` does the `format!` concatenation of
/// [`LUA_HELPERS`] with each body and the SHA1 digest in its constructor, so
/// building these once instead of per call removes that allocation and hashing
/// from the throughput-critical consume loop.
///
/// The `scripts::*` body functions above remain the single source of script text;
/// [`Scripts::new`] is the only caller that wraps them in a [`redis::Script`].
pub(crate) struct Scripts {
    pub(crate) enqueue: redis::Script,
    pub(crate) enqueue_batch: redis::Script,
    pub(crate) reserve: redis::Script,
    pub(crate) ack: redis::Script,
    pub(crate) retry: redis::Script,
    pub(crate) defer: redis::Script,
    pub(crate) fail: redis::Script,
    pub(crate) extend: redis::Script,
    pub(crate) requeue: redis::Script,
    pub(crate) purge_dead: redis::Script,
    pub(crate) pending_count: redis::Script,
    pub(crate) enqueue_scheduled: redis::Script,
    pub(crate) classify: redis::Script,
}

impl Scripts {
    /// Build and hash every script once. Called from the `RedisBroker`
    /// constructor; infallible (`redis::Script::new` does not return a `Result`).
    pub(crate) fn new() -> Self {
        Self {
            enqueue: redis::Script::new(&enqueue()),
            enqueue_batch: redis::Script::new(&enqueue_batch()),
            reserve: redis::Script::new(&reserve()),
            ack: redis::Script::new(&ack()),
            retry: redis::Script::new(&retry()),
            defer: redis::Script::new(&defer()),
            fail: redis::Script::new(&fail()),
            extend: redis::Script::new(&extend()),
            requeue: redis::Script::new(&requeue()),
            purge_dead: redis::Script::new(&purge_dead()),
            pending_count: redis::Script::new(PENDING_COUNT),
            enqueue_scheduled: redis::Script::new(&enqueue_scheduled()),
            classify: redis::Script::new(CLASSIFY),
        }
    }
}
