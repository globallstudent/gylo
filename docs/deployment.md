# Deployment

## The shape of a worker

```bash
gylo worker --app myapp:app --queue default,mail
```

One `gylo` process is a Rust supervisor that owns the database pool, leasing,
retries, scheduling, and every state write. It spawns **one Python child per
core** (`--processes`) and dispatches jobs to them over a Unix socket. Task
code holds the interpreter lock, so a single child uses a single core no
matter how high concurrency goes — the per-core children are what spend the
rest of the machine. Scale beyond one machine by running more workers; they
coordinate through the queue itself and need no discovery of each other.

A child that dies is restarted with backoff and its in-flight jobs handed
back; a child that *cannot stay up* (usually a misconfigured `--app`) stops
the worker with the child's own last words in the error, rather than leaving
a process that looks healthy at zero capacity.

## Settings that matter

| Flag | Default | What it truly controls |
|---|---|---|
| `--processes` | core count | parallelism; one child ≈ one core |
| `--concurrency` | 256 | in-flight jobs per child; how far ahead of it the supervisor leases |
| `--batch` | 128 | jobs leased per round trip — bounded for crash-recovery latency, not speed |
| `--lease` | 30s | how long a dead worker's jobs wait before another recovers them |
| `--retry-base` | 1s | delay before the first retry; doubles each attempt, with jitter |
| `--retry-cap` | 1h | ceiling the retry backoff never exceeds |
| `--retain-completed` | 24h | finished-job (and result) lifetime — see below |
| `--retain-discarded` | 7d | dead-letter lifetime |

Leases renew automatically while a job runs, so `--lease` bounds *recovery
time*, never task duration. Settings whose failure mode would be silent are
rejected at startup instead — a maintenance interval at or above the lease,
for instance, would reclaim healthy jobs and run them twice.

Queues: `--queue a,b,c` consumes several; priority compares **across** them,
so a high-priority job in a quiet queue is not outranked by a busy neighbour.

## Shutdown

SIGTERM and SIGINT both drain: stop leasing, finish in-flight work (up to
30s), exit 0. Handlers are installed before anything else at startup, so a
rollout's early SIGTERM is honoured too. Anything not drained in time is
recovered by lease expiry — `kill -9` is an ordinary, tested event, not a
disaster.

For Kubernetes: `terminationGracePeriodSeconds` ≥ 40, and use
`--observe :9464` for probes — **liveness on `/healthz`** (process alive),
never on database reachability: a probe that fails on a Postgres blip
restarts every worker at once, turning a recovery into an outage.

## The table stays bounded

Maintenance deletes completed jobs after `--retain-completed` and dead
letters after `--retain-discarded`, in bounded, time-boxed batches that never
delay crash recovery. Two consequences worth planning around: the completed
window is also how long [results](results.md) are retrievable, and deleted
space is reused rather than returned to the OS — the table plateaus, it does
not shrink. First deployment onto a database with months of history:
`gylo jobs prune --older-than 30d` once, deliberately, rather than letting
maintenance discover it.

## Migrations

`gylo migrate` before rollout — embedded in the binary, additive, safe to
re-run, and safe to run while old workers are still up.
