# Retries and timeouts

```python
--8<-- "examples/retries.py"
```

## The retry policy

A failure is retried when the exception matches `retry_on` (default: any
`Exception`) and does not match `no_retry_on`. Exclusions win, so a broad
policy narrows without restating itself. Raising `gylo.NoRetryError` fails the
job permanently regardless of policy — for when the *data* is wrong and no
number of attempts will change that.

Two failures never consult the policy, because another attempt cannot come out
differently: a task name no worker recognises, and a payload that does not
decode.

## Backoff

Retries reschedule with exponential backoff — `retry_base` (1s) doubling per
attempt up to `retry_cap` (1h) — with 50–100% jitter, so a burst of
simultaneous failures does not return as a synchronised thundering herd. The
delay is computed in the database from the job's own attempt count.

After `max_attempts` (default 20), the job dead-letters with its complete
error history — every attempt's traceback, timestamped, on the row. See
[Operations](operations.md) for inspecting and retrying dead letters.

## Timeouts — on by default

Every task has a deadline: 300 seconds from the app, overridable per task,
`None` to opt out. The default matters more than the knob. A task with no
deadline that stops making progress holds its lease *forever* — the worker
keeps renewing it faithfully — and the concurrency slot it occupies never
comes back. Enough of those and a worker sits at zero throughput reporting no
errors. A default that covers tasks whose author never thought about it is
what prevents that.

What a timeout can honestly do differs by task type, and gylo does not paper
over it:

- an **async** task is cancelled at the deadline — the work actually stops
- a **sync** task runs on a thread, and threads cannot be interrupted: the job
  fails on time and the slot returns, but the thread runs to completion in the
  background. The only way to kill it would be killing the child process and
  every sibling job with it.

Timeouts count as ordinary failures: the retry policy decides what happens
next, and the error history records `TimeoutError`.

## Attempt-aware behaviour

For "retry twice, then page a human", combine the retry policy with
`context=True` and `ctx.final` — see [Tasks and options](tasks.md#task-self-context).
