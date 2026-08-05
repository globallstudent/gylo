# Durable steps

The problem: `fulfil_order` charges the card, then crashes buying the
shipping label. The retry runs the whole function again — and charges the
card twice. Retries and idempotency are usually left as the reader's problem;
durable steps make partial progress a first-class thing instead.

```python
--8<-- "examples/durable_steps.py"
```

## How it works

A task registered with `durable=True` receives a `StepContext`. Each
`ctx.step(name, work)` runs `work` once and records what it returned; when
the task retries, completed steps **replay their recorded result instead of
running again**. The crash above resumes as: `"charge"` returns the recorded
`charge_id` without touching the card, `"label"` actually runs, life
continues.

The guarantee is strict on purpose: control does not pass to the next step
until the current one's record is durable in Postgres. A step that ran but
was not yet recorded would be repeated after a crash — which is the entire
thing being avoided — so the task waits for the write's acknowledgement, and
that round trip is the feature's honest price. Recording a repeat after a
crash-between-write-and-ack collapses into the existing record; a step never
duplicates.

## Rules that follow

**Step names are identities.** Stable names per logical action; a loop wants
`f"charge-{item.id}"`, not the same name twice — a repeated name replays the
first result.

**Step results must encode.** Whatever `work` returns is stored with
MessagePack and handed back on replay. Return identifiers, not live objects.

**Code between steps repeats.** Only steps replay; everything around them
runs on every attempt. Anything with a side effect belongs inside a step —
between steps, keep to pure computation on step results.

**Durable tasks are async.** The context is awaited, so a synchronous body
cannot hold it; registration rejects the combination outright.

The step context also carries `attempt`, `max_attempts`, and `final`, so a
durable task can tell its first attempt from its last.

## When to reach for it

One non-idempotent side effect: make the *task* idempotent instead. Two or
more, where a crash between them costs money or sends duplicate email —
that is what this is for. Steps are per-task opt-in and jobs that do not use
them pay nothing.
