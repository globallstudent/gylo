# Workflows

```python
--8<-- "examples/workflows.py"
```

## One primitive

A workflow is a directed graph of jobs and the edges between them. `chain`,
`group`, and `chord` are constructors over that one primitive, not three
separate machineries:

- `chain(a, b, c)` — each step starts when the one before it finishes
- `group(a, b, c)` — every branch runs independently
- `chord(body, callback)` — the callback starts once everything in `body`
  finished

They compose: a chain step may itself be a group, in which case every leaf of
one stage becomes a parent of every root of the next. Anything expressible as
a DAG is expressible here, and there is exactly one dependency mechanism to
be correct about.

## How it runs

A job with unmet dependencies is parked outside the fetch's view; completing a
parent decrements its children and, at zero, releases them. Fan-in is safe by
construction — the decrement happens in the same statement that finalises the
parents, so several parents finishing simultaneously cannot lose an update.

The whole graph becomes visible **atomically**. Enqueueing a workflow opens a
transaction (a savepoint, when your connection already holds one), so no
worker can lease a root before the edges behind it exist. This is not
theoretical care: an earlier build committed nodes before edges, and a fast
worker could complete a root in the gap — stranding the rest of the graph
forever. The soak harness caught it in its first minute.

## Failure semantics

A job that dead-letters cancels everything downstream of it, transitively.
Without that, a failed step in a chain leaves its descendants waiting on a
parent that will never finish. Branches not downstream of the failure are
untouched — one shard failing does not cancel its siblings, only the chord
callback that needed all of them.

Retention never breaks a graph apart: finished members of a workflow are kept
while *any* member is still waiting or running, however old they are, so a
graph under inspection always reads whole.

## What workflows are not

There is no data passing along edges — a child receives its own arguments,
not its parents' return values. Share state through your own tables keyed by
something the tasks agree on. This is deliberate scope: implicit result piping
turns a queue into a badly-specified dataflow engine.
