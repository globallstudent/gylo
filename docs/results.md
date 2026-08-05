# Results and cancellation

```python
--8<-- "examples/results.py"
```

## Results are opt-in

`store_result=True` keeps the task's return value for retrieval with
`gylo.outcome(conn, job_id)`. It is off by default deliberately: most jobs run
for their effects, and storing what nobody reads costs a write per job and
rows that exist only to be pruned. A task that opts in must return something
MessagePack can encode — anything else fails the job rather than storing
garbage.

`outcome` returns the job's state, the decoded result, and the full error
history (every failed attempt, timestamped). `None` means no such job — which
includes a job whose row retention already removed. **The completed-retention
window is also the result-retrieval window** (24 hours by default); results
that must outlive it belong in your own tables, written by the task itself —
ideally as a [durable step](durable-steps.md).

There is no built-in blocking wait. Poll `outcome` if you must; but a caller
that needs the answer synchronously usually wanted a function call, not a
queue.

## Cancellation is honest

`gylo.cancel(conn, *ids)` cancels jobs that have not started, and returns how
many that was. A job already running is left alone — and this is a stance,
not a gap. Interrupting Python mid-task from outside means killing the child
process, which takes every sibling job in it; queues that advertise "revoke"
either do that or quietly do nothing. gylo does the part that can be done
correctly and tells you exactly what it did.

For cooperative cancellation of long tasks, put a flag in your own data and
have the task check it between units of work — with a [timeout](retries.md)
as the backstop for a task that stops checking.

Cancelling a workflow member behaves like any dead parent: descendants that
depended on it are cancelled with it.
