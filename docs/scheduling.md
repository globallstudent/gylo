# Scheduling and cron

```python
--8<-- "examples/scheduling.py"
```

## Delayed jobs

`delay=` holds a job for that many seconds before it becomes runnable. The
delay is applied in the database, so it survives worker restarts and needs no
scheduler process.

## Cron

`@app.cron(expression, ...)` registers a task that also runs on a schedule.
Five-field expressions are standard cron; a six-field form adds leading
seconds. `timezone` matters for anything coarser than hourly: `0 3 * * *` in
`Europe/London` is a different UTC instant in January than in July, and gylo
resolves the zone — including skipped and repeated hours at DST transitions —
rather than drifting twice a year.

**The schedule lives with the code.** When a worker starts, its children
declare their schedules upward and gylo records them; deploying the code *is*
deploying the schedule. Editing the expression takes effect on the next
deploy; a rename is a new schedule.

**There is no leader.** Every worker checks for due schedules; a conditional
`UPDATE` on the schedule row means exactly one of them wins each occurrence.
No election, no lock service, no single process whose death silences every
schedule. This was measured against the alternative: leader election added a
failover stall and saved nothing.

**Missed runs are skipped, not backfilled.** A schedule due while the whole
fleet was down advances to its next occurrence on recovery. If you need
catch-up semantics, enqueue the backlog explicitly — silently replaying an
unknown number of missed runs is the wrong default for jobs like "charge all
customers".

Resolution is the worker's maintenance interval (10s by default): schedules
are examined once per tick, so an every-second expression fires once per tick,
not sixty times a minute.

## Pausing

Set `paused` on the row in `gylo_cron` to stop a schedule without deploying.
gylo never unsets it — a redeploy updates the schedule's definition but leaves
an operator's pause standing.
