# Keyed concurrency

One tenant uploads ten thousand exports; every worker slot fills with their
jobs; every other tenant waits. Global concurrency limits cannot express the
fix — "at most N *per tenant*" needs a limit attached to a key.

```python
--8<-- "examples/keyed.py"
```

`concurrency_key` and `max_concurrency` are set together at enqueue: while
`max_concurrency` jobs sharing the key are running, further jobs with that key
wait — for the key's slots, not blocking anyone else's. Keys are free-form
strings; `tenant:acme`, `export:eu-west`, one key per external API you must
not hammer.

## What the limit actually promises

The limit holds **across every worker and every child process**, not merely
within one — enforced at the moment of leasing, in the database, under a lock
that makes the running-count trustworthy. This is worth stating because it
was once false here: an early build let two workers each read "zero running"
and both admit, and the test that caught it had to watch execution overlap
rather than count completions. The guarantee is now pinned by tests that fail
on precisely that overlap.

Two properties worth knowing:

- **Keyed work cannot be starved.** Even when unkeyed jobs saturate every
  fetch, at least one keyed job is admitted per cycle whenever any are
  waiting.
- **Only keyed jobs pay.** A deployment that never sets a key runs the same
  single-statement fetch path as if the feature did not exist; the
  serialisation cost applies only where the guarantee is wanted.

## What it is not

It is not rate limiting — it bounds *simultaneous* jobs, not jobs per second.
A million jobs under one key with `max_concurrency=2` all run eventually, two
at a time. Priorities still order jobs *within* what admission allows.
