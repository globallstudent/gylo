"""Workflows as a single directed graph.

There is one primitive — a graph of jobs and the edges between them — and
`chain`, `group`, and `chord` are constructors over it rather than separate
kinds of thing. Anything they can express, a graph can, so there is only one
composition to make correct.

A job with unmet dependencies is inserted as `available` with `scheduled_at`
at infinity, so it is invisible to the ordinary fetch without needing a state
of its own. Completing a parent decrements its children and, at zero, sets
`scheduled_at` to now.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Any

import msgspec

from ._adapters import adapter_for

if TYPE_CHECKING:
    from . import Options

__all__ = ["Signature", "Workflow", "chain", "chord", "group"]


@dataclass(frozen=True, slots=True)
class Signature:
    """A task and its arguments, not yet enqueued."""

    task: str
    args: tuple[Any, ...]
    kwargs: dict[str, Any]
    options: Options
    durable: bool = False


@dataclass
class Workflow:
    """Jobs and the edges between them, enqueued together or not at all."""

    nodes: list[Signature] = field(default_factory=list)
    edges: set[tuple[int, int]] = field(default_factory=set)

    def _absorb(self, other: Workflow) -> list[int]:
        """Copies another graph in, returning its nodes' new indices."""
        offset = len(self.nodes)
        self.nodes.extend(other.nodes)
        self.edges.update(
            (parent + offset, child + offset) for parent, child in other.edges
        )
        return list(range(offset, offset + len(other.nodes)))

    @property
    def roots(self) -> list[int]:
        """Nodes nothing else depends on producing."""
        blocked = {child for _, child in self.edges}
        return [i for i in range(len(self.nodes)) if i not in blocked]

    @property
    def leaves(self) -> list[int]:
        """Nodes that nothing depends on."""
        parents = {parent for parent, _ in self.edges}
        return [i for i in range(len(self.nodes)) if i not in parents]

    def dependencies(self) -> list[int]:
        counts = [0] * len(self.nodes)
        for _, child in self.edges:
            counts[child] += 1
        return counts

    async def enqueue(self, conn: Any, /) -> list[int]:
        """Insert every job and edge, returning the ids in node order.

        The adapter opens the driver's own transaction — a savepoint when the
        caller already holds one — so the graph becomes visible whole or not
        at all. On an autocommit connection the statements would otherwise
        commit one by one, and a worker can complete a root before its edges
        exist; the fan-in then finds nothing to decrement and the rest of the
        graph parks forever.
        """
        if not self.nodes:
            return []
        return await adapter_for(conn).insert_workflow(
            conn,
            [
                (
                    node.options.queue,
                    node.task,
                    _encoded(node),
                    node.options.priority,
                    node.options.max_attempts,
                    float(node.options.delay),
                    node.options.concurrency_key,
                    node.options.max_concurrency,
                    node.durable,
                    pending,
                )
                for node, pending in zip(self.nodes, self.dependencies(), strict=True)
            ],
            sorted(self.edges),
        )


def _encoded(node: Signature) -> bytes:
    return msgspec.msgpack.encode((node.args, node.kwargs))


def _as_workflow(item: Signature | Workflow) -> Workflow:
    if isinstance(item, Workflow):
        return item
    return Workflow(nodes=[item])


def chain(*steps: Signature | Workflow) -> Workflow:
    """Run each step after the one before it finishes.

    A step may itself be a graph, in which case every leaf of the previous step
    becomes a parent of every root of the next.
    """
    flow = Workflow()
    tails: list[int] = []
    for step in steps:
        sub = _as_workflow(step)
        if not sub.nodes:
            continue
        offset = len(flow.nodes)
        heads = [offset + root for root in sub.roots]
        next_tails = [offset + leaf for leaf in sub.leaves]
        flow._absorb(sub)
        for parent in tails:
            for head in heads:
                flow.edges.add((parent, head))
        tails = next_tails
    return flow


def group(*branches: Signature | Workflow) -> Workflow:
    """Run every branch independently, with nothing waiting on the others."""
    flow = Workflow()
    for branch in branches:
        flow._absorb(_as_workflow(branch))
    return flow


def chord(body: Signature | Workflow, callback: Signature) -> Workflow:
    """Run `callback` once everything in `body` has finished.

    Fan-in needs no special handling: the callback counts more than one
    dependency, and whichever parent finishes last releases it.
    """
    flow = _as_workflow(body)
    tails = flow.leaves
    added = flow._absorb(Workflow(nodes=[callback]))
    for tail in tails:
        flow.edges.add((tail, added[0]))
    return flow
