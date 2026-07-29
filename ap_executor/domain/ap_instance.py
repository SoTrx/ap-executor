import re
from collections.abc import Iterator
from typing import Any, Dict, Tuple

from moma_management.domain.analytical_pattern import AnalyticalPattern
from moma_management.domain.generated.edges.edge_schema import EdgeLabel
from moma_management.domain.generated.nodes.node_schema import Node
from pydantic import BaseModel


class ApInstance(BaseModel):
    """An AP *instance*: an AP template plus the runtime values to execute it with.

    ``state`` is namespaced by operator node id, mapping each operator's input
    name to its caller-supplied value (or, once executed, its output):
    ``{node_id: {name: value}}``.
    """
    ap: AnalyticalPattern
    state: Dict[str, Dict[str, Any]] = {}

    def model_dump(self, **kwargs):
        """Serialize by alias by default.

        dapr-ext-workflow's automatic workflow-input serialization dumps by
        field name but reconstructs by alias only (no ``populate_by_name``).
        That breaks for any nested ``moma_management`` model whose alias
        differs from its field name --- e.g. ``Edge.from_`` (aliased to
        ``"from"``, since ``from`` is a Python keyword). Default to
        ``by_alias=True`` so the round trip through Dapr matches.
        """
        kwargs.setdefault("by_alias", True)
        return super().model_dump(**kwargs)

    def iter_operators(self) -> Iterator[Node]:
        """Return operator nodes in topological (dependency) order.

        Operators connected via ``follows`` edges are ordered such that
        predecessors are executed first. Operators without ordering constraints
        are appended in node-list order.
        """
        OPERATOR_LABELS = {"Operator"}
        operator_nodes = [
            n for n in self.ap.nodes
            if OPERATOR_LABELS & set(n.labels)
        ]

        if not operator_nodes:
            return []

        # Build adjacency from "follows" edges (from_ follows to_ => to_ first)
        id_to_node = {n.id: n for n in operator_nodes}
        in_degree: dict = {n.id: 0 for n in operator_nodes}
        adj: dict = {n.id: [] for n in operator_nodes}

        for edge in self.ap.edges or []:
            if EdgeLabel.follows in edge.labels:
                # edge.from_ follows edge.to  →  edge.to must come before edge.from_
                if edge.to in id_to_node and edge.from_ in id_to_node:
                    adj[edge.to].append(edge.from_)
                    in_degree[edge.from_] += 1

        # Kahn's algorithm (operator with the least dependency is executed first)
        queue = [nid for nid, deg in in_degree.items() if deg == 0]
        seen = set()

        while queue:
            nid = queue.pop(0)
            seen.add(nid)
            yield id_to_node[nid]

            for neighbour in adj[nid]:
                in_degree[neighbour] -= 1
                if in_degree[neighbour] == 0:
                    queue.append(neighbour)

    def resolve_operator_input_values(self, operator_id: str) -> Dict[str, Any]:
        """
        Resolve the input values of an operator given the current state of the ap instance

        Args:
            operator_id: The id of the operator whose inputs are being resolved

        Upstream-mapped values overlay (override) any same-named caller parameter.
        """
        resolved: Dict[str, Any] = dict(
            self.state.get(operator_id, {}))
        output_index = self._output_index()

        for edge in self.ap.edges or []:
            if EdgeLabel.input not in edge.labels or str(edge.to) != operator_id:
                continue

            edge_mapping = (
                edge.properties.mapping if edge.properties else None) or {}
            for target_expr, source_expr in edge_mapping.items():
                target_input = self._last_key(target_expr)
                source_field = self._last_key(source_expr)
                # Trace through a ResultType hop if present; otherwise the input
                # edge points straight at the producing operator.
                producer_id, output_name = output_index.get(
                    (str(edge.from_), source_field), (str(edge.from_), source_field)
                )
                producer_result = self.state.get(producer_id)
                if producer_result is not None and output_name in producer_result:
                    resolved[target_input] = producer_result[output_name]

        return resolved

    def _output_index(self) -> Dict[Tuple[str, str], Tuple[str, str]]:
        """Map every ``output`` edge to ``(target_node, field) -> (producer_op, output_name)``.

        Lets an ``input`` edge that reads a ``ResultType`` field trace back to the
        operator (and output name) that actually produced the value.
        """
        index: Dict[Tuple[str, str], Tuple[str, str]] = {}
        for edge in self.ap.edges or []:
            if EdgeLabel.output not in edge.labels:
                continue
            edge_mapping = (
                edge.properties.mapping if edge.properties else None) or {}
            for target_expr, source_expr in edge_mapping.items():
                index[(str(edge.to), self._last_key(target_expr))] = (
                    str(edge.from_),
                    self._last_key(source_expr),
                )
        return index

    def _last_key(self, expr: str) -> str:
        """Return the last bracketed identifier of a mapping expression.

        ``to['inputs']['sql']`` -> ``sql``; ``from['outputs']['query']`` -> ``query``.
        Falls back to the raw expression if it isn't bracketed.
        """
        # TODO: Replace by jsonpath parsing
        KEY_RE = re.compile(r"\['([^']+)'\]")
        keys = KEY_RE.findall(expr)
        return keys[-1] if keys else expr
