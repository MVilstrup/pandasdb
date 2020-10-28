import re
from collections import namedtuple

import networkx as nx


def generate_graph(tables):
    DiG = nx.DiGraph()

    for table in tables.values():
        DiG.add_node(table.table_name)

    for from_table in tables.values():
        for to_table in tables.values():

            fk_pattern = re.compile(f".*{from_table.table_name}_id.*")
            for column in to_table.columns:
                if fk_pattern.match(column.name):
                    DiG.add_edge(to_table.table_name, from_table.table_name, **{"from": "id", "to": f"{column}"})

    return DiG


def recursive_copy(from_graph, to_graph, node, d):
    if d == 0:
        return

    for to_node in from_graph.neighbors(node):
        to_graph.add_edge(node, to_node, **{"to": "id", "from": f"{to_node}_id"})
        recursive_copy(from_graph, to_graph, to_node, d - 1)

    for from_node in from_graph.predecessors(node):
        to_graph.add_edge(from_node, node, **{"to": f"id", "from": f"{node}_id"})
        recursive_copy(from_graph, to_graph, from_node, d - 1)


def shortest_join(G: nx.DiGraph, from_table: str, to_table: str):
    Connection = namedtuple("Connection", ("from_table", "to_table", "from_column", "to_column"))
    path = nx.shortest_path(G, from_table, to_table)

    joins = []
    for from_table, to_table in zip(path, path[1:]):
        edge = G.edges[from_table, to_table]
        joins.append(
            Connection(from_table=from_table, to_table=to_table, from_column=edge["to"], to_column=edge["from"]))

    return joins
