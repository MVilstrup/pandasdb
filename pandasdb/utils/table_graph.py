import networkx as nx
import re


def generate_graph(tables):
    table_data = {table.name: table for table in tables}

    DiG = nx.DiGraph()

    for table in table_data.values():
        DiG.add_node(table.name, table=table)

    for from_table in table_data.values():
        for to_table in table_data.values():

            fk_pattern = re.compile(f".*{from_table.name}_id.*")
            for column in to_table.Columns:
                if fk_pattern.match(column.name):
                    DiG.add_edge(to_table.name, from_table.name, **{"from": "id", "to": f"{column}"})

    return DiG


def recursive_copy(from_graph, to_graph, node, d):
    if d == 0:
        return

    for to_node in from_graph.neighbors(node):
        to_graph.add_edge(node, to_node, **{"to": "id", "from": f"{to_node}_id"})
        recursive_copy(from_graph, to_graph, to_node, d - 1)

    for from_node in from_graph.predecessors(node):
        to_graph.add_edge(from_node, node, **{"to": "id", "from": f"{node}_id"})
        recursive_copy(from_graph, to_graph, from_node, d - 1)