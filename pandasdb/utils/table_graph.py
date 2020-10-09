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
            for column in to_table.COL:
                if fk_pattern.match(column.name):
                    DiG.add_edge(to_table.name, from_table.name, connection=f"id <--> {column}")

    return DiG


def graph_for_table(graph, table):
    pass
