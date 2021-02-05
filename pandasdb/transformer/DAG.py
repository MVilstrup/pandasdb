import numpy as np
import networkx as nx


class TransformDAG:
    __root__ = "<ROOT>"

    def __init__(self, initial_columns):
        self.DAG = nx.DiGraph()
        self.DAG.add_node(self.__root__, type="ROOT")

        for column in initial_columns:
            self.DAG.add_node(column, type="INPUT", dependencies=None)
            self.DAG.add_edge(self.__root__, column)

    def add(self, transformation):
        if transformation.name not in self.DAG.nodes:
            self.DAG.add_node(transformation.name,
                              type=type(transformation),
                              dependencies=transformation.input_columns,
                              element=transformation)

        else:
            self.DAG.nodes[transformation.name]["type"] = type(transformation)
            self.DAG.nodes[transformation.name]["dependencies"] = transformation.input_columns
            self.DAG.nodes[transformation.name]["element"] = transformation

        for dependency in transformation.input_columns:
            self.DAG.add_edge(dependency, transformation.name)

    def __iter__(self):
        required_columns = set([name for name in self.DAG.nodes if name])

        state = set()
        for name in self.DAG.successors(self.__root__):
            if name in required_columns and name != self.__root__:
                state.add(name)

                yield self.DAG.nodes[name].get("element")

        missing_streams = [name for name in required_columns if name not in state and name != self.__root__]
        while missing_streams:
            for name in missing_streams:
                dependencies = [dependency for dependency in self.DAG.predecessors(name) if dependency != self.__root__]
                if all([dependency in state for dependency in dependencies]):
                    state.add(name)

                    yield self.DAG.nodes[name].get("element")

            new_missing_streams = [node for node in self.DAG.nodes if node not in state and node != self.__root__]

            if set(new_missing_streams) == set(missing_streams):
                raise ValueError(f"Cannot solve the dependencies for: {new_missing_streams}")
            else:
                missing_streams = new_missing_streams
