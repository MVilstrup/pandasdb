import pandasdb.sql.utils as utils


def test_graph_generation(postgres_db):
    DiG = utils.generate_graph(postgres_db.Tables)

    for table in postgres_db.Tables:
        assert DiG.has_node(table.name)
        assert DiG.nodes[table.name]["table"] == table

    user_table = postgres_db.Tables.user.name
    car_table = postgres_db.Tables.car.name
    assert DiG.has_edge(user_table, user_table)
    assert DiG.has_edge(car_table, user_table)
    assert not DiG.has_edge(user_table, car_table)
