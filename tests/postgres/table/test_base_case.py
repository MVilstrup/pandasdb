def test_base_case(postgres_db, clean_sql):
    assert clean_sql(postgres_db.TAB.user.query) == "SELECT * FROM test.user"
    assert clean_sql(postgres_db.TAB.car.query) == "SELECT * FROM test.car"


