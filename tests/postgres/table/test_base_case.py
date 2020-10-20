def test_base_case(postgres_db, clean_sql):
    assert clean_sql(postgres_db.Tables.user.sql) == "SELECT * FROM test.user"
    assert clean_sql(postgres_db.Tables.car.sql) == "SELECT * FROM test.car"


