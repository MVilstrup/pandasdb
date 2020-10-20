

def test_filter(postgres_db, clean_sql):
    user = postgres_db.Tables.user

    assert clean_sql(user.filter(user.Columns.id == 1).sql) == "SELECT * FROM test.user WHERE test.user.id = 1"
    assert clean_sql(user.filter(user.Columns.id == "cool").sql) == "SELECT * FROM test.user WHERE test.user.id = 'cool'"
    assert clean_sql(user.where(user.Columns.id == "cool").sql) == "SELECT * FROM test.user WHERE test.user.id = 'cool'"


