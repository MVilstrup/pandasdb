

def test_filter(postgres_db, clean_sql):
    user = postgres_db.TAB.user

    assert clean_sql(user.filter(user.COL.id == 1).query) == "SELECT * FROM test.user WHERE test.user.id = 1"
    assert clean_sql(user.filter(user.COL.id == "cool").query) == "SELECT * FROM test.user WHERE test.user.id = 'cool'"
    assert clean_sql(user.where(user.COL.id == "cool").query) == "SELECT * FROM test.user WHERE test.user.id = 'cool'"


