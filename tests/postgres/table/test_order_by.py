

def test_filter(postgres_db, clean_sql):
    user = postgres_db.Tables.user

    assert clean_sql(user.order_by("id").sql) == "SELECT * FROM test.user ORDER BY id ASC"
    assert clean_sql(user.order_by("id", "name").sql) == "SELECT * FROM test.user ORDER BY id, name ASC"
    assert clean_sql(user.order_by("name", user.Columns.id, ascending=False).sql) == "SELECT * FROM test.user ORDER BY name, test.user.id DESC"



