def test_join(postgres_db, clean_sql):
    user = postgres_db.Tables.user
    sql = clean_sql(user.join(postgres_db.Tables.car, on_column="user_id", from_column="id", kind="LEFT"))
    assert sql == 'SELECT * FROM test.user LEFT JOIN SELECT * FROM test.car ON test.user.id = test.car.user_id'

    sql = clean_sql(user.join(postgres_db.Tables.car, on_column="user_id", from_column="id", kind="RIGHT"))
    assert sql == 'SELECT * FROM test.user RIGHT JOIN SELECT * FROM test.car ON test.user.id = test.car.user_id'

    sql = clean_sql(user.join(postgres_db.Tables.car, on_column="user_id", from_column="id", kind="INNER"))
    assert sql == 'SELECT * FROM test.user INNER JOIN SELECT * FROM test.car ON test.user.id = test.car.user_id'

    sql = clean_sql(user.join(postgres_db.Tables.car, on_column="user_id", from_column="id", kind="OUTER LEFT"))
    assert sql == 'SELECT * FROM test.user OUTER LEFT JOIN SELECT * FROM test.car ON test.user.id = test.car.user_id'


def test_implicit_join(postgres_db, clean_sql):
    user = postgres_db.Tables.user
    sql = clean_sql(user.join(postgres_db.Tables.car, kind="LEFT"))
    assert sql == 'SELECT * FROM test.user LEFT JOIN SELECT * FROM test.car ON test.user.id = test.car.user_id'
