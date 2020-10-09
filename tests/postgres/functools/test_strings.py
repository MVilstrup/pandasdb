import pandasdb.functools as ft


def test_like(postgres_db, clean_sql):
    user = postgres_db.TAB.user

    assert clean_sql(str(ft.like(user.COL.id, ".*4"))) == "test.user.id LIKE '.*4'"
    assert clean_sql(str(ft.not_like(user.COL.id, ".*4"))) == "test.user.id NOT LIKE '.*4'"



