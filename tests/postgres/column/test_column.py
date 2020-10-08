from pandasdb.connections import PostgresConnection as PC
import pandasdb.operators as ops
import pytest


def test_attributes(postgres_db):
    assert hasattr(postgres_db.TAB.user, "COL")
    assert hasattr(postgres_db.TAB.user.COL, "name")
    assert hasattr(postgres_db.TAB.user.COL.name, "name")
    assert hasattr(postgres_db.TAB.user.COL.name, "dtype")

def test_operations(postgres_db):
    user = postgres_db.TAB.user

    assert issubclass(type(user.COL.name == "Mikkel"), ops.Operator)
    assert type(user.COL.name == "Mikkel") == ops.EQ
    assert str(user.COL.name == "Mikkel") == "test.user.name = 'Mikkel'"

def test_descriptive_functions(postgres_db):
    user = postgres_db.TAB.user

    min_age = user.COL.age.min()
    assert str(min_age).replace("\n", " ").strip() == "SELECT MIN(test.user.age) FROM test.user"

    mean_age = user.COL.age.avg()
    assert str(mean_age).replace("\n", " ").strip() == "SELECT AVG(test.user.age) FROM test.user"

    max_age = user.COL.age.max()
    assert str(max_age).replace("\n", " ").strip() == "SELECT MAX(test.user.age) FROM test.user"

    count_age = user.COL.age.count()
    assert str(count_age).replace("\n", " ").strip() == "SELECT COUNT(test.user.age) FROM test.user"

    sum_age = user.COL.age.sum()
    assert str(sum_age).replace("\n", " ").strip() == "SELECT SUM(test.user.age) FROM test.user"


