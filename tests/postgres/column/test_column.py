from pandasdb.connections import PostgresConnection as PC
import pandasdb.operators as ops
import pytest


def test_attributes(postgres_db):
    assert hasattr(postgres_db.Ts.user, "Cs")
    assert hasattr(postgres_db.Ts.user.Cs, "name")
    assert hasattr(postgres_db.Ts.user.Cs.name, "name")
    assert hasattr(postgres_db.Ts.user.Cs.name, "dtype")

def test_operations(postgres_db):
    user = postgres_db.Ts.user

    assert issubclass(type(user.Cs.name == "Mikkel"), ops.Operator)
    assert type(user.Cs.name == "Mikkel") == ops.EQ
    assert str(user.Cs.name == "Mikkel") == "test.user.name = 'Mikkel'"

def test_descriptive_functions(postgres_db):
    user = postgres_db.Ts.user

    min_age = user.Cs.age.min()
    assert str(min_age).replace("\n", " ").strip() == "SELECT MIN(test.user.age) FROM test.user"

    mean_age = user.Cs.age.avg()
    assert str(mean_age).replace("\n", " ").strip() == "SELECT AVG(test.user.age) FROM test.user"

    max_age = user.Cs.age.max()
    assert str(max_age).replace("\n", " ").strip() == "SELECT MAX(test.user.age) FROM test.user"

    count_age = user.Cs.age.count()
    assert str(count_age).replace("\n", " ").strip() == "SELECT COUNT(test.user.age) FROM test.user"

    sum_age = user.Cs.age.sum()
    assert str(sum_age).replace("\n", " ").strip() == "SELECT SUM(test.user.age) FROM test.user"


