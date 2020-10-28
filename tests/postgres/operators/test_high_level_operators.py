import numpy as np

def test_in(postgres_db):
    assert str(postgres_db.ops.IN(1, [1, 2, 3])) == "1 IN (1, 2, 3)"
    assert str(postgres_db.ops.IN(left=1, right=[1, 2, 3])) == "1 IN (1, 2, 3)"
    assert str(postgres_db.ops.IN(left=1, right=(1, 2, 3))) == "1 IN (1, 2, 3)"
    assert str(postgres_db.ops.IN(left=1, right=np.array([1, 2, 3]))) == "1 IN (1, 2, 3)"
    assert str(postgres_db.ops.IN(left=1, right={1: True, 2: True, 3: True})) == "1 IN (1, 2, 3)"

