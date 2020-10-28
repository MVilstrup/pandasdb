
"""
Mathematical Operators
"""


def test_add(postgres_db):
    assert str(postgres_db.ops.ADD(1, 2)) == "1 + 2"
    assert str(postgres_db.ops.ADD("cool", 2)) == "'cool' + 2"
    assert str(postgres_db.ops.ADD(postgres_db.ops.Column("test", int), 2)) == "test + 2"
    assert str(postgres_db.ops.ADD(2, postgres_db.ops.Column("test", int))) == "2 + test"


def test_subtract(postgres_db):
    assert str(postgres_db.ops.SUB(1, 2)) == "1 - 2"
    assert str(postgres_db.ops.SUB("cool", 2)) == "'cool' - 2"
    assert str(postgres_db.ops.SUB(postgres_db.ops.Column("test", int), 2)) == "test - 2"
    assert str(postgres_db.ops.SUB(2, postgres_db.ops.Column("test", int))) == "2 - test"


def test_multiply(postgres_db):
    assert str(postgres_db.ops.MUL(left=1, right=2)) == "1 * 2"
    assert str(postgres_db.ops.MUL(left="cool", right=2)) == "'cool' * 2"
    assert str(postgres_db.ops.MUL(left=postgres_db.ops.Column("test", int), right=2)) == "test * 2"

    assert str(postgres_db.ops.MUL(left=2, right="cool")) == "2 * 'cool'"
    assert str(postgres_db.ops.MUL(left=2, right=postgres_db.ops.Column("test", int))) == "2 * test"


def test_divide(postgres_db):
    assert str(postgres_db.ops.DIV(left=1, right=2)) == "1 / 2"
    assert str(postgres_db.ops.DIV(left="cool", right=2)) == "'cool' / 2"
    assert str(postgres_db.ops.DIV(left=postgres_db.ops.Column("test", int), right=2)) == "test / 2"

    assert str(postgres_db.ops.DIV(left=2, right="cool")) == "2 / 'cool'"
    assert str(postgres_db.ops.DIV(left=2, right=postgres_db.ops.Column("test", int))) == "2 / test"


def test_modulo(postgres_db):
    assert str(postgres_db.ops.MOD(left=1, right=2)) == "1 % 2"
    assert str(postgres_db.ops.MOD(left="cool", right=2)) == "'cool' % 2"
    assert str(postgres_db.ops.MOD(left=postgres_db.ops.Column("test", int), right=2)) == "test % 2"

    assert str(postgres_db.ops.MOD(left=2, right="cool")) == "2 % 'cool'"
    assert str(postgres_db.ops.MOD(left=2, right=postgres_db.ops.Column("test", int))) == "2 % test"


def test_power(postgres_db):
    assert str(postgres_db.ops.POW(left=1, right=2)) == "1 ^ 2"
    assert str(postgres_db.ops.POW(left="cool", right=2)) == "'cool' ^ 2"
    assert str(postgres_db.ops.POW(left=postgres_db.ops.Column("test", int), right=2)) == "test ^ 2"

    assert str(postgres_db.ops.POW(left=2, right="cool")) == "2 ^ 'cool'"
    assert str(postgres_db.ops.POW(left=2, right=postgres_db.ops.Column("test", int))) == "2 ^ test"


"""
Logical Operators
"""


def test_and(postgres_db):
    assert str(postgres_db.ops.AND(left=1, right=2)) == "(1 AND 2)"
    assert str(postgres_db.ops.AND(left="cool", right=2)) == "('cool' AND 2)"
    assert str(postgres_db.ops.AND(left=postgres_db.ops.Column("test", int), right=2)) == "(test AND 2)"
    assert str(postgres_db.ops.AND(left=2, right="cool")) == "(2 AND 'cool')"
    assert str(postgres_db.ops.AND(left=2, right=postgres_db.ops.Column("test", int))) == "(2 AND test)"


def test_or(postgres_db):
    assert str(postgres_db.ops.OR(left=1, right=2)) == "(1 OR 2)"
    assert str(postgres_db.ops.OR(left="cool", right=2)) == "('cool' OR 2)"
    assert str(postgres_db.ops.OR(left=postgres_db.ops.Column("test", int), right=2)) == "(test OR 2)"
    assert str(postgres_db.ops.OR(left=2, right="cool")) == "(2 OR 'cool')"
    assert str(postgres_db.ops.OR(left=2, right=postgres_db.ops.Column("test", int))) == "(2 OR test)"


def test_not(postgres_db):
    assert str(postgres_db.ops.NOT(right=2)) == "~ 2"
    assert str(postgres_db.ops.NOT(right="cool")) == "~ 'cool'"
    assert str(postgres_db.ops.NOT(right=postgres_db.ops.Column("test", int))) == "~ test"


"""
Comparison Operators
"""


def test_less_than(postgres_db):
    assert str(postgres_db.ops.LT(left=1, right=2)) == "1 < 2"
    assert str(postgres_db.ops.LT(left="cool", right=2)) == "'cool' < 2"
    assert str(postgres_db.ops.LT(left=postgres_db.ops.Column("test", int), right=2)) == "test < 2"
    assert str(postgres_db.ops.LT(left=2, right="cool")) == "2 < 'cool'"
    assert str(postgres_db.ops.LT(left=2, right=postgres_db.ops.Column("test", int))) == "2 < test"


def test_greater_than(postgres_db):
    assert str(postgres_db.ops.GT(left=1, right=2)) == "1 > 2"
    assert str(postgres_db.ops.GT(left="cool", right=2)) == "'cool' > 2"
    assert str(postgres_db.ops.GT(left=postgres_db.ops.Column("test", int), right=2)) == "test > 2"
    assert str(postgres_db.ops.GT(left=2, right="cool")) == "2 > 'cool'"
    assert str(postgres_db.ops.GT(left=2, right=postgres_db.ops.Column("test", int))) == "2 > test"


def test_less_equal(postgres_db):
    assert str(postgres_db.ops.LE(left=1, right=2)) == "1 <= 2"
    assert str(postgres_db.ops.LE(left="cool", right=2)) == "'cool' <= 2"
    assert str(postgres_db.ops.LE(left=postgres_db.ops.Column("test", int), right=2)) == "test <= 2"
    assert str(postgres_db.ops.LE(left=2, right="cool")) == "2 <= 'cool'"
    assert str(postgres_db.ops.LE(left=2, right=postgres_db.ops.Column("test", int))) == "2 <= test"


def test_greater_equal(postgres_db):
    assert str(postgres_db.ops.GE(left=1, right=2)) == "1 >= 2"
    assert str(postgres_db.ops.GE(left="cool", right=2)) == "'cool' >= 2"
    assert str(postgres_db.ops.GE(left=postgres_db.ops.Column("test", int), right=2)) == "test >= 2"
    assert str(postgres_db.ops.GE(left=2, right="cool")) == "2 >= 'cool'"
    assert str(postgres_db.ops.GE(left=2, right=postgres_db.ops.Column("test", int))) == "2 >= test"


def test_equal(postgres_db):
    assert str(postgres_db.ops.EQ(left=1, right=2)) == "1 = 2"
    assert str(postgres_db.ops.EQ(left="cool", right=2)) == "'cool' = 2"
    assert str(postgres_db.ops.EQ(left=postgres_db.ops.Column("test", int), right=2)) == "test = 2"
    assert str(postgres_db.ops.EQ(left=2, right="cool")) == "2 = 'cool'"
    assert str(postgres_db.ops.EQ(left=2, right=postgres_db.ops.Column("test", int))) == "2 = test"


def test_not_equal(postgres_db):
    assert str(postgres_db.ops.NE(left=1, right=2)) == "1 != 2"
    assert str(postgres_db.ops.NE(left="cool", right=2)) == "'cool' != 2"
    assert str(postgres_db.ops.NE(left=postgres_db.ops.Column("test", int), right=2)) == "test != 2"
    assert str(postgres_db.ops.NE(left=2, right="cool")) == "2 != 'cool'"
    assert str(postgres_db.ops.NE(left=2, right=postgres_db.ops.Column("test", int))) == "2 != test"
