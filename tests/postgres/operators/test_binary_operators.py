from pandasdb.connections import PostgresConnection as PC

"""
Mathematical Operators
"""


def test_add():
    assert str(PC.ops.ADD(1, 2)) == "1 + 2"
    assert str(PC.ops.ADD("cool", 2)) == "'cool' + 2"
    assert str(PC.ops.ADD(PC.ops.Column("test", int), 2)) == "test + 2"
    assert str(PC.ops.ADD(2, PC.ops.Column("test", int))) == "2 + test"


def test_subtract():
    assert str(PC.ops.SUB(1, 2)) == "1 - 2"
    assert str(PC.ops.SUB("cool", 2)) == "'cool' - 2"
    assert str(PC.ops.SUB(PC.ops.Column("test", int), 2)) == "test - 2"
    assert str(PC.ops.SUB(2, PC.ops.Column("test", int))) == "2 - test"


def test_multiply():
    assert str(PC.ops.MUL(left=1, right=2)) == "1 * 2"
    assert str(PC.ops.MUL(left="cool", right=2)) == "'cool' * 2"
    assert str(PC.ops.MUL(left=PC.ops.Column("test", int), right=2)) == "test * 2"

    assert str(PC.ops.MUL(left=2, right="cool")) == "2 * 'cool'"
    assert str(PC.ops.MUL(left=2, right=PC.ops.Column("test", int))) == "2 * test"


def test_divide():
    assert str(PC.ops.DIV(left=1, right=2)) == "1 / 2"
    assert str(PC.ops.DIV(left="cool", right=2)) == "'cool' / 2"
    assert str(PC.ops.DIV(left=PC.ops.Column("test", int), right=2)) == "test / 2"

    assert str(PC.ops.DIV(left=2, right="cool")) == "2 / 'cool'"
    assert str(PC.ops.DIV(left=2, right=PC.ops.Column("test", int))) == "2 / test"


def test_modulo():
    assert str(PC.ops.MOD(left=1, right=2)) == "1 % 2"
    assert str(PC.ops.MOD(left="cool", right=2)) == "'cool' % 2"
    assert str(PC.ops.MOD(left=PC.ops.Column("test", int), right=2)) == "test % 2"

    assert str(PC.ops.MOD(left=2, right="cool")) == "2 % 'cool'"
    assert str(PC.ops.MOD(left=2, right=PC.ops.Column("test", int))) == "2 % test"


def test_power():
    assert str(PC.ops.POW(left=1, right=2)) == "1 ^ 2"
    assert str(PC.ops.POW(left="cool", right=2)) == "'cool' ^ 2"
    assert str(PC.ops.POW(left=PC.ops.Column("test", int), right=2)) == "test ^ 2"

    assert str(PC.ops.POW(left=2, right="cool")) == "2 ^ 'cool'"
    assert str(PC.ops.POW(left=2, right=PC.ops.Column("test", int))) == "2 ^ test"


"""
Logical Operators
"""


def test_and():
    assert str(PC.ops.AND(left=1, right=2)) == "(1 AND 2)"
    assert str(PC.ops.AND(left="cool", right=2)) == "('cool' AND 2)"
    assert str(PC.ops.AND(left=PC.ops.Column("test", int), right=2)) == "(test AND 2)"
    assert str(PC.ops.AND(left=2, right="cool")) == "(2 AND 'cool')"
    assert str(PC.ops.AND(left=2, right=PC.ops.Column("test", int))) == "(2 AND test)"


def test_or():
    assert str(PC.ops.OR(left=1, right=2)) == "(1 OR 2)"
    assert str(PC.ops.OR(left="cool", right=2)) == "('cool' OR 2)"
    assert str(PC.ops.OR(left=PC.ops.Column("test", int), right=2)) == "(test OR 2)"
    assert str(PC.ops.OR(left=2, right="cool")) == "(2 OR 'cool')"
    assert str(PC.ops.OR(left=2, right=PC.ops.Column("test", int))) == "(2 OR test)"


def test_not():
    assert str(PC.ops.NOT(right=2)) == "~ 2"
    assert str(PC.ops.NOT(right="cool")) == "~ 'cool'"
    assert str(PC.ops.NOT(right=PC.ops.Column("test", int))) == "~ test"


"""
Comparison Operators
"""


def test_less_than():
    assert str(PC.ops.LT(left=1, right=2)) == "1 < 2"
    assert str(PC.ops.LT(left="cool", right=2)) == "'cool' < 2"
    assert str(PC.ops.LT(left=PC.ops.Column("test", int), right=2)) == "test < 2"
    assert str(PC.ops.LT(left=2, right="cool")) == "2 < 'cool'"
    assert str(PC.ops.LT(left=2, right=PC.ops.Column("test", int))) == "2 < test"


def test_greater_than():
    assert str(PC.ops.GT(left=1, right=2)) == "1 > 2"
    assert str(PC.ops.GT(left="cool", right=2)) == "'cool' > 2"
    assert str(PC.ops.GT(left=PC.ops.Column("test", int), right=2)) == "test > 2"
    assert str(PC.ops.GT(left=2, right="cool")) == "2 > 'cool'"
    assert str(PC.ops.GT(left=2, right=PC.ops.Column("test", int))) == "2 > test"


def test_less_equal():
    assert str(PC.ops.LE(left=1, right=2)) == "1 <= 2"
    assert str(PC.ops.LE(left="cool", right=2)) == "'cool' <= 2"
    assert str(PC.ops.LE(left=PC.ops.Column("test", int), right=2)) == "test <= 2"
    assert str(PC.ops.LE(left=2, right="cool")) == "2 <= 'cool'"
    assert str(PC.ops.LE(left=2, right=PC.ops.Column("test", int))) == "2 <= test"


def test_greater_equal():
    assert str(PC.ops.GE(left=1, right=2)) == "1 >= 2"
    assert str(PC.ops.GE(left="cool", right=2)) == "'cool' >= 2"
    assert str(PC.ops.GE(left=PC.ops.Column("test", int), right=2)) == "test >= 2"
    assert str(PC.ops.GE(left=2, right="cool")) == "2 >= 'cool'"
    assert str(PC.ops.GE(left=2, right=PC.ops.Column("test", int))) == "2 >= test"


def test_equal():
    assert str(PC.ops.EQ(left=1, right=2)) == "1 = 2"
    assert str(PC.ops.EQ(left="cool", right=2)) == "'cool' = 2"
    assert str(PC.ops.EQ(left=PC.ops.Column("test", int), right=2)) == "test = 2"
    assert str(PC.ops.EQ(left=2, right="cool")) == "2 = 'cool'"
    assert str(PC.ops.EQ(left=2, right=PC.ops.Column("test", int))) == "2 = test"


def test_not_equal():
    assert str(PC.ops.NE(left=1, right=2)) == "1 != 2"
    assert str(PC.ops.NE(left="cool", right=2)) == "'cool' != 2"
    assert str(PC.ops.NE(left=PC.ops.Column("test", int), right=2)) == "test != 2"
    assert str(PC.ops.NE(left=2, right="cool")) == "2 != 'cool'"
    assert str(PC.ops.NE(left=2, right=PC.ops.Column("test", int))) == "2 != test"
