import pandasdb.functools as ft
import numpy as np


def test_is_in(postgres_db):
    user = postgres_db.Tables.user

    is_in_age = ft.is_in(user.Columns.age, [23, 24, 25])
    assert str(is_in_age).replace("\n", " ").strip() == "test.user.age IN (23, 24, 25)"

    is_in_age = ft.is_in(user.Columns.age, tuple([24, 23, 25]))
    assert str(is_in_age).replace("\n", " ").strip() == "test.user.age IN (24, 23, 25)"

    data = {23: True, 24: True, 25: True}
    is_in_age = ft.is_in(user.Columns.age, data)
    assert str(is_in_age).replace("\n", " ").strip() == "test.user.age IN (23, 24, 25)"

    is_in_age = ft.is_in(user.Columns.age, np.array([24, 23, 25]))
    assert str(is_in_age).replace("\n", " ").strip() == "test.user.age IN (24, 23, 25)"

def test_not_in(postgres_db):
    user = postgres_db.Tables.user

    not_in_age = ft.not_in(user.Columns.age, [23, 24, 25])
    assert str(not_in_age).replace("\n", " ").strip() == "test.user.age NOT IN (23, 24, 25)"

    not_in_age = ft.not_in(user.Columns.age, tuple([24, 23, 25]))
    assert str(not_in_age).replace("\n", " ").strip() == "test.user.age NOT IN (24, 23, 25)"

    data = {23: True, 24: True, 25: True}
    not_in_age = ft.not_in(user.Columns.age, data)
    assert str(not_in_age).replace("\n", " ").strip() == "test.user.age NOT IN (23, 24, 25)"

    not_in_age = ft.not_in(user.Columns.age, np.array([24, 23, 25]))
    assert str(not_in_age).replace("\n", " ").strip() == "test.user.age NOT IN (24, 23, 25)"
