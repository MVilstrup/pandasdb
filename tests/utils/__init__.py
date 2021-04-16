import pandasdb.libraries.utils as utils


def test_iterable():
    assert utils.iterable([])
    assert utils.iterable(set())
    assert utils.iterable(tuple())
    assert utils.iterable({})

    assert not utils.iterable(str)
    assert not utils.iterable(int)
    assert not utils.iterable(float)
    assert not utils.iterable(bool)


def test_camel_to_snake():
    assert utils.camel_to_snake("CoolStuff") == "cool_stuff"


def test_string_to_python_attr():
    assert utils.string_to_python_attr("CoolStuff and stuff") == "cool_stuff_and_stuff"


def test_type_check():
    assert utils.type_check(int, int)
    assert utils.type_check(1, int)
    assert not utils.type_check(1., int)