"""
Test goes here

"""

from mylib.query import query


def test_query():
    """Test the query function"""
    assert query("SELECT COUNT(*) FROM table1") == [(36556,)]

    pass


def test_extract():
    pass


def test_transform_load():
    pass
