"""
Test goes here

"""

# fix table 1 and query !!!!

from databricks import sql
import os

from mylib import transform_load


def test_query():
    # """Test the query function"""
    # assert query("SELECT COUNT(*) FROM table1") == [(36556,)]

    pass


def test_extract():
    pass


def test_transform_load():
    with sql.connect(
        server_hostname=os.getenv("DATABRICKS_SERVER_HOSTNAME"),
        http_path=os.getenv("DATABRICKS_HTTP_PATH"),
        access_token=os.getenv("DATABRICKS_TOKEN"),
    ) as connection:

        with connection.cursor() as cursor:
            cursor.execute("SELECT * FROM table1 LIMIT 2")
            result = cursor.fetchall()

            for row in result:
                print(row)
