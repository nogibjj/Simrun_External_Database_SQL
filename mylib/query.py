"""Query the database"""

from databricks import sql
import os
from databricks import sql
import os


def query():
    """Query the database for all rows of the JeopardyDB table"""
    conn = sql.connect(
        server_hostname="adb-6268784207758746.6.azuredatabricks.net",
        http_path="/sql/1.0/warehouses/c96a5e8d89f16478",
        access_token="dapi8194f2fd51cb10dae0c6e958138f0e53-3",
    )

    cursor = conn.cursor()
    cursor.execute("SELECT * from range(10)")
    rows = cursor.fetchall()

    return print(rows)


if __name__ == "__main__":
    query()
