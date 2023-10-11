"""This file contains information on how to delete tabluar data from Databricks sql database.""" ""

from databricks import sql
import os


def drop(table_name: str = "table1", sql_conn=None, condition="column_names = '8'"):

    """function to drop data based on condition and table"""

    if not sql_conn:
        conn = sql.connect(
            server_hostname="adb-6268784207758746.6.azuredatabricks.net",
            http_path="/sql/1.0/warehouses/c96a5e8d89f16478",
            access_token="dapi8194f2fd51cb10dae0c6e958138f0e53-3",
        )

    else:
        conn = sql_conn

    cursor = conn.cursor()
    cursor.execute(f"DELETE FROM {table_name} WHERE {condition};")
    conn.commit()
    conn.close()

    print(f"Table {table_name} dropped from Databricks sql database.")
