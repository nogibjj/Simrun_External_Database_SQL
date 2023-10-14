"""
Transforms and Loads data into the local SQLite3 database
Example:
"Show Number", "Air Date", "Round", "Category", "Value", "Question", "Answer"
"""
import csv
from databricks import sql
import os


def load(dataset="data/Jeopardy.csv", db_name: str = "JeopardyDB.db", sql_conn=None):
    """Creating a remote databricks sql database with data from this csv."""

    with open(dataset, newline="") as csvfile:
        payload = list(csv.reader(csvfile, delimiter=","))

    column_names = [
        "Show Number",
        "Air Date",
        "Round",
        "Category",
        "Value",
        "Question",
        "Answer",
    ]

    if not sql_conn:
        # sql connection to databricks remote database
        conn = sql.connect(
            server_hostname="adb-6268784207758746.6.azuredatabricks.net",
            http_path="/sql/1.0/warehouses/c96a5e8d89f16478",
            access_token="dapi8194f2fd51cb10dae0c6e958138f0e53-3",
        )

        # print(f"Database {db_name} created.")
        # else:
        # conn = sql_conn

        c = conn.cursor()  # create a cursor
        # drop the table if it exists
        c.execute(f"DROP TABLE IF EXISTS {db_name}")
        print(f"Excuted: DROP TABLE IF EXISTS {db_name}")
        c.execute(f"CREATE TABLE IF NOT EXISTS {db_name} ({', '.join(column_names)})")
        print(f"Excuted: CREATE TABLE {db_name} ({', '.join(column_names)})")
        # insert the data from payload
        payload[1:] = [f"{tuple(row)}" for row in payload[1:]]
        c.execute(
            f"INSERT INTO {db_name} ({', '.join(column_names)}) VALUES {', '.join(payload[1:])}"
        )

        print(
            f"Excuted: INSERT INTO {db_name} VALUES"
            f"({', '.join(['?']*len(column_names))})"
        )

        conn.commit()
        conn.close()

    return conn


if __name__ == "__main__":
    load()