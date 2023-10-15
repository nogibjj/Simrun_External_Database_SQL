"""
Transforms and Loads data into the local SQLite3 database
Example:
"Show Number", "Air Date", "Round", "Category", "Value", "Question", "Answer"
"""
import csv
from databricks import sql
import os
import pandas as pd
from dotenv import load_dotenv


def load(dataset="data/Jeopardy.csv"):
    """Loads the dataset from temporary csv from data/Jeopardy.csv and into a Databricks remote database"""

    df1 = pd.read_csv(dataset, delimiter=",", skiprows=1)
    load_dotenv()
    host = os.getenv("DB_HOST")
    token = os.getenv("DB_TOKEN")
    path = os.getenv("DB_PATH")

    with sql.connect(
        server_hostname = host,
        http_path=path,
        access_token=token,
    ) as connection:
        c = connection.cursor()
        c.execute("SHOW TABLES FROM default")
        tables = c.fetchall()

        for table in tables:
            print(table)
            table_name = table.tableName
            c.execute(f"DROP TABLE IF EXISTS {table_name}")

        c.execute("SHOW TABLES FROM default LIKE 'jeopardy'")
        result = c.fetchall()
        print(f"tables : {result}")
        if not result:
            c.execute(
                """ 
                    CREATE TABLE IF NOT EXISTS jeopardy (
                        Show Number,
                        Air Date,
                        Round,
                        Category,
                        Value,
                        Question,
                        Answer
                        )
                    """
                )
            for _, row in df1.iterrows():
                new_row = []
                for r in row:
                    if r is None:
                        new_row.append("Null")
                    else:
                        new_row.append(r)
                print(new_row)
                convert = (_,) + tuple(new_row)
                c.execute(f"INSERT INTO jeopardy VALUES {convert}")
        full_results = c.fetchall()
        print(full_results)
        c.close()
    return "done"


if __name__ == "__main__":
    load()