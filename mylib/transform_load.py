"""
Transforms and Loads data into the Databricks SQL database
Example:
"Show Number", "Air Date", "Round", "Category", "Value", "Question", "Answer"
"""

import csv
from databricks import sql
import os
import pandas as pd
from dotenv import load_dotenv
from delta.tables import DeltaTable


def load(dataset="data/Jeopardy.csv", dataset2 = "data/Jeopardy2.csv""):
    """Loads the dataset from temporary csv from data/Jeopardy.csv and into a Databricks remote database"""

    df1 = pd.read_csv(dataset, delimiter=",", skiprows=1)
    df2 = pd.read_csv(dataset2, delimiter=",", skiprows=1)

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


        













#         for _, row in df1.iterrows():
#                 c.execute(f"INSERT INTO jeopardy VALUES {tuple(row)}")
#         c.execute("SHOW TABLES FROM default LIKE 'jeopardy2'")
#         results = c.fetchall()
#         if not results:
#             c.execute(
#                 """
#                 CREATE TABLE IF NOT EXISTS jeopardy2 (
#     "Show Number" TEXT,
#     "Air Date" TEXT,
#     "Round" TEXT,
#     "Category" TEXT,
#     "Value" TEXT,
#     "Question" TEXT,
#     "Answer" TEXT
# )
# """

#                 )
#             for _, row in df2.iterrows():
#                 c.execute(f"INSERT INTO jeopardy2 VALUES {tuple(row)}")
#         final_results = c.fetchall()
#         print(final_results)
#         c.close()
    # return "done"


if __name__ == "__main__":
    load()