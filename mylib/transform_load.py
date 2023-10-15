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


def load(dataset_1="data/Jeopardy.csv", dataset_2="data/Jeopardy2.csv"):
    """Loads the dataset from temporary csv from data/Jeopardy.csv and into a Databricks SQL database"""

    df1 = pd.read_csv(dataset_1, delimiter=",", skiprows=1)
    df2 = pd.read_csv(dataset_2, delimiter=",", skiprows=1)

    load_dotenv(dotenv_path='keys.env')
    host = os.getenv("DB_HOST")
    token = os.getenv("DB_TOKEN")
    path = os.getenv("DB_PATH")

    with sql.connect(
        server_hostname=host,
        http_path=path,
        access_token=token,
    ) as connection:
        c = connection.cursor()

        # Create jeopardy table
        c.execute('''
    CREATE TABLE IF NOT EXISTS jeopardy (
        `Show Number` STRING,
        `Air Date` STRING,
        `Round` STRING,
        `Category` STRING,
        `Value` STRING,
        `Question` STRING,
        `Answer` STRING
    )
''')
# Assuming 'spark' is your SparkSession
delta = DeltaTable.forPath(spark, "path_to_table")
delta.upgradeTableProtocol(1, 3)  # Upgrades to readerVersion=1, writerVersion=3

print("jeopardy table created successfully.")


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