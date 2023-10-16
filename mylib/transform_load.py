"""
Transforms and Loads data into the Databricks SQL database
Example:
"Show Number", "Air Date", "Round", "Category", "Value", "Question", "Answer"
"""

import csv
from multiprocessing import connection
from databricks import sql
import pandas as pd


def load(dataset="data/Jeopardy.csv"):
    """Loads the dataset from temporary csv from data/Jeopardy.csv and into a Databricks remote database"""

    df1 = pd.read_csv(dataset, delimiter=",", skiprows=1)

    DB_TOKEN = 'dapie73a0bd9ca8aa631b7e8e50ee667473e-3'
    DB_HOST = 'adb-6268784207758746.6.azuredatabricks.net'
    DB_PORT = 443
    DEBUG= True
    DB_PATH = '/sql/1.0/warehouses/c96a5e8d89f16478'

    with sql.connect(
        server_hostname = DB_HOST,
        http_path=DB_PATH,
        access_token=DB_TOKEN,
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
                        Show INT,
                        Air VARCHAR(255),
                        Round VARCHAR(255),
                        Category VARCHAR(255),
                        Value VARCHAR(255),
                        Question VARCHAR(255),
                        Answer VARCHAR(255)
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
                convert = (_,) + tuple(new_row)
                print(convert)
                c.execute(f"INSERT INTO jeopardy VALUES {convert}")
        results = c.fetchall()
        print(results)
        c.close()
    return "done"

def load2(dataset2 = "data/Jeopardy2.csv"):
    df2 = pd.read_csv(dataset2, delimiter=",", skiprows=1)
    DB_TOKEN = 'dapie73a0bd9ca8aa631b7e8e50ee667473e-3'
    DB_HOST = 'adb-6268784207758746.6.azuredatabricks.net'
    DB_PORT = 443
    DEBUG= True
    DB_PATH = '/sql/1.0/warehouses/c96a5e8d89f16478'

    with sql.connect(
        server_hostname = DB_HOST,
        http_path=DB_PATH,
        access_token=DB_TOKEN,
    ) as connection:
        c = connection.cursor()
        c.execute("SHOW TABLES FROM default")
        tables = c.fetchall()
        for table in tables:
            print(table)
            table_name = table.tableName
            c.execute(f"DROP TABLE IF EXISTS {table_name}")

        c.execute("SHOW TABLES FROM default LIKE 'jeopardy2'")
        
        result = c.fetchall()
        print(f"tables : {result}")
        if not result:
            c.execute(
                """ 
                    CREATE TABLE IF NOT EXISTS jeopardy2 (
                        Show INT,
                        Air VARCHAR(255),
                        Round VARCHAR(255),
                        Category VARCHAR(255),
                        Value VARCHAR(255),
                        Question VARCHAR(255),
                        Answer VARCHAR(255)
                        )
                    """
                )
            
            df2.fillna(0, inplace=True)
            for _, row in df2.iterrows():
                new_row = []
                for r in row:
                    if r is None:
                        new_row.append("Null")
                    else:
                        new_row.append(r)
                convert = (_,) + tuple(new_row)
                print(convert)
                c.execute(f"INSERT INTO jeopardy2 VALUES {convert}")
        results = c.fetchall()
        print(results)
        c.close()
    return "double done"

if __name__ == "__main__":
    load()
    load2()