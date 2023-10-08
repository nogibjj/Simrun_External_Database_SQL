"""
Transforms and Loads data into the local SQLite3 database
Example:
"Show Number", "Air Date", "Round", "Category", "Value", "Question", "Answer"
"""
import sqlite3
import pandas as pd


def load(the_query):
    dataset = "data/Jeopardy.csv"
    df = pd.read_csv(dataset)
    database_path = "/workspaces/Simrun_sqlite-lab/JeopardyDB.db"
    connect = sqlite3.connect(database_path)
   
    df.to_sql("table1", connect, if_exists="replace", index=False)
    cursor = connect.cursor()
    cursor.execute(the_query)
    connect.commit()
    connect.close()
  
