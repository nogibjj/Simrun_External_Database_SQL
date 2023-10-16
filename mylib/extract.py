"""
Extract a dataset from a URL like Kaggle or data.gov.
JSON or CSV formats tend to work well

Jeopardy dataset
"""
import requests
import pandas as pd


def extract(
    url_1="https://raw.githubusercontent.com/nogibjj/Simrun_sqlite-lab/main/data/Jeopardy.csv",
    file_path="data/Jeopardy.csv", url_2 = "https://github.com/nogibjj/Simrun_External_Database_SQL/raw/main/data/Jeopardy_2.csv",
    file_path_2 = "data/Jeopardy_2.csv"
):
    """ "Extract a url to a file path"""
    with requests.get(url_1) as r:
        with open(file_path, "wb") as f:
            f.write(r.content)

    df1 = pd.read_csv(file_path, delimiter=",", skiprows=1)
    print(df1.head())

    with requests.get(url_2) as r:
        with open(file_path_2, "wb") as f:
            f.write(r.content)
    
    df2 = pd.read_csv(file_path_2, delimiter=",", skiprows=1)
    print(df2.head())

    return df1, df2


extract()