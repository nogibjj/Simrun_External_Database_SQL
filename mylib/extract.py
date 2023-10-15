"""
Extract a dataset from a URL like Kaggle or data.gov.
JSON or CSV formats tend to work well

Jeopardy dataset
"""
import requests
import pandas as pd


def extract(
    url="https://raw.githubusercontent.com/nogibjj/Simrun_sqlite-lab/main/data/Jeopardy.csv",
    file_path="data/Jeopardy.csv",
):
    """ "Extract a url to a file path"""
    with requests.get(url) as r:
        with open(file_path, "wb") as f:
            f.write(r.content)

    df1 = pd.read_csv(file_path, delimiter=",", skiprows=1)
    print(df1.head())
    return df1, file_path


extract()