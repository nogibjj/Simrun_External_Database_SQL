"""Query the database"""

from databricks import sql
import pandas as pd


def query():
    """Testing the query function on the newly loaded database"""
    DB_TOKEN = 'dapie73a0bd9ca8aa631b7e8e50ee667473e-3'
    DB_HOST = 'adb-6268784207758746.6.azuredatabricks.net'
    DB_PORT = 443
    DEBUG= True
    DB_PATH = '/sql/1.0/warehouses/c96a5e8d89f16478'
    
    with sql.connect(
        server_hostname=DB_HOST,
        access_token=DB_TOKEN,
        http_path=DB_PATH,
    ) as connection:
        c = connection.cursor()
        c.execute("""
            SELECT
    j.Category,
    COUNT(j.Round) AS Round_Count,
    SUM(j.Value) AS Total_Value
FROM
    jeopardy j
JOIN
    jeopardy2 j2
ON
    j.Round = j2.Round
WHERE
    j.Value = '200'
GROUP BY
    j.Category
ORDER BY
    Total_Value DESC;
        """)
        results = c.fetchall()
        columns = [desc[0] for desc in c.description]
        filtered_df = pd.DataFrame(results, columns=columns) 
        c.close()

    print(filtered_df)
    
    return filtered_df

if __name__ == "__main__":
    query()
