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
        WITH aggregated_data AS (
    SELECT
        j1.Category,
        SUM(CAST(j1.Value AS INT)) AS TotalValue_j1,
        SUM(CAST(COALESCE(j2.Value, '0') AS INT)) AS TotalValue_j2
    FROM
        jeopardy j1
    LEFT JOIN
        jeopardy2 j2 ON j1.Category = j2.Category
    GROUP BY
        j1.Category
)

SELECT
    Category,
    TotalValue_j1,
    TotalValue_j2,
    TotalValue_j1 + TotalValue_j2 AS CombinedTotalValue
FROM
    aggregated_data
ORDER BY
    CombinedTotalValue DESC;


        """)
        results = c.fetchall()
        columns = [desc[0] for desc in c.description]
        filtered_df = pd.DataFrame(results, columns=columns) 
        c.close()

    print(filtered_df)
    
    return filtered_df

if __name__ == "__main__":
    query()
