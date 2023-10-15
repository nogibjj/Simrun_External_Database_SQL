"""Query the database"""

from databricks import sql
import os
from dotenv import load_dotenv


def query():
    """Testing the query function on the newly loaded database"""
    load_dotenv()
    host = os.getenv("DB_HOST")
    token = os.getenv("DB_TOKEN")
    path = os.getenv("DB_PATH")
    with sql.connect(
        server_hostname=host,
        access_token=token,
        http_path=path,
    ) as connection:
        c = connection.cursor()
        c.execute("""
            SELECT w1.Region, w1.Country, w1.year_2000, w1.year_2010, w1.year_2020, w2.year_2022, AVG(w2.year_2022) OVER(PARTITION BY w1.Region) as avg_year_2022
            FROM wages_1 w1
            JOIN wages_2 w2 ON w1.Country = w2.Country
            ORDER BY avg_year_2022 DESC, w1.Country
            LIMIT 5
        """)
        results = c.fetchall()
        columns = [desc[0] for desc in c.description]
        filtered_df = pd.DataFrame(results, columns=columns) 
        c.close()

    print(filtered_df)
    
    return filtered_df

if __name__ == "__main__":
    query()
