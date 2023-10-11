"""this function is used to update the Databricks sql database.""""

from databricks import sql
import os


def update_db(sql_conn=None, 
              database:str="JeopardyDB",
              the_query:str='')->None:
    """Update the database"""
    if not sql_conn:
        conn = sql.connect(
            server_hostname="adb-6268784207758746.6.azuredatabricks.net",
            http_path="/sql/1.0/warehouses/c96a5e8d89f16478",
            access_token="dapi8194f2fd51cb10dae0c6e958138f0e53-3",
        )

        print(f"Database {database} Connected to.")

    else:
        conn = sql_conn
    
    cursor = conn.cursor()

    if the_query == '':
        cursor.execute("UPDATE JeopardyDB SET semantic_tree_name = 'Simrun'"\
                    " WHERE column_names = '8'")
    else:
        cursor.execute(the_query)
    
    conn.commit()

    print("Database has been updated")


if __name__ == '__main__':
    connection = sql.connect("Jeopardy.db")
    update_db(connection)
    connection.close()