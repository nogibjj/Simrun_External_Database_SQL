"""Query the database"""

import sqlite3


def query(the_query):
    """Query the database for all rows of the JeopardyDB table"""
    conn = sqlite3.connect("JeopardyDB.db")
    cursor = conn.cursor()
    cursor.execute(the_query)
    rows = cursor.fetchall()
    print(rows)
    conn.close()
    return rows
