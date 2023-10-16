"""
ETL-Query script
"""
import fire
from mylib.extract import extract
from mylib.transform_load import load, load2
from mylib.query import query


def main(the_query):
    """Run the ETL process"""
    # Extract
    print("Extracting data...")
    extract()

    # Transform and load
    print("Transforming data...")
    # load(the_query)
    load2(the_query)

    # Query
    print("Querying data...")
    query(the_query)


if __name__ == "__main__":
    # load("SELECT COUNT(*) FROM table1")
    # fire.Fire(main)
    main("SELECT COUNT(*) FROM jeopardy2")
    # query("SELECT COUNT(*) FROM table1")
