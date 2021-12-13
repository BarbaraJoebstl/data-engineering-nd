import configparser
import psycopg2
from sql_queries import copy_table_queries, insert_table_queries


def load_staging_tables(cur, conn):
    """
    loads data from files (in our case stored in S3) to the staging tables using the queries provided in sql_queries.py

    Parameters:
        cur: cursor object using the connection string
        conn: connection string for the database
    """

    print('---------------Loading and inserting data for the staging tables')
    for query in copy_table_queries:
        print('---Query: ' + query)
        cur.execute(query)
        conn.commit()


def insert_tables(cur, conn):
    """
    selects and transforms the data from the staging tables into the desired tables using the queries provided in sql_queries.py

    Parameters:
        cur: cursor object using the connection string
        conn: connection string for the database
    """

    print('---------------Inserting into star schema tables')
    for query in insert_table_queries:
        print('---Query: ' + query)
        cur.execute(query)
        conn.commit()


def main():
    """
    load data from S3 into staging tables (operative data), transform the dataa and load it into dimensional tables for analytics.
    Parameters:
      cur:
      conn:
    """

    config = configparser.ConfigParser()
    config.read('dwh.cfg')

    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(
        *config['CLUSTER'].values()))
    cur = conn.cursor()

    load_staging_tables(cur, conn)
    insert_tables(cur, conn)

    conn.close()


if __name__ == "__main__":
    main()