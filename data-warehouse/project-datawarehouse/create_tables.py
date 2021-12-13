import configparser
import psycopg2
from sql_queries import create_table_queries, drop_table_queries


def drop_tables(cur, conn):
    """
    drops existing tables

    Parameters:
        cur: cursor object using the connection string
        conn: connection string for the database
    """
    for query in drop_table_queries:
        cur.execute(query)
        conn.commit()


def create_tables(cur, conn):
    """
    creates tables

    Parameters:
        cur: cursor object using the connection string
        conn: connection string for the database
    """
    for query in create_table_queries:
        cur.execute(query)
        conn.commit()


def main():
    """
    gets all configuration information
    connects to the database
    drops all existing tables
    creates new tables
    """

    config = configparser.ConfigParser()
    config.read('dwh.cfg')

    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(
        *config['CLUSTER'].values()))
    cur = conn.cursor()

    drop_tables(cur, conn)
    create_tables(cur, conn)

    conn.close()


if __name__ == "__main__":
    main()