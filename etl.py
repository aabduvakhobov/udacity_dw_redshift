import configparser
import psycopg2
from sql_queries import copy_table_queries, insert_table_queries
from time import time


def load_staging_tables(cur, conn):
    """
    Function iterates over load queries to load data from S3 to staging tables in Redshift
    :param cur: cursor object
    :param conn: cursor connection to db
    :return: None
    """
    for query in copy_table_queries:
        print(f"--LOADING STAGING: {query}")
        t0 = time()
        cur.execute(query)
        conn.commit()
        print(f"Loaded in {time()-t0}")


def insert_tables(cur, conn):
    """
    Function iterates over insert/etl queries and commits them to star schema db.
    :param cur: connection cursor object
    :param conn: connection object to db
    :return: None
    """
    try:
        for query in insert_table_queries:
            cur.execute(query)
            conn.commit()
    except Exception as e:
        print(e)


def main():
    """
    Function first gets all AWS credentials and connects to a Redshift.
    Then load and insert queries are executed respectively, after which connection with db is closed

    :return: None
    """
    config = configparser.ConfigParser()
    config.read('dwh.cfg')

    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
    cur = conn.cursor()

    load_staging_tables(cur, conn)
    insert_tables(cur, conn)

    conn.close()


if __name__ == "__main__":
    main()