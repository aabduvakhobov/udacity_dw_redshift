import configparser
import psycopg2
from sql_queries import copy_table_queries, insert_table_queries
from time import time


def load_staging_tables(cur, conn):
    for query in copy_table_queries:
        print(f"--LOADING STAGING: {query}")
        t0 = time()
        cur.execute(query)
        conn.commit()
        print(f"Loaded in {time()-t0}")


def insert_tables(cur, conn):
    try:
        for query in insert_table_queries:
            print(query)
            cur.execute(query)
            conn.commit()
    except Exception as e:
        print(e)



def main():
    config = configparser.ConfigParser()
    config.read('dwh.cfg')

    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
    cur = conn.cursor()

    load_staging_tables(cur, conn)
    insert_tables(cur, conn)

    conn.close()


if __name__ == "__main__":
    main()