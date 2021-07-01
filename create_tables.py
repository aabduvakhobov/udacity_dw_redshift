import configparser
import psycopg2
from sql_queries import create_table_queries, drop_table_queries


def drop_tables(cur, conn):
    """
    description: function iterates over drop table queries and executes them via cursor object

    :param cur:cursor object
    :param conn:
    :return: None
    """
    for query in drop_table_queries:
        cur.execute(query)
        conn.commit()


def create_tables(cur, conn):
    """
    description: function iterates over create table queries and creates in the shown table
    :param cur: cursor object
    :param conn: cursor connection to db
    :return:
    """
    try:
        for query in create_table_queries:
            cur.execute(query)
            conn.commit()
    except Exception as e:
        print(e)


def main():
    """
    Function first gets all AWS credentials and connects to a Redshift.
    Then drop and create queries are executed respectively, after which connection with db is closed
    :return: None
    """
    config = configparser.ConfigParser()
    config.read('dwh.cfg')

    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
    cur = conn.cursor()

    drop_tables(cur, conn)
    create_tables(cur, conn)
    print()

    conn.close()


if __name__ == "__main__":
    main()