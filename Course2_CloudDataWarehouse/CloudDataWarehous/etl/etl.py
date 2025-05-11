import configparser
import psycopg2
import os
from sql.sql_queries import copy_table_queries, insert_table_queries


def load_staging_tables(cur, conn):
    for query in copy_table_queries:
        print(f"executing query: {query.split('')[1]}")
        cur.execute(query)
        conn.commit()
        print("Completed Load")


def insert_tables(cur, conn):
    for query in insert_table_queries:
        print(f"Inserting into table: {query.split('')[2]}")
        cur.execute(query)
        conn.commit()
        print("Completed Insert")


def main():

    config = configparser.ConfigParser()
    config_path = os.path.join(os.path.dirname(__file__), '..', 'config', 'dwh.cfg')
    config.read(config_path)

    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
    cur = conn.cursor()

    print(" S3 to Staging Starting Loading")
    load_staging_tables(cur, conn)

    print(" Staging to Star Schema Starting Insertion")
    insert_tables(cur, conn)

    conn.close()
    print("ETL process Completed and disconnected")


if __name__ == "__main__":
    main()