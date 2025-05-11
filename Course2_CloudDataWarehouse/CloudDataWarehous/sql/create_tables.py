import configparser
import psycopg2
import os
from .sql_queries import create_table_queries, drop_table_queries


def drop_tables(cur, conn):
    for query in drop_table_queries:
        cur.execute(query)
        conn.commit()


def create_tables(cur, conn):
    for query in create_table_queries:
        cur.execute(query)
        conn.commit()


def main():
    config = configparser.ConfigParser()
    config_path = os.path.join(os.path.dirname(__file__), '..', 'config', 'dwh.cfg')
    config.read(config_path)

    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
    cur = conn.cursor()

    print("🔄 Deleting tables...")
    drop_tables(cur, conn)

    print("🧱 Creating tables...")
    create_tables(cur, conn)

    conn.close()
    print("✅ Tables have been removed...")


if __name__ == "__main__":
    main()