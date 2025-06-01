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

    print("--- Debug information from create_tables.py ---")
    cluster_config = config['CLUSTER']
    print(f"Original config['CLUSTER'].values(): {list(cluster_config.values())}") # Confirm order

    # Explicitly get values
    db_host = cluster_config.get('HOST')
    db_name = cluster_config.get('DB_NAME')
    db_user = cluster_config.get('DB_USER')
    db_password = cluster_config.get('DB_PASSWORD') # Avoid printing passwords in actual logs!
    db_port = cluster_config.get('DB_PORT')

    print(f"HOST: {db_host}")
    print(f"DB_NAME: {db_name}")
    print(f"DB_USER: {db_user}")
    print(f"DB_PORT: {db_port}")
    print("--------------------------------------")

    # For clarity and safety, use explicit variables and improved f-string formatting
    # Consider adding single quotes around values in case of special characters in passwords (psycopg2 usually handles this)
    conn_string = f"host='{db_host}' dbname='{db_name}' user='{db_user}' password='{db_password}' port='{db_port}'"
    print(f"Attempting to connect with the following connection string: {conn_string}")

    conn = psycopg2.connect(conn_string) # Use the modified connection string
    cur = conn.cursor()

    print("🔄 Dropping tables...")
    drop_tables(cur, conn)

    print("🧱 Creating tables...")
    create_tables(cur, conn)

    conn.close()
    print("✅ Table operations completed.") # Modified message (instead of "deleted" message)

if __name__ == "__main__":
    main()