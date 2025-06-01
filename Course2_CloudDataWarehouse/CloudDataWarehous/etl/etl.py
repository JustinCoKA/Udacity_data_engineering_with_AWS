import configparser
import psycopg2
import os
# importlib is no longer needed

from sql.sql_queries import copy_table_queries, insert_table_queries # Import modified template list

def load_staging_tables(cur, conn, config): # Receive the latest config object from main
    print("etl.py: S3 to Staging Starting Loading")

    for template_query in copy_table_queries:
        formatted_query = ""
        s3_data_actual_region = 'us-west-2' # S3 data is fixed to us-west-2

        try:
            if "staging_events" in template_query:
                formatted_query = template_query.format(
                    s3_log_data=config.get('S3', 'LOG_DATA'),
                    iam_role_arn=config.get('IAM_ROLE', 'ARN'),
                    s3_log_jsonpath=config.get('S3', 'LOG_JSONPATH'),
                    s3_data_region=s3_data_actual_region # Use S3 bucket region
                )
            elif "staging_songs" in template_query:
                formatted_query = template_query.format(
                    s3_song_data=config.get('S3', 'SONG_DATA'),
                    iam_role_arn=config.get('IAM_ROLE', 'ARN'),
                    s3_data_region=s3_data_actual_region # Use S3 bucket region
                )
            else:
                print(f"WARNING: Unknown COPY template: {template_query[:50]}")
                continue

        except IndexError as e:
            print(
                f"ERROR: Formatting COPY query failed due to IndexError (likely positional placeholder {{}} issue): {e}")
            print(f"       Template being formatted: {template_query[:150]}...")
            continue

        print(f"Executing COPY query (first 100 chars): {formatted_query[:100]}...")

        try:
            cur.execute(formatted_query)
            conn.commit()
            print("Completed Load for one query")

        except Exception as e_query:
            print(f"ERROR executing COPY query: {formatted_query[:100]}... Error: {e_query}")
            conn.rollback()
    print("All Staging Loads Completed")

def insert_tables(cur, conn):
    # The config object can be passed if needed
    print("etl.py: Staging to Star Schema Starting Insertion")
    for query in insert_table_queries: # Assuming insert_table_queries don't require .format
        print(f"Executing INSERT query (first 100 chars): {query[:100]}...")
        try:
            cur.execute(query)
            conn.commit()
            print("Completed Insert for one query")
        except Exception as e_query:
            print(f"ERROR executing INSERT query: {query[:100]}... Error: {e_query}")
            conn.rollback()
    print("All Insertions Completed")

def main():
    config_etl = configparser.ConfigParser()
    config_path_etl = os.path.join(os.path.dirname(__file__), '..', 'config', 'dwh.cfg')
    config_etl.read(config_path_etl)

    # Reloading sql.sql_queries is no longer necessary as it's directly imported and
    # the config_etl object is passed to load_staging_tables for dynamic formatting.

    try:
        db_host = config_etl.get('CLUSTER', 'HOST')
        db_name = config_etl.get('CLUSTER', 'DB_NAME')
        db_user = config_etl.get('CLUSTER', 'DB_USER')
        db_password = config_etl.get('CLUSTER', 'DB_PASSWORD')
        db_port = config_etl.get('CLUSTER', 'DB_PORT')
    except configparser.NoOptionError as e:
        print(f"ERROR (etl.py): Required settings are missing in the [CLUSTER] section of dwh.cfg: {e}")
        return

    # Debug output
    print("--- Debug Info from etl.py ---")
    if 'CLUSTER' in config_etl:
        cluster_section = config_etl['CLUSTER']
        print(f"Original config['CLUSTER'].values(): {list(cluster_section.values())}")
        print(f"Explicitly retrieved HOST value: {db_host}")
    else:
        print("ERROR (etl.py): [CLUSTER] section is missing from dwh.cfg.")
    print("---------------------------")

    conn_string = f"host='{db_host}' dbname='{db_name}' user='{db_user}' password='{db_password}' port='{db_port}'"

    conn = None
    try:
        conn = psycopg2.connect(conn_string)
        cur = conn.cursor()
        print("SUCCESS etl.py: Connected to Redshift cluster.")

        # Pass the config_etl object to load_staging_tables
        load_staging_tables(cur, conn, config_etl)
        insert_tables(cur, conn) # Pass config_etl if insert_tables also needs it

        print("etl.py: ETL process Completed and disconnected")

    except psycopg2.OperationalError as e:
        print(f"ERROR (etl.py): An error occurred during Redshift connection or ETL operation: {e}")
    except ValueError as e:
        print(f"ERROR (etl.py): ValueError occurred (likely related to string.split('')): {e}")
    except Exception as e:
        print(f"ERROR (etl.py): An unexpected error occurred: {e}")
    finally:
        if conn:
            conn.close()

if __name__ == "__main__":
    main()