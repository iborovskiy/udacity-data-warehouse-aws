import configparser
import psycopg2
from sql_queries import copy_table_queries, insert_table_queries


def load_staging_tables(cur, conn):
    """
    Loads data from S3 to staging tables on Redshift
    by executing corresponding SQL Query for each staging table.
    """
    for query in copy_table_queries:
        cur.execute(query)
        conn.commit()


def insert_tables(cur, conn):
    """
    Loads data from staging tables to analytics (fact & dimension) tables on Redshift
    by executing corresponding SQL Query for each table.
    """
    for query in insert_table_queries:
        cur.execute(query)
        conn.commit()


def main():
    """
    - Loads Redshift cluster connection configuration from <dwh.cfg>.

    - Establishes connection with the Redshift database and gets cursor to it.

    - Loads data from S3 to staging tables on Redshift.

    - Loads data from staging tables to analytics tables on Redshift.

    - Finally, closes the connection.
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