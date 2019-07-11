import configparser
import psycopg2
from sql_queries import copy_table_queries, insert_table_queries


def load_staging_tables(cur, conn):
    """ Loads data from S3 buckets
    to Redshift staging tables"""
    for query in copy_table_queries:
        cur.execute(query)
        conn.commit()


def insert_tables(cur, conn):
    """ Inserts data from staged tables
    into OLAP tables"""
    for query in insert_table_queries:
        cur.execute(query)
        conn.commit()


def main():
    config = configparser.ConfigParser()
    config.read('dwh.cfg')

    print('Connecting to Redshift Database')
    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
    print('Connected to Redshift Database')
    cur = conn.cursor()

    print('Loading redshift staging tables...')
    load_staging_tables(cur, conn)
    print('Finished loading staging tables!')

    print('Running transformation and loading from staging to OLAP tables...')
    insert_tables(cur, conn)
    print('finished transforming and loading from staging to OLAP tables!')

    conn.close()
    print('Successfully completed ETL!')


if __name__ == "__main__":
    main()
