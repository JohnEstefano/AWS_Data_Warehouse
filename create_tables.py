import configparser
import psycopg2
from sql_queries import create_table_queries, drop_table_queries



def drop_tables(cur, conn):
    """drop current,if any, database tables listed on drop_table_queries
    """
    for query in drop_table_queries:
        cur.execute(query)
        conn.commit()


def create_tables(cur, conn):
    """create database tables listed on create_table_queries
    """
    for query in create_table_queries:
        cur.execute(query)
        conn.commit()


def main():
    config = configparser.ConfigParser()
    config.read('dwh.cfg')

    #connect to database
    print('Atempting to connect to Redshift database...')
    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
    print('Connected to Redshift database')
    cur = conn.cursor()

    #drop existing tables
    print('dropping existing tables')
    drop_tables(cur, conn)

    #create tables
    print('creating tables')
    create_tables(cur, conn)

    conn.close()
    print('tables created')


if __name__ == "__main__":
    main()
