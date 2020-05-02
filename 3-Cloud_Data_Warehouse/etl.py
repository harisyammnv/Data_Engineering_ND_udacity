import configparser
import psycopg2
from sql_queries import copy_table_queries, insert_table_queries
from tqdm import tqdm

def load_staging_tables(cur, conn):
    for query in tqdm(copy_table_queries):
        cur.execute(query)
        conn.commit()


def insert_tables(cur, conn):
    for query in tqdm(insert_table_queries):
        cur.execute(query)
        conn.commit()


def main():
    config = configparser.ConfigParser()
    config.read('dwh.cfg')

    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
    cur = conn.cursor()
    print('----- LOADING FROM S3 TO STAGING TABLES -----')
    load_staging_tables(cur, conn)
    print('----- FINISHED LOADING INTO STAGING TABLES -----')
    insert_tables(cur, conn)
    print('----- REDSHIFT INSERTION FINISHED -----')
    conn.close()


if __name__ == "__main__":
    main()