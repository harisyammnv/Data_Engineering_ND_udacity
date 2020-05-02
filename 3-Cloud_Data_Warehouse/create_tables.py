import configparser
import psycopg2
from sql_queries import create_table_queries, drop_table_queries
from tqdm import tqdm

def drop_tables(cur, conn):
    for query in tqdm(drop_table_queries):
        cur.execute(query)
        conn.commit()


def create_tables(cur, conn):
    for query in tqdm(create_table_queries):
        cur.execute(query)
        conn.commit()


def main():
    config = configparser.ConfigParser()
    config.read('dwh.cfg')

    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
    cur = conn.cursor()
    print('----- DROPPING EXISTING TABLES -----')
    drop_tables(cur, conn)
    print('----- DROPPING FINISHED -----')
    print('----- CREATING NEW TABLES -----')
    create_tables(cur, conn)

    conn.close()


if __name__ == "__main__":
    main()