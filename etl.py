import configparser
import psycopg2
from sql_queries import copy_table_queries, insert_table_queries


def load_staging_tables(cur, conn):
    '''
    Loads the staging tables for events and songs from S3 bucket with queries from sql_queries.py
    '''
    for query in copy_table_queries:
        print('Running ' + query)
        cur.execute(query)
        conn.commit()


def insert_tables(cur, conn):
    '''
    Inserts data into the created tables from the staging tables with queries from sql_queries.py
    '''
    for query in insert_table_queries:
        print('Running ' + query)
        cur.execute(query)
        conn.commit()


def main():
    # reading the config at dwh.cfg
    config = configparser.ConfigParser()
    config.read('dwh.cfg')

    # connects to database 
    # documentation here: https://www.psycopg.org/docs/module.html?highlight=connect#psycopg2.connect 
    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
    cur = conn.cursor()
    print("Connected to database")
    
    print("Loading staging tables")
    load_staging_tables(cur, conn)
    print("Staging tables have been loaded from S3")
    print("Inserting data in facts and dimension tables")
    insert_tables(cur, conn)
    print("Data has been inserted")

    conn.close()
    print("Connection closed")


if __name__ == "__main__":
    main()