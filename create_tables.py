import configparser
import psycopg2
from sql_queries import create_table_queries, drop_table_queries


def drop_tables(cur, conn):
    '''
    Function loops through all tables defined in drop_table_queries from sql_queries.py
    And drops each table if it exists
    '''
    for query in drop_table_queries:
        print("Dropping tables - Executing: " + query)
        cur.execute(query)
        conn.commit()


def create_tables(cur, conn):
    '''
    Function loops through all tables defined in create_tables_queries from sql_queries.py
    And creates each table listed there
    '''
    for query in create_table_queries:
        print("Creating tables - Executing: " + query)
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
    
    print("Dropping tables")
    drop_tables(cur, conn)
    print("Creating tables")
    create_tables(cur, conn)
    print("Tables created")

    conn.close()
    print("Connection closed")


if __name__ == "__main__":
    main()