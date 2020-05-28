# Paul Wiper - 28/05/2020

import psycopg2
from psycopg2 import sql
from psycopg2.extensions import AsIs
import sys, os
import numpy as np
import pandas as pd
import psql_creds as creds
import pandas.io.sql as psql
import string


def psql_query(cursor):    
    
    test_string = 'searchingforthisstring'
    tablenames = []
    
    #the tablenames returned are not clean strings
    bad_chars = '()\',\"'
    
    #get tablenames
    try:
        cursor.execute("""SELECT table_name FROM information_schema.tables
        WHERE table_schema = 'public';""")
    except psycopg2.errors.LockNotAvailable:
        locked = True
    
    # grab and sanitize all tablenames
    for table in cursor.fetchall():
        table = str(table)
        trantab = table.maketrans('', '', bad_chars)
        table = table.translate(trantab)
        tablenames.append(table)
        
    # sql.SQL cant escape brackets so I guess well have to declare these strings first
    cid = '.id'
    start = 'INNER JOIN '
    mid = ' ON c.id = '
    bracket = '('
    cbracket = ')'

    query_1 = sql.SQL("SELECT {coltable} FROM {open_br}pkey_table AS c {table} WHERE cn LIKE '{cn}' limit 10").format(
        table = sql.SQL(' ').join(
        (sql.Identifier(start+t+mid+t+cid+cbracket) for t in cert_tables),
    ),
    coltable = sql.SQL(', ').join( 
        (sql.Identifier(t+cid) for t in cert_tables),
    ),
    cn = sql.Identifier(p_url),
    open_br = sql.Identifier( (bracket * len(cert_tables)))
    )
        fields=sql.Identifier('id'))
    

def psql_conn():
    # # Set up a connection to the postgres server.
    conn_string = "host="+ creds.PGHOST +" port="+ "8080" +" dbname="+ creds.PGDATABASE +" user=" + creds.PGUSER \
    +" password="+ creds.PGPASSWORD
    try:
        conn=psycopg2.connect(conn_string)
        print('\n', 'Connected to db')
    except:
        print('an exception occured when connecting to db')
    # Create a cursor object
    cursor = conn.cursor()
    return cursor

def(main):
    cursor = psql_conn()
    
main()
