import pandas as pd
from sqlalchemy import create_engine
from sqlalchemy import exc


def create_db_connection(host_name, user_name, user_password, db_name):
    conn = None
    try:
        conn = create_engine(f'mysql+pymysql://{user_name}:{user_password}@{host_name}/{db_name}').connect()
        print("MySQL database connection successfull")
    except exc.SQLAlchemyError as err:
        print(f"Error: {err}")
    return conn


def read_query(conn, query):
    result = None
    try:
        fetch_data = conn.execute(query).fetchall()
        result = pd.DataFrame(fetch_data)
        return result
    except exc.SQLAlchemyError as err:
        print(f"Error: {err}")
        
        

def create_db(host_name, user_name, user_password, db_name):
    try:
        con = create_engine(f'mysql+pymysql://{user_name}:{user_password}@{host_name}')
        con.execute(f"create database if not exists {db_name}")
        print(f"{db_name} berhasil dibuat.")
    except exc.SQLAlchemyError as err:
        print(f"Error: {err}")
        
        
def execute_multi_query(conn, query):
    try:
        conn.execute(query)
    except exc.SQLAlchemyError as err:
        print(f"Error: {err}")