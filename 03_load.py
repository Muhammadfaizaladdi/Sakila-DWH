from src.wr_data import insert_data
from src.sql_con import create_db_connection
from query.query_relasi import *
from src.sql_con import execute_multi_query
from src.yaml_loader import yaml_loader

file_path = "query/sql_params.yaml"

mysql_params = yaml_loader(file_path)

host=mysql_params["host"]
user=mysql_params["user"]
password=mysql_params["password"]
db=mysql_params["db"]

data_to_insert = {
                    "customer":{"filename" : "customer_transform.csv",
                              "table_name" : "customer_dim"
                             },
                    "film":{"filename" : "film_extract.csv",
                              "table_name" : "film_dim"
                             },
                    "staff":{"filename" : "staff_extract.csv",
                              "table_name" : "staff_dim"
                             },
                    "store":{"filename" : "store_extract.csv",
                              "table_name" : "store_dim"
                             },
                    "rental":{"filename" : "rental_transform.csv",
                              "table_name" : "rental_fact"
                             }
                }
    

#Create DB Connection
con = create_db_connection(host, user, password, db)


# Insert Data
for data, value in data_to_insert.items():
    insert_data(value["filename"], con, value["table_name"])
    

list_query = [alter_cust, alter_film, alter_store, alter_staff, alter_rental]
# Creata table relation
for query in list_query:
    execute_multi_query(con, query)
print("alter table success")