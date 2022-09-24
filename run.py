from src.sql_con import *
from src.wr_data import *
from query.query_read import *
from query.query_relasi import *
from src.yaml_loader import yaml_loader

# Load parameters
params_file_path = "query/sql_params.yaml"

mysql_params = yaml_loader(params_file_path)
HOST=mysql_params["host"]
USER=mysql_params["user"]
PASSWORD=mysql_params["password"]
DB_EXTRACT=mysql_params["db_extract"]
DB_LOAD=mysql_params["db_load"]

PATH_LOG_DATA = "log_data/"

# Extract Data
## create connection to mysql db
con = create_db_connection(HOST, USER, PASSWORD, DB_EXTRACT)

## load Customer Data
customer_dim = read_query(con, query_cust)

## load Store Data
store_dim = read_query(con, query_store)

## load Staff Data
staff_dim = read_query(con, query_staff)

## load Film Data
film_dim = read_query(con, query_film)

## load Rental Data
rental_fact = read_query(con, query_rental)

## file_name
cust_filename = "customer"
store_filename = "store"
staff_filename = "staff"
film_filename = "film"
rental_filename = "rental"

## Save the Data
save_tocsv(customer_dim, cust_filename, stage="extract")
save_tocsv(store_dim, store_filename, stage="extract")
save_tocsv(staff_dim, staff_filename, stage="extract")
save_tocsv(film_dim, film_filename, stage="extract")
save_tocsv(rental_fact, rental_filename, stage="extract")
print("extract data successfull")

# Transform Data
## Load params to process the data
customer_data = {"file_name" : PATH_LOG_DATA+"customer_extract.csv", 
                 "task" : ["handle_nan", "parse_date"],
                 "columns" : ["district", "valid_from"],
                 "value_to_fill" : ["unknown"]} 

rental_data = {"file_name" : PATH_LOG_DATA+"rental_extract.csv", 
               "task" : ["parse_date"],
               "columns" : ["rental_date"]}


## clean customer data
data_customer = clean_data(customer_data["file_name"], 
                           list_task=customer_data["task"], 
                           list_column_to_clean=customer_data["columns"], 
                           list_value_to_fill=customer_data["value_to_fill"])
save_tocsv(data_customer, "customer", "transform")

## clean rental data
data_rental = clean_data(rental_data["file_name"], 
                           list_task=rental_data["task"], 
                          list_column_to_clean=rental_data["columns"])
save_tocsv(data_rental, "rental", "transform")
print("Transform data successfull")

# Load Data
## Create Connection to new database 
con = create_db_connection(HOST, USER, PASSWORD, DB_LOAD)
## Params to load data
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

## Load Data to Database with new schema
for data, value in data_to_insert.items():
    insert_data(value["filename"], con, value["table_name"])
    

list_query = [alter_cust, alter_film, alter_store, alter_staff, alter_rental]

## Creata table relation
for query in list_query:
    execute_multi_query(con, query)
print("Load data success")