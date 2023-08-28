import src.sql_con as sc
import src.wr_data as wd
import query.query_read as qrd
import query.query_relasi as qrl
from src.yaml_loader import yaml_loader
from sqlalchemy import text

# Load parameters
params_file_path = "query/sql_params.yml"

mysql_params = yaml_loader(params_file_path)
print(mysql_params)
print(type(mysql_params))

HOST=mysql_params["host"]
USER=mysql_params["user"]
PASSWORD=mysql_params["password"]
DB_EXTRACT=mysql_params["db_extract"]
DB_LOAD=mysql_params["db_load"]

PATH_LOG_DATA = "log_data/"

# Extract Data
## create connection to mysql db
con = sc.create_db_connection(HOST, USER, PASSWORD, DB_EXTRACT)

## load Date Data
date_dim = sc.read_query(con, text(qrd.query_date))

## load Customer Data
customer_dim = sc.read_query(con, text(qrd.query_cust))

## load Store Data
store_dim = sc.read_query(con, text(qrd.query_store))

## load Film Data
film_dim = sc.read_query(con, text(qrd.query_film))

## load Rental Data
fact_sales = sc.read_query(con, text(qrd.query_sales))

## file_name
cust_filename = "customer"
store_filename = "store"
date_filename = "date"
film_filename = "film"
sales_filename = "sales"

## Save the Data
wd.save_tocsv(customer_dim, cust_filename, stage="extract")
wd.save_tocsv(store_dim, store_filename, stage="extract")
wd.save_tocsv(date_dim, date_filename, stage="extract")
wd.save_tocsv(film_dim, film_filename, stage="extract")
wd.save_tocsv(fact_sales, sales_filename, stage="extract")
print("extract data successfull")

# Transform Data
## Load params to process the data
customer_data = {"file_name" : PATH_LOG_DATA+"customer_extract.csv", 
                 "task" : ["handle_nan", "parse_date"],
                 "columns" : ["district", "valid_from"],
                 "value_to_fill" : ["unknown"]} 


## clean customer data
data_customer = wd.clean_data(customer_data["file_name"], 
                           list_task=customer_data["task"], 
                           list_column_to_clean=customer_data["columns"], 
                           list_value_to_fill=customer_data["value_to_fill"])
wd.save_tocsv(data_customer, "customer", "transform")

# Load Data
## Create Connection to new database 
con = sc.create_db_connection(HOST, USER, PASSWORD, DB_LOAD)
## Params to load data
data_to_insert = {
                    "customer":{"filename" : "customer_transform.csv",
                              "table_name" : "customer_dim"
                             },
                    "film":{"filename" : "film_extract.csv",
                              "table_name" : "film_dim"
                             },
                    "date":{"filename" : "date_extract.csv",
                              "table_name" : "date_dim"
                             },
                    "store":{"filename" : "store_extract.csv",
                              "table_name" : "store_dim"
                             },
                    "sales":{"filename" : "sales_extract.csv",
                              "table_name" : "fact_sales"
                             }
                }

## Load Data to Database with new schema
for data, value in data_to_insert.items():
    wd.insert_data(value["filename"], con, value["table_name"])
    

list_query = [qrl.alter_cust, 
              qrl.alter_film, 
              qrl.alter_store, 
              qrl.alter_date_type, 
              qrl.alter_date, 
              qrl.alter_date_sales_type, 
              qrl.alter_sales]

## Creata table relation
for query in list_query:
    sc.execute_multi_query(con, text(query))
print("Load data success")