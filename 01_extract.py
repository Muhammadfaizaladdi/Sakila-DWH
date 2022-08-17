from src.sql_con import *
from src.query import *
from src.wr_data import *

HOST = "localhost"
USER = "root"
PASSWORD = "RumahMakan02"
DB = "sakila"


# create connection to mysql db
con = create_db_connection(HOST, USER, PASSWORD, DB)


# load data
# Customer Data
customer_dim = read_query(con, query_cust)

# Store Data
store_dim = read_query(con, query_store)

# Staff Data
staff_dim = read_query(con, query_staff)

# Film Data
film_dim = read_query(con, query_film)

# Rental Data
rental_fact = read_query(con, query_rental)



# Save the Data
# Output Files Name
cust_filename = "customer"
store_filename = "store"
staff_filename = "staff"
film_filename = "film"
rental_filename = "rental"

# Customer Data
save_tocsv(customer_dim, cust_filename, stage="extract")
save_tocsv(store_dim, store_filename, stage="extract")
save_tocsv(staff_dim, staff_filename, stage="extract")
save_tocsv(film_dim, film_filename, stage="extract")
save_tocsv(rental_fact, rental_filename, stage="extract")

print("extract data successfull")