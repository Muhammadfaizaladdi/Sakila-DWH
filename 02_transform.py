from src.wr_data import save_tocsv
from src.wr_data import read_data
from src.wr_data import clean_data

PATH_LOG = "log_data/"

customer_data = {"file_name" : PATH_LOG+"customer_extract.csv", 
                 "task" : ["handle_nan", "parse_date"],
                 "columns" : ["district", "valid_from"],
                 "value_to_fill" : ["unknown"]} 

rental_data = {"file_name" : PATH_LOG+"rental_extract.csv", 
               "task" : ["parse_date"],
               "columns" : ["rental_date"]}


# clean customer data
data_customer = clean_data(customer_data["file_name"], 
                           list_task=customer_data["task"], 
                           list_column_to_clean=customer_data["columns"], 
                           list_value_to_fill=customer_data["value_to_fill"])

save_tocsv(data_customer, "customer", "transform")

# clean rental data
data_rental = clean_data(rental_data["file_name"], 
                           list_task=rental_data["task"], 
                          list_column_to_clean=rental_data["columns"])

save_tocsv(data_rental, "rental", "transform")