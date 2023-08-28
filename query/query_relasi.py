alter_cust =  """ alter table customer_dim
                  add primary key (customer_id); 
              """

alter_film = """
                alter table film_dim
                add primary key (film_id); 
             """

alter_store= """
                alter table store_dim
                add primary key (store_id); 
             """

alter_date_type= """
                ALTER TABLE date_dim MODIFY COLUMN date_id date;
             """

alter_date= """
                alter table date_dim
                add primary key (date_id); 
             """

alter_date_sales_type= """
                ALTER TABLE fact_sales MODIFY COLUMN date_id date;
             """

alter_sales= """
            ALTER TABLE fact_sales
            ADD FOREIGN KEY (customer_id) REFERENCES customer_dim(customer_id),
            ADD FOREIGN KEY (film_id) REFERENCES film_dim(film_id),
            ADD FOREIGN KEY (store_id) REFERENCES store_dim(store_id),
            ADD FOREIGN KEY (date_id) REFERENCES date_dim(date_id);
             """