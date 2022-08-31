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

alter_staff= """
                alter table staff_dim
                add primary key (staff_id); 
             """

alter_rental= """
            ALTER TABLE rental_fact
            ADD FOREIGN KEY (customer_id) REFERENCES customer_dim(customer_id),
            ADD FOREIGN KEY (film_id) REFERENCES film_dim(film_id),
            ADD FOREIGN KEY (store_id) REFERENCES store_dim(store_id),
            ADD FOREIGN KEY (staff_id) REFERENCES staff_dim(staff_id);
             """