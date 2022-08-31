query_cust = """select 
                    c.customer_id,
                    CONCAT(c.first_name, " ", c.last_name) full_name,
                    IF(c.active=1, "Yes", "No") is_active,
                    IFNULL(a.address, a.address2) address,
                    a.district,
                    ci.city,
                    co.country,
                    c.create_date valid_from
                FROM customer c
                    JOIN address a
                        USING(address_id)
                    JOIN city as ci
                        USING(city_id)
                    JOIN country as co
                        USING(country_id);
                """


query_store = """
                select 
                    s.store_id, 
                    a.address store_address, 
                    a.district store_district, 
                    ci.city store_city, 
                    co.country store_country,
                    s.manager_staff_id store_manager_id,
                    CONCAT(st.first_name, " ", st.last_name) store_manager_name
                FROM store s
                    JOIN address a
                        USING(address_id)
                    JOIN city as ci
                        USING(city_id)
                    JOIN country as co
                        USING(country_id)
                    JOIN staff st
                        ON s.manager_staff_id = st.staff_id;
                """


query_staff = """
                SELECT 
                    st.staff_id, 
                    CONCAT(st.first_name, " ", st.last_name) staff_name,
                    s.store_id staff_store_id,
                    IF(st.active=1, "Yes", "No") is_active
                from staff st
                    JOIN store s
                        ON s.manager_staff_id = st.staff_id;
              """


query_film = """
            SELECT
                f.film_id,
                f.title, 
                f.description, 
                f.release_year,
                l.name language,
                f.rental_duration,
                f.rental_rate,
                f.length film_duration,
                f.replacement_cost,
                f.rating,
                c.name category
            FROM film f
                JOIN language l
                    USING(language_id)
                JOIN film_category fc
                    USING(film_id)
                JOIN category c
                    USING(category_id)
                """


query_rental = """
                SELECT 
                    r.customer_id,
                    r.staff_id,
                    i.store_id,
                    i.film_id,
                    r.rental_date,
                    DATEDIFF(r.return_date, r.rental_date) rent_duration,
                    p.amount
                FROM rental r
                    JOIN inventory i
                        USING(inventory_id)
                    JOIN payment p
                        USING(rental_id)
                """