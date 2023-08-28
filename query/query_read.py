query_cust = """
SELECT 
    c.customer_id,
    CONCAT(c.first_name, " ", c.last_name) full_name,
    c.email,
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
        USING(country_id);"""

query_date = """SELECT 
    DISTINCT(DATE_FORMAT(payment_date, '%Y-%m-%d')) as date_id,
    DATE_FORMAT(payment_date, '%Y-%m-%d') as date,
    EXTRACT(year FROM payment_date) as year,
    EXTRACT(quarter FROM payment_date) as quarter,
    EXTRACT(month FROM payment_date) as month,
    EXTRACT(day FROM payment_date) as day,
    EXTRACT(week FROM payment_date) as week
FROM payment;"""

query_store = """
                SELECT 
                    s.store_id, 
                    a.address store_address, 
                    a.district store_district, 
                    ci.city store_city, 
                    co.country store_country,
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


query_film = """
            SELECT
                f.film_id,
                f.title, 
                f.description, 
                f.release_year,
                l.name language,
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
                    USING(category_id);
                """


query_sales = """SELECT 
					p.payment_id,
                    DATE_FORMAT(p.payment_date, '%Y-%m-%d') as date_id,
                    p.customer_id,
                    i.store_id,
                    i.film_id,
                    p.amount sales_amount
                FROM payment p
                    JOIN rental r
                        USING(rental_id)
					JOIN inventory i
						USING(inventory_id);
                """