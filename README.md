# Fastest Methods to Bulk Insert Pandas Dataframe into PostgreSQL
 Hello everyone. There are too ways to load data (pandas dataframe) to databases. We are going to compare ways to load pandas dataframe into database. We are going to use PostgreSQL (Local host and version: 16.1) for database.

Methods from fastest to slowest:
1– df.to_sql with method=callable (7.84 seconds)
2- copy_expert and csv ( 8.15 seconds)
3- 25.75 seconds (9.12 seconds)
4- execute_values (25.75 seconds)
5- df.to_sql (30.48 seconds)

If you want to work with me, firstly you should create a table in postgres database
 CREATE TABLE test (
    master_id UUID ,
    order_channel VARCHAR(50),
    last_order_channel VARCHAR(50),
    first_order_date DATE,
    last_order_date DATE,
    last_order_date_online DATE,
    last_order_date_offline DATE,
    order_num_total_ever_online DECIMAL(10, 2),
    order_num_total_ever_offline DECIMAL(10, 2),
    customer_value_total_ever_offline DECIMAL(10, 2),
    customer_value_total_ever_online DECIMAL(10, 2),
    interested_in_categories_12 VARCHAR(100)
);
