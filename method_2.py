# import libraries
import pandas as pd
import psycopg2
from psycopg2.extras import execute_values
import time

df = pd.read_csv("flo_data_20k.csv") # read csv file from your local

# Establish a connection to your PostgreSQL database
conn = psycopg2.connect(
    dbname='postgres',
    user='postgres',
    password='postgresql',
    host='localhost',
    port='5432'
)

start_time = time.time() # get start time before insert

with conn.cursor() as c:
    execute_values(
        cur=c,
        sql="""
            INSERT INTO test
            (master_id , order_channel , last_order_channel , first_order_date, last_order_date , last_order_date_online , last_order_date_offline, order_num_total_ever_online, order_num_total_ever_offline, customer_value_total_ever_offline, customer_value_total_ever_online, interested_in_categories_12)
            VALUES %s;
            """,
        argslist=df.to_dict(orient="records"),
        template="""
            (
                %(master_id)s, %(order_channel)s, %(last_order_channel)s,
                %(first_order_date)s, %(last_order_date)s, %(last_order_date_online)s,
                %(last_order_date_offline)s, %(order_num_total_ever_online)s,
                %(order_num_total_ever_offline)s, %(customer_value_total_ever_offline)s,
                %(customer_value_total_ever_online)s, %(interested_in_categories_12)s
            )
            """
    )
    conn.commit()

end_time = time.time() # get end time after insert
total_time = end_time - start_time # calculate the time
print(f"Insert time: {total_time} seconds") # print time