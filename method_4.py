# import libraries
import pandas as pd
import time
from io import StringIO
import psycopg2

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

sio = StringIO()
df.to_csv(sio, index=None, header=None)
sio.seek(0)
with conn.cursor() as c:
    c.copy_expert(
        sql="""
        COPY test (
            master_id, 
            order_channel, 
            last_order_channel, 
            first_order_date, 
            last_order_date, 
            last_order_date_online, 
            last_order_date_offline,
            order_num_total_ever_online, 
            order_num_total_ever_offline, 
            customer_value_total_ever_offline,
            customer_value_total_ever_online, 
            interested_in_categories_12
        ) FROM STDIN WITH CSV""",
        file=sio
    )
    conn.commit()

end_time = time.time() # get end time after insert
total_time = end_time - start_time # calculate the time
print(f"Insert time: {total_time} seconds") # print time