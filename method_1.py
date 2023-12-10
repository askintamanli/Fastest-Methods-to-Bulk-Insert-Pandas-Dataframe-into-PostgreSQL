# import libraries
import pandas as pd
from sqlalchemy import create_engine
import time

df = pd.read_csv("flo_data_20k.csv") # read csv file from your local

# Example: 'postgresql://username:password@localhost:5432/your_database'
engine = create_engine('postgresql://postgres:postgresql@localhost:5432/postgres')

start_time = time.time() # get start time before insert

df.to_sql(
    name="test", # table name
    con=engine,  # engine
    if_exists="append", #  If the table already exists, append
    index=False # no index
)

end_time = time.time() # get end time after insert
total_time = end_time - start_time # calculate the time
print(f"Insert time: {total_time} seconds") # print time