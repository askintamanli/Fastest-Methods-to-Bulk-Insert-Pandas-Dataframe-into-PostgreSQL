# import libraries
import pandas as pd
from sqlalchemy import create_engine
import time
import csv
from io import StringIO

def psql_insert_copy(table, conn, keys, data_iter): #mehod
    """
    Execute SQL statement inserting data

    Parameters
    ----------
    table : pandas.io.sql.SQLTable
    conn : sqlalchemy.engine.Engine or sqlalchemy.engine.Connection
    keys : list of str
        Column names
    data_iter : Iterable that iterates the values to be inserted
    """
    # gets a DBAPI connection that can provide a cursor
    dbapi_conn = conn.connection
    with dbapi_conn.cursor() as cur:
        s_buf = StringIO()
        writer = csv.writer(s_buf)
        writer.writerows(data_iter)
        s_buf.seek(0)

        columns = ', '.join('"{}"'.format(k) for k in keys)
        if table.schema:
            table_name = '{}.{}'.format(table.schema, table.name)
        else:
            table_name = table.name

        sql = 'COPY {} ({}) FROM STDIN WITH CSV'.format(
            table_name, columns)
        cur.copy_expert(sql=sql, file=s_buf)


df = pd.read_csv("flo_data_20k.csv") # read csv file from your local

# Example: 'postgresql://username:password@localhost:5432/your_database'
engine = create_engine('postgresql://postgres:postgresql@localhost:5432/postgres')

start_time = time.time() # get start time before insert

df.to_sql(
    name="test",
    con=engine,
    if_exists="append",
    index=False,
    method=psql_insert_copy
)

end_time = time.time() # get end time after insert
total_time = end_time - start_time # calculate the time
print(f"Insert time: {total_time} seconds") # print time