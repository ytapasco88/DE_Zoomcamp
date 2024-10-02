
import argparse
import os
from time import time
import pandas as pd
import pyarrow.parquet as pq
from sqlalchemy import create_engine

import warnings
warnings.filterwarnings('ignore')

def main(params):

    user=params.user
    password=params.password
    host=params.host
    port=params.port
    db=params.db
    table_name=params.table_name
    url=params.url

    csv_name = "output.csv"
    
    #os.system(f"wget {url} -O {csv_name}")

    engine = create_engine(f"postgresql://{user}:{password}@{host}:{port}/{db}")
    engine.connect()
    con = engine.raw_connection()

    df_parquet = pd.read_parquet(url)
    df_parquet.to_csv(csv_name, index=False)

    df_iter = pd.read_csv(csv_name, iterator=True, chunksize=100000)
    
    df = next(df_iter)
    
    df.tpep_pickup_datetime = pd.to_datetime(df.tpep_pickup_datetime)
    df.tpep_dropoff_datetime = pd.to_datetime(df.tpep_dropoff_datetime)
    
    # df.head(0).to_sql(name=table_name, con=engine, if_exists="replace")
    # df.to_sql(name=table_name, con=engine, if_exists="append")
    
    df.head(0).to_sql(name=table_name, con=con, if_exists="replace")
    df.to_sql(name=table_name, con=con, if_exists="append")
 

    while True:
        
        t_start = time()

        df = next(df_iter)
        df.tpep_pickup_datetime = pd.to_datetime(df.tpep_pickup_datetime)
        df.tpep_dropoff_datetime = pd.to_datetime(df.tpep_dropoff_datetime)
        
        df.to_sql(name=table_name, con=engine, if_exists="append")

        t_end = time()

        print("Inserted another chunk..., took %.3f seconds" %(t_end - t_start))

if __name__ == "__main__":

    try:
        parser = argparse.ArgumentParser(description='Ingest parquet data, convert to csv and put into Postgresql database')

        parser.add_argument('--user', help="user name for postgres")
        parser.add_argument('--password', help="password for postgres")
        parser.add_argument('--host', help="host for postgres")
        parser.add_argument('--port', help="port for postgres")
        parser.add_argument('--db', help="database name for postgres")
        parser.add_argument('--table_name', help="name of the table where we will write the results to")
        parser.add_argument('--url', help="url of the csv file")
       
        args = parser.parse_args()
        main(args)
        
    except Exception as e:
        print(e)