import argparse

parser = argparse.ArgumentParser(description='Ingest parquet data, convert to csv and put into Postgresql database')

parser.add_argument('--user', help="user name for postgres")
parser.add_argument('--password', help="password for postgres")
parser.add_argument('--host', help="host for postgres")
parser.add_argument('--port', help="port for postgres")
parser.add_argument('--db', help="database name for postgres")
parser.add_argument('--table_name', help="name of the table where we will write the results to")
parser.add_argument('--url', help="url of the csv file")

args = parser.parse_args()
print(args._get_kwargs)