# I want to write a function which takes in the table name, and the csv and inserts into it

from .connector import psql_execute_single
from sqlalchemy import text

def insert_into_table(table_name = '', file_name = ''):
    # assuming it is a CSV file
    container_base_path = '/var/lib/postgresql/static_data/'
    query = text(f"copy {table_name} from '{container_base_path + file_name}' delimiter ',' header csv;")
    print('EXECUTE THIS QUERY IN CONTAINER: \n', query)
    print(query)
    return query
