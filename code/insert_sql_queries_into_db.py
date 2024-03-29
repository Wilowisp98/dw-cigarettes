import os
import tqdm
import sqlalchemy   # Requires pymysql
import utils


@utils.log_wrapper
def main(
    sql_queries_dir='../sql_queries',
    user='root',
    password='root',
    host='localhost'
):
    queries = os.listdir(sql_queries_dir)
    queries.sort()
    
    engine = sqlalchemy.create_engine(f'mysql+pymysql://{user}:{password}@{host}/')
    connection = engine.connect()
    for sql_file in tqdm.tqdm(queries, desc='Executing SQL queries'):
        with open(f'{sql_queries_dir}/{sql_file}', 'r') as file:
            queries = file.read()
            # Check if create table exists in the query file, if not ignore it
            if not 'CREATE' in queries: continue
            # if there is more than 1 query, execute them all
            for query in queries.split(';'):
                if len(query) < 5: continue
                query = query.replace(':', '::')  # If there is a ":" in the text, ignore it as SQLAlchemy bugs out
                connection.execute(sqlalchemy.text(query))
    connection.close()