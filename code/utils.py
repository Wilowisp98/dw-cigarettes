import datetime as dt
import pandas as pd
import tqdm

from typing import Optional

def log_wrapper(func):
    def wrapper(*args, **kwargs):
        start = dt.datetime.now()
        print(f'<{start}> [{func.__module__}.{func.__name__}] Running')
        output = func(*args, **kwargs)
        end = dt.datetime.now()
        print(f'<{end}> [{func.__module__}.{func.__name__}] Finished executing. Duration: {end - start}')
        return output
    return wrapper


def generate_sql(df: pd.DataFrame, table_name: str, file_name: str, primary_key: Optional[str] = None, use_tqdm: bool=True) -> None:
    '''
        Generate a SQL query from a given dataframe and save it to a file
        Args:
            df (pd.DataFrame): the dataframe to be converted to a SQL query
            table_name (str): the name of the table to be created
            file_name (str): the path to the file where the SQL query will be saved
    '''
    types_mapping = {
        'int32': 'INT',
        'int64': 'INT',
        'float32': 'FLOAT',
        'float64': 'FLOAT',
        'datetime64[ns]': 'DATE',
        'object': 'VARCHAR(50)',
        'string': 'VARCHAR(50)'
    }
    types_prefixs_suffixes = {'VARCHAR': '"'}
    # Create table statement
    sql_statement = f'CREATE TABLE IF NOT EXISTS {table_name} ('
    for column in df.columns:
        sql_statement += f'\n    {column} {types_mapping[df[column].dtype.name]},'
    sql_statement = f'{sql_statement[:-1]}'
    if primary_key is not None: sql_statement += f',\n    PRIMARY KEY({primary_key})'
    sql_statement += '\n);'

    # Writing the insert statement
    sql_statement += f'\nINSERT INTO {table_name} VALUES (\n'
    iter = tqdm.tqdm(df.iterrows(), total=df.shape[0], desc='Iterating rows') if use_tqdm else df.iterrows()
    for _, row in iter:
        sql_statement += '    ('
        for column in df.columns[:-1]: 
            if types_mapping[df[column].dtype.name].startswith('VARCH') or types_mapping[df[column].dtype.name].startswith('DAT'): 
                sql_statement += f'"{row[column]}", '
            else: sql_statement += f'{row[column]}, '
        sql_statement = f'{sql_statement[:-2]}),\n'
    sql_statement = f'{sql_statement[:-2]}\n);'

    with open(file_name, 'w') as sql_file:
        # Writing the create table statement
        sql_file.write(sql_statement)
    return True