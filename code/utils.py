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


def generate_id(df: pd.DataFrame, cols_for_id: list) -> pd.Series:
    output = ''
    for col in cols_for_id: output += df[col].astype(str).fillna(f'no_{col.lower()}')
    id_mapping = {id_str: index for (index, id_str) in enumerate(output.unique())}
    output = output.map(id_mapping).astype(int)
    return output


def generate_sql(df: pd.DataFrame, table_name: str, file_name: str, primary_key: Optional[str] = None, use_tqdm: bool=True, insert_every_row: bool=False, drop_table: bool=True) -> None:
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
    with open(file_name, 'w') as sql_file:
        # Create table statement
        if drop_table: sql_file.write(f'DROP TABLE IF EXISTS {table_name};\n')
        sql_file.write(f'CREATE TABLE IF NOT EXISTS {table_name} (')
        for column in df.columns[:-1]:
            sql_file.write(f'\n    {column} {types_mapping[df[column].dtype.name]},')
        column = df.columns[-1]
        sql_file.write(f'\n    {column} {types_mapping[df[column].dtype.name]}')
        if primary_key is not None: sql_file.write(f',\n    PRIMARY KEY({primary_key})')
        sql_file.write('\n);')

        # Writing the insert statement
        if not insert_every_row: sql_file.write( f'\nINSERT INTO {table_name} VALUES (\n')
        else: sql_file.write('\n')
        iter = tqdm.tqdm(df.iloc[:-1].iterrows(), total=df.shape[0], desc='Iterating rows') if use_tqdm else df.iloc[:-1].iterrows()
        for _, row in iter:
            if not insert_every_row: sql_file.write('    (')
            if insert_every_row: sql_file.write( f'INSERT INTO {table_name} VALUES (')
            for column in df.columns[:-1]: 
                if types_mapping[df[column].dtype.name].startswith('VARCH') or types_mapping[df[column].dtype.name].startswith('DAT'): 
                    sql_file.write(f'"{row[column]}", ')
                else: sql_file.write(f'{row[column]}, ')
            column = df.columns[-1]
            if types_mapping[df[column].dtype.name].startswith('VARCH') or types_mapping[df[column].dtype.name].startswith('DAT'): 
                sql_file.write(f'"{row[column]}"')
            else: sql_file.write(f'{row[column]}')
            sql_file.write(')')
            if insert_every_row: sql_file.write(';')
            sql_file.write('\n')

        if not insert_every_row: sql_file.write('    (')
        if insert_every_row: sql_file.write( f'INSERT INTO {table_name} VALUES (')
        row = df.iloc[-1]
        for column in df.columns[:-1]: 
            if types_mapping[df[column].dtype.name].startswith('VARCH') or types_mapping[df[column].dtype.name].startswith('DAT'): 
                sql_file.write(f'"{row[column]}", ')
            else: sql_file.write(f'{row[column]}, ')
        column = df.columns[-1]
        if types_mapping[df[column].dtype.name].startswith('VARCH') or types_mapping[df[column].dtype.name].startswith('DAT'): 
            sql_file.write(f'"{row[column]}"')
        else: sql_file.write(f'{row[column]}')
        sql_file.write(')')
        if insert_every_row: sql_file.write(';')
        sql_file.write('\n')

    return True