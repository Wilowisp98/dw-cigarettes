import os
import pandas as pd
from utils import log_wrapper, generate_sql


# Obtaining current working directory (necessary for when running with another folder as "Project" in VSCode)
current_directory = '\\'.join(__file__.split("\\")[:-1])


@log_wrapper
def main(db_name = 'dw_cigarettes'):
    df = pd.read_feather(f'{current_directory}/../datasets/cigarettes_treated.feather')
    dim_time = pd.read_feather(f'{current_directory}/../datasets/dim_time.feather')

    nth_query = 0

    df = df.sort_values(by=['Year', 'Month', 'Day'])

    queries_directory = f'{current_directory}\\..\\sql_queries'
    
    # Generate SQL to create Database if not exists
    with open(f'{queries_directory}\\{nth_query:02d}-create_dbase.sql', 'w') as sql_file:
        sql_file.write(f'DROP DATABASE {db_name};\nCREATE DATABASE IF NOT EXISTS {db_name};')
    nth_query += 1

    # -------------------------------------------------------------
    # STORES TABLE
    columns = ['Store_ID', 'Store_Type', 'Store_Subtype']
    df_t = df[columns].drop_duplicates().reset_index(drop=True)
    generate_sql(df_t, f'{db_name}.dim_store', f'{queries_directory}\\{nth_query:02d}-dim_store.sql', insert_every_row=True, primary_key='Store_ID')
    nth_query += 1

    # -------------------------------------------------------------
    # Countries Table
    columns = ['Country_ID', 'Country']
    df_t = df[columns].drop_duplicates().reset_index(drop=True)
    generate_sql(df_t, f'{db_name}.dim_country', f'{queries_directory}\\{nth_query:02d}-dim_country.sql', primary_key='Country_ID', insert_every_row=True)
    nth_query += 1

    # -------------------------------------------------------------
    # LOCATION TABLE
    columns = ['Suburb_ID', 'Suburb', 'Province_ID', 'Province', 'City_ID', 'City', 'Country_ID', 'Country']
    df_t = df[columns].drop_duplicates().reset_index(drop=True)
    foreign_keys = {'Country_ID': 'DIM_COUNTRY(Country_ID)'}
    generate_sql(df_t, f'{db_name}.dim_location', f'{queries_directory}\\{nth_query:02d}-dim_location.sql', insert_every_row=True, primary_key='Suburb_ID', foreign_keys=foreign_keys)
    nth_query += 1

    # -------------------------------------------------------------
    # Year Table
    columns = ['Year_ID', 'Year']
    df_t = dim_time[columns].drop_duplicates().reset_index(drop=True)
    generate_sql(df_t, f'{db_name}.dim_year', f'{queries_directory}\\{nth_query:02d}-dim_year.sql', primary_key='Year_ID', insert_every_row=True)
    nth_query += 1

    # -------------------------------------------------------------
    # TIME TABLE
    columns = ['Day_ID', 'Day', 'weekday', 'Month_ID', 'Month', 'Month_Name', 'Year_ID', 'Year']
    df_t = dim_time[columns].drop_duplicates().reset_index(drop=True)
    foreign_keys = {'Year_ID': 'DIM_YEAR(Year_ID)'}
    generate_sql(df_t, f'{db_name}.dim_time', f'{queries_directory}\\{nth_query:02d}-dim_time.sql', insert_every_row=True, primary_key='Day_ID', foreign_keys=foreign_keys)
    nth_query += 1

    # -------------------------------------------------------------
    # Product TABLE
    columns = ['Product_ID', 'Category', 'Sub_Brand_ID', 'Sub_Brand', 'Brand_ID', 'Brand']
    df_t = df[columns].drop_duplicates().reset_index(drop=True)
    generate_sql(df_t, f'{db_name}.dim_product', f'{queries_directory}\\{nth_query:02d}-dim_product.sql', insert_every_row=True, primary_key='Product_ID')
    nth_query += 1

    # -------------------------------------------------------------
    # SALES TABLE
    columns = ['Store_ID', 'Suburb_ID', 'Day_ID', 'Product_ID', 'Quantity', 'Dollar_Price']
    df_t = df[columns].drop_duplicates().reset_index(drop=True).rename(columns={'Dollar_Price': 'Price'})
    foreign_keys = {
        'Suburb_ID': 'DIM_LOCATION(Suburb_ID)',
        'Day_ID': 'DIM_TIME(Day_ID)',
        'Product_ID': 'DIM_PRODUCT(Product_ID)',
        'Store_ID': 'DIM_STORE(Store_ID)'
    }
    generate_sql(df_t, f'{db_name}.sales', f'{queries_directory}\\{nth_query:02d}-sales.sql', insert_every_row=True, foreign_keys=foreign_keys)
    nth_query += 1

    # -------------------------------------------------------------
    # Purchases Table
    purchases = pd.read_feather(f'{current_directory}/../datasets/purchases.feather')
    purchases = purchases[['Day_ID', 'Store_ID', 'Product_ID', 'Quantity']]
    foreign_keys = {
        'Day_ID': 'DIM_TIME(Day_ID)',
        'Store_ID': 'DIM_STORE(Store_ID)',
        'Product_ID': 'DIM_PRODUCT(Product_ID)'
    }
    generate_sql(purchases, f'{db_name}.purchases', f'{queries_directory}\\{nth_query:02d}-purchases.sql', insert_every_row=True, foreign_keys=foreign_keys)
    nth_query += 1

    # -------------------------------------------------------------
    # Stocks Table
    stocks = pd.read_feather(f'{current_directory}/../datasets/stocks.feather')
    generate_sql(stocks[['Store_ID', 'Product_ID', 'Day_ID', 'stock_qty']], f'{db_name}.stocks', f'{queries_directory}\\{nth_query:02d}-stocks.sql', insert_every_row=True, foreign_keys=foreign_keys)
    nth_query += 1

    # -------------------------------------------------------------
    # Population Table
    foreign_keys = {
        'Country_ID': 'DIM_COUNTRY(Country_ID)',
        'Year_ID': 'DIM_TIME(Year_ID)'
    }
    population = pd.read_feather(f'{current_directory}/../datasets/population.feather')
    generate_sql(population, f'{db_name}.population', f'{queries_directory}\\{nth_query:02d}-population.sql', insert_every_row=True, foreign_keys=foreign_keys)
    nth_query += 1


if __name__ == '__main__':
    main(db_name = 'dw_cigarettes')
