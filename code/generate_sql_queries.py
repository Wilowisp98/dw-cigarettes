import os
import pandas as pd
from utils import log_wrapper, generate_sql


@log_wrapper
def main():
    try:
        df = pd.read_feather('datasets/cigarettes_treated.feather')
    except Exception as e:
        df = pd.read_csv('../datasets/cigarettes_treated.csv')
        df['Date'] = pd.to_datetime(df['Date'])
    df = df.sort_values(by=['Year', 'Month', 'Day'])

    queries_directory = f'{__file__.split("\\")[:-1]}\\..\\sql_queries'

    # -----------------------------------------------
    # ALL OF THIS CAN GO TO THE DATA_TREATMENT SCRIPT
    # -----------------------------------------------
    # Creating Suburb IDs
    df['Suburb_ID'] = df['Suburb'].fillna('no_suburb') + df['Province'].fillna('no_province') + df['City'].fillna('no_city') + df['Country'].fillna('no_country')
    suburb_ids = {suburb_id: index for (index, suburb_id) in enumerate(df['Suburb_ID'].unique())}
    df['Suburb_ID'] = df['Suburb_ID'].map(suburb_ids)

    # Creating Province IDs
    df['Province_ID'] = df['Province'].fillna('no_province') + df['City'].fillna('no_city') + df['Country'].fillna('no_Country')
    province_ids = {province_id: index for (index, province_id) in enumerate(df['Province_ID'].unique())}
    df['Province_ID'] = df['Province_ID'].map(province_ids)

    # Creating City IDs
    df['City_ID'] = df['City'].fillna('no_city') + df['Country'].fillna('no_Country')
    city_ids = {city_id: index for (index, city_id) in enumerate(df['City_ID'].unique())}
    df['City_ID'] = df['City_ID'].map(city_ids)

    # Creating Country IDs
    df['Country_ID'] = df['Country'].fillna('no_Country')
    country_ids = {country_id: index for (index, country_id) in enumerate(df['Country_ID'].unique())}
    df['Country_ID'] = df['Country_ID'].map(country_ids)

    # Creating Month Name Columns
    month_names = {1: 'January', 2: 'February', 3: 'March', 4: 'April', 5: 'May', 6: 'June',
                7: 'July', 8: 'August', 9: 'September', 10: 'October', 11: 'November', 12: 'December'}

    df['Month_Name'] = df['Month'].map(month_names)

    # Creating Month IDs
    df['Day_ID'] = pd.factorize(df['Day'].astype(str) + '_' + df['Month'].astype(str) + '_' + df['Year'].astype(str))[0]

    df['weekday'] = pd.to_datetime(df[['Year', 'Month', 'Day']]).dt.weekday + 1

    # Creating Month IDs
    df['Month_ID'] = pd.factorize(df['Month'].astype(str) + '_' + df['Year'].astype(str))[0]

    df['Month'] = df['Date'].dt.month

    # Creating Year IDs
    df['Year_ID'] = df['Year'].fillna('no_year')
    year_ids = {year_id: index for (index, year_id) in enumerate(df['Year_ID'].unique())}
    df['Year_ID'] = df['Year_ID'].map(year_ids)

    # Creating Sub-brand IDs
    df['Sub_Brand_ID'] = df['Sub_Brand'].fillna('no_sub_brand') + df['Brand'].fillna('no_brand')
    sub_brand_ids = {sub_brand_id: index for (index, sub_brand_id) in enumerate(df['Sub_Brand_ID'].unique())}
    df['Sub_Brand_ID'] = df['Sub_Brand_ID'].map(sub_brand_ids)

    # Creating Brand IDs
    df['Brand_ID'] = df['Brand'].fillna('no_brand')
    brand_ids = {brand_id: index for (index, brand_id) in enumerate(df['Brand_ID'].unique())}
    df['Brand_ID'] = df['Brand_ID'].map(brand_ids)

    # -------------------------------------------------------------
    # STORES TABLE

    columns = ['Store_ID2', 'Outlet_Type', 'Retail_Subtype']
    df_t = df[columns]
    df_t = df_t.drop_duplicates().reset_index(drop=True)

    sql_queries = [
        "CREATE TABLE IF NOT EXISTS DIM_STORE (Store_ID INT,Outlet_Type VARCHAR(50), Retail_Subtype VARCHAR(50), PRIMARY KEY(Store_ID));"
    ]

    for row in range(len(df_t)):
        ins = f'INSERT INTO DIM_STORE (Store_ID, Outlet_Type, Retail_Subtype) VALUES ({df_t["Store_ID2"][row]}, "{df_t["Outlet_Type"][row]}", "{df_t["Retail_Subtype"][row]}");'
        sql_queries.append(ins)

    with open(f'{queries_directory}\\dim_store.sql', 'w') as sql_file:

        for query in sql_queries:
            sql_file.write(query + '\n')

    # -------------------------------------------------------------
    # LOCATION TABLE
            
    columns = ['Suburb_ID', 'Suburb', 'Province_ID', 'Province', 'City_ID', 'City', 'Country_ID', 'Country']
    df_t = df[columns]
    df_t = df_t.drop_duplicates().reset_index(drop=True)

    sql_queries = [
        "CREATE TABLE IF NOT EXISTS DIM_LOCATION (Suburb_ID INT, Suburb VARCHAR(50), Province_ID INT, Province VARCHAR(50), City_ID INT, City VARCHAR(50), Country_ID INT, Country VARCHAR(50), PRIMARY KEY(Suburb_ID));"
    ]

    for row in range(len(df_t)):
        ins = f'INSERT INTO DIM_LOCATION (Suburb_ID, Suburb, Province_ID, Province, City_ID, City, Country_ID, Country) VALUES ({df_t["Suburb_ID"][row]}, "{df_t["Suburb"][row]}", {df_t["Province_ID"][row]},"{df_t["Province"][row]}", {df_t["City_ID"][row]},"{df_t["City"][row]}", {df_t["Country_ID"][row]},"{df_t["Country"][row]}");'
        sql_queries.append(ins)

    with open(f'{queries_directory}\\dim_location.sql', 'w') as sql_file:

        for query in sql_queries:
            sql_file.write(query + '\n')

    # -------------------------------------------------------------
    # TIME TABLE

    columns = ['Day_ID', 'Day', 'weekday', 'Month_ID', 'Month', 'Month_Name', 'Year_ID', 'Year']
    df_t = df[columns]
    df_t = df_t.drop_duplicates().reset_index(drop=True)

    sql_queries = [
        "CREATE TABLE IF NOT EXISTS DIM_TIME (Day_ID INT AUTO_INCREMENT, Day INT, Weekday INT, Month_ID INT, Month INT, Month_Name VARCHAR(20), Year_ID INT, Year INT, PRIMARY KEY(Day_ID));"
    ]

    for row in range(len(df_t)):
        ins = f'INSERT INTO DIM_TIME (Day, Weekday, Month_ID, Month, Month_Name, Year_ID, Year) VALUES ({int(df_t["Day"][row])}, {int(df_t["weekday"][row])}, {int(df_t["Month_ID"][row])}, {int(df_t["Month"][row])}, "{df_t["Month_Name"][row]}", {int(df_t["Year_ID"][row])}, {int(df_t["Year"][row])});'
        sql_queries.append(ins)

    with open(f'{queries_directory}\\dim_time.sql', 'w') as sql_file:

        for query in sql_queries:
            sql_file.write(query + '\n')

    # -------------------------------------------------------------
    # PRODUCT TABLE

    columns = ['Product_ID', 'Product', 'Sub_Brand_ID', 'Sub_Brand', 'Brand_ID', 'Brand']
    df_t = df[columns]
    df_t = df_t.drop_duplicates().reset_index(drop=True)

    sql_queries = [
        "CREATE TABLE IF NOT EXISTS DIM_PRODUCT (Product_ID INT, Product VARCHAR(50), Sub_Brand_ID INT, Sub_Brand VARCHAR(100), Brand_ID INT, Brand VARCHAR(50), PRIMARY KEY(Product_ID));"
    ]

    for row in range(len(df_t)):
        ins = f'INSERT INTO DIM_PRODUCT (Product_ID, Product, Sub_Brand_ID, Sub_Brand, Brand_ID, Brand) VALUES ({df_t["Product_ID"][row]}, "{df_t["Product"][row]}", {df_t["Sub_Brand_ID"][row]},"{df_t["Sub_Brand"][row]}", {df_t["Brand_ID"][row]}, "{df_t["Brand"][row]}");'
        sql_queries.append(ins)

    with open(f'{queries_directory}\\dim_product.sql', 'w') as sql_file:

        for query in sql_queries:
            sql_file.write(query + '\n')

    # -------------------------------------------------------------
    # SALES TABLE

    columns = ['Store_ID2', 'Suburb_ID', 'Day_ID', 'Product_ID', 'Quantity', 'Dollar_Price']
    df_t = df[columns]
    df_t = df_t.drop_duplicates().reset_index(drop=True)

    sql_queries = [
        "CREATE TABLE IF NOT EXISTS SALES (Store_ID INT, Suburb_ID INT, Day_ID INT, Product_ID INT, Quantity INT, Price DECIMAL(5,4), FOREIGN KEY (Store_ID) REFERENCES DIM_STORE(Store_ID), FOREIGN KEY (Suburb_ID) REFERENCES DIM_LOCATION(Suburb_ID), FOREIGN KEY (Day_ID) REFERENCES DIM_TIME(Day_ID), FOREIGN KEY (Product_ID) REFERENCES PRODUCT(Product_ID));"
    ]

    for row in range(len(df_t)):
        ins = f'INSERT INTO SALES (Store_ID, Suburb_ID, Day_ID, Product_ID, Quantity, Price) VALUES ({df_t["Store_ID2"][row]}, {df_t["Suburb_ID"][row]},{df_t["Day_ID"][row]}, {df_t["Product_ID"][row]}, {df_t["Quantity"][row]}, {df_t["Dollar_Price"][row]});'
        sql_queries.append(ins)

    with open(f'{queries_directory}\\sales.sql', 'w') as sql_file:

        for query in sql_queries:
            sql_file.write(query + '\n')

    # -------------------------------------------------------------
    # Purchases Table
    purchases = pd.read_feather('../datasets/purchases.feather') if os.path.isdir('../datasets') else pd.read_feather('datasets/purchases.feather')
    generate_sql(purchases, 'purchases', f'{queries_directory}\\purchases.sql', )

if __name__ == '__main__':
    main()