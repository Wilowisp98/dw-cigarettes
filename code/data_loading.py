import pandas as pd

df = pd.read_csv('datasets/cigarettes_treated.csv')
df = df.sort_values(by=['Year', 'Month', 'Day'])

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

# Creating Month IDs
df['Month_ID'] = pd.factorize(df['Month'].astype(str) + '_' + df['Year'].astype(str))[0]

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
    "CREATE TABLE IF NOT EXISTS STORE (Store_ID INT,Outlet_Type VARCHAR(50), Retail_Subtype VARCHAR(50), PRIMARY KEY(Store_ID));"
]

for row in range(len(df_t)):
    ins = f'INSERT INTO STORE (Store_ID, Outlet_Type, Retail_Subtype) VALUES ({df_t["Store_ID2"][row]}, "{df_t["Outlet_Type"][row]}", "{df_t["Retail_Subtype"][row]}");'
    sql_queries.append(ins)

with open('store.sql', 'w') as sql_file:

    for query in sql_queries:
        sql_file.write(query + '\n')

# -------------------------------------------------------------
# LOCALIZATION TABLE
           
columns = ['Suburb_ID', 'Suburb', 'Province_ID', 'Province', 'City_ID', 'City', 'Country_ID', 'Country']
df_t = df[columns]
df_t = df_t.drop_duplicates().reset_index(drop=True)

sql_queries = [
    "CREATE TABLE IF NOT EXISTS LOCALIZATION (Suburb_ID INT, Suburb VARCHAR(50), Province_ID INT, Province VARCHAR(50), City_ID INT, City VARCHAR(50), Country_ID INT, Country VARCHAR(50), PRIMARY KEY(Suburb_ID));"
]

for row in range(len(df_t)):
    ins = f'INSERT INTO LOCALIZATION (Suburb_ID, Suburb, Province_ID, Province, City_ID, City, Country_ID, Country) VALUES ({df_t["Suburb_ID"][row]}, "{df_t["Suburb"][row]}", {df_t["Province_ID"][row]},"{df_t["Province"][row]}", {df_t["City_ID"][row]},"{df_t["City"][row]}", {df_t["Country_ID"][row]},"{df_t["Country"][row]}");'
    sql_queries.append(ins)

with open('localization.sql', 'w') as sql_file:

    for query in sql_queries:
        sql_file.write(query + '\n')

# -------------------------------------------------------------
# TIME TABLE

columns = ['Day_ID', 'Day', 'Month_ID', 'Month_Name', 'Year_ID', 'Year']
df_t = df[columns]
df_t = df_t.drop_duplicates().reset_index(drop=True)

sql_queries = [
    "CREATE TABLE IF NOT EXISTS TIME (Day_ID INT, Day INT, Month_ID INT, Month VARCHAR(20), Year_ID INT, Year INT, PRIMARY KEY(Day_ID));"
]

for row in range(len(df_t)):
    ins = f'INSERT INTO TIME (Day_ID, Day, Month_ID, Month, Year_ID, Year) VALUES ({df_t["Day_ID"][row]}, {df_t["Day"][row]}, {df_t["Month_ID"][row]},"{df_t["Month_Name"][row]}", {df_t["Year_ID"][row]},{df_t["Year"][row]});'
    sql_queries.append(ins)

with open('time.sql', 'w') as sql_file:

    for query in sql_queries:
        sql_file.write(query + '\n')

# -------------------------------------------------------------
# PRODUCT TABLE

columns = ['Product_ID', 'Product', 'Sub_Brand_ID', 'Sub_Brand', 'Brand_ID', 'Brand']
df_t = df[columns]
df_t = df_t.drop_duplicates().reset_index(drop=True)

sql_queries = [
    "CREATE TABLE IF NOT EXISTS PRODUCT (Product_ID INT, Product VARCHAR(50), Sub_Brand_ID INT, Sub_Brand VARCHAR(100), Brand_ID INT, Brand VARCHAR(50), PRIMARY KEY(Product_ID));"
]

for row in range(len(df_t)):
    ins = f'INSERT INTO PRODUCT (Product_ID, Product, Sub_Brand_ID, Sub_Brand, Brand_ID, Brand) VALUES ({df_t["Product_ID"][row]}, "{df_t["Product"][row]}", {df_t["Sub_Brand_ID"][row]},"{df_t["Sub_Brand"][row]}", {df_t["Brand_ID"][row]}, "{df_t["Brand"][row]}");'
    sql_queries.append(ins)

with open('product.sql', 'w') as sql_file:

    for query in sql_queries:
        sql_file.write(query + '\n')

# -------------------------------------------------------------
# SALES TABLE

columns = ['Store_ID2', 'Suburb_ID', 'Day_ID', 'Product_ID', 'Quantity', 'Dollar_Price']
df_t = df[columns]
df_t = df_t.drop_duplicates().reset_index(drop=True)

sql_queries = [
    "CREATE TABLE IF NOT EXISTS SALES (Store_ID INT, Suburb_ID INT, Day_ID INT, Product_ID INT, Quantity INT, Price DECIMAL(5,4), FOREIGN KEY (Store_ID) REFERENCES STORE(Store_ID), FOREIGN KEY (Suburb_ID) REFERENCES LOCALIZATION(Suburb_ID), FOREIGN KEY (Day_ID) REFERENCES TIME(Day_ID), FOREIGN KEY (Product_ID) REFERENCES PRODUCT(Product_ID));"
]

for row in range(len(df_t)):
    ins = f'INSERT INTO SALES (Store_ID, Suburb_ID, Day_ID, Product_ID, Quantity, Price) VALUES ({df_t["Store_ID2"][row]}, {df_t["Suburb_ID"][row]},{df_t["Day_ID"][row]}, {df_t["Product_ID"][row]}, {df_t["Quantity"][row]}, {df_t["Dollar_Price"][row]});'
    sql_queries.append(ins)

with open('sales.sql', 'w') as sql_file:

    for query in sql_queries:
        sql_file.write(query + '\n')