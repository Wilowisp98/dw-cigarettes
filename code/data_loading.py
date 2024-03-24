import pandas as pd

df = pd.read_csv('datasets/cigarettes_treated.csv')

# Creating Province IDs
df['Province_ID'] = df['Province'].fillna('no_province')
province_ids = {province_id: index for (index, province_id) in enumerate(df['Province_ID'].unique())}
df['Province_ID'] = df['Province_ID'].map(province_ids)

# Creating City IDs
df['City_ID'] = df['City'].fillna('no_city')
city_ids = {city_id: index for (index, city_id) in enumerate(df['City_ID'].unique())}
df['City_ID'] = df['City_ID'].map(city_ids)

# Creating Country IDs
df['Country_ID'] = df['Country'].fillna('no_country')
country_ids = {country_id: index for (index, country_id) in enumerate(df['Country_ID'].unique())}
df['Country_ID'] = df['Country_ID'].map(country_ids)

# -------------------------------------------------------------
# STORES TABLE

columns = ['Store_ID2', 'Outlet_Type', 'Retail_Subtype']
df_t = df[columns]
df_t = df_t.drop_duplicates().reset_index(drop=True)

sql_queries = [
    "CREATE TABLE IF NOT EXISTS STORE (Store_ID INT,Outlet_Type VARCHAR(50), Retail_Subtype VARCHAR(50), PRIMARY KEY(StoreID));"
]

for row in range(len(df_t)):
    ins = f'INSERT INTO STORE (Store_ID, Outlet_Type, Retail_Subtype) VALUES ({df_t["Store_ID2"][row]}, "{df_t["Outlet_Type"][row]}", "{df_t["Retail_Subtype"][row]}");'
    sql_queries.append(ins)

with open('store.sql', 'w') as sql_file:

    for query in sql_queries:
        sql_file.write(query + '\n')

# -------------------------------------------------------------
# LOCALIZATION TABLE
           
columns = ['Suburb', 'Province_ID', 'Province', 'City_ID', 'City', 'Country_ID', 'Country']
df_t = df[columns]
df_t = df_t.drop_duplicates().reset_index(drop=True)

sql_queries = [
    "CREATE TABLE IF NOT EXISTS LOCALIZATION (Suburb_ID INT AUTO_INCREMENT, Suburb VARCHAR(50), Province_ID INT, Province VARCHAR(50), City_ID INT, City VARCHAR(50), Country_ID INT, Country VARCHAR(50), PRIMARY KEY(Suburb_ID));"
]

for row in range(len(df_t)):
    ins = f'INSERT INTO LOCALIZATION (Suburb, Province_ID, Province, City_ID, City, Country_ID, Country) VALUES ("{df_t["Suburb"][row]}", {df_t["Province_ID"][row]},"{df_t["Province"][row]}", {df_t["City_ID"][row]},"{df_t["City"][row]}", {df_t["Country_ID"][row]},"{df_t["Country"][row]}");'
    sql_queries.append(ins)

with open('localization.sql', 'w') as sql_file:

    for query in sql_queries:
        sql_file.write(query + '\n')
