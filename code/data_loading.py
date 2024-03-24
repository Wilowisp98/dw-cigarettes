import pandas as pd

df = pd.read_csv('datasets/cigarettes_treated.csv')

# -----------------------------------------------
# ALL OF THIS CAN GO TO THE DATA_TREATMENT SCRIPT
# -----------------------------------------------

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

# Creating Month Name Columns
month_names = {1: 'January', 2: 'February', 3: 'March', 4: 'April', 5: 'May', 6: 'June',
               7: 'July', 8: 'August', 9: 'September', 10: 'October', 11: 'November', 12: 'December'}

df['Month_Name'] = df['Month'].map(month_names)

# Creating Month IDs
df['Month_ID'] = pd.factorize(df['Month'].astype(str) + '_' + df['Year'].astype(str))[0]

# Creating Year IDs
df['Year_ID'] = df['Year'].fillna('no_year')
year_ids = {year_id: index for (index, year_id) in enumerate(df['Year_ID'].unique())}
df['Year_ID'] = df['Year_ID'].map(year_ids)


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

# -------------------------------------------------------------
# TIME TABLE

df_sorted = df.sort_values(by=['Year', 'Month', 'Day'])
columns = ['Day', 'Month_ID', 'Month_Name', 'Year_ID', 'Year']
df_t = df_sorted[columns]
df_t = df_t.drop_duplicates().reset_index(drop=True)

sql_queries = [
    "CREATE TABLE IF NOT EXISTS TIME (Day_ID INT AUTO_INCREMENT, Day INT, Month_ID INT, Month VARCHAR(20), Year_ID INT, Year INT, PRIMARY KEY(Day_ID));"
]

for row in range(len(df_t)):
    ins = f'INSERT INTO TIME (Day, Month_ID, Month, Year_ID, Year) VALUES ({df_t["Day"][row]}, {df_t["Month_ID"][row]},"{df_t["Month_Name"][row]}", {df_t["Year_ID"][row]},{df_t["Year"][row]});'
    sql_queries.append(ins)

with open('time.sql', 'w') as sql_file:

    for query in sql_queries:
        sql_file.write(query + '\n')