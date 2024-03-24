import pandas as pd

df = pd.read_csv('datasets/cigarettes_treated.csv')

columns = ['Store_ID2', 'Outlet_Type', 'Retail_Subtype']
df_t = df[columns]
df_t = df_t.drop_duplicates().reset_index(drop=True)

sql_queries = [
    "CREATE TABLE IF NOT EXISTS STORE (StoreID INT, Outlet_Type VARCHAR(50), Retail_Subtype VARCHAR(50));"
]

for row in range(len(df_t)):
    ins = f"INSERT INTO STORE (StoreID, Outlet_Type, Retail_Subtype) VALUES ({df_t['Store_ID2'][row]}, '{df_t['Outlet_Type'][row]}', '{df_t['Retail_Subtype'][row]}');"
    sql_queries.append(ins)

with open('store.sql', 'w') as sql_file:

    for query in sql_queries:
        sql_file.write(query + '\n')

