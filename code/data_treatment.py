import pandas as pd

# Obtaining current working directory (necessary for when running with another folder as "Project" in VSCode)
current_directory = '\\'.join(__file__.split("\\")[:-1])
file_name = f'{current_directory}/../datasets/acp-r1-r12-2016-2022-v1.6.csv'

# -------------
# Loading Data
# -------------
df = pd.read_csv(file_name, encoding='latin-1', low_memory=False)

# -------------
# Treating Data
# -------------
df = df.query('Year >= 1000')   # Drop everything which has a incorrect Year
df = df.dropna(subset=['Year', 'Month', 'Day'])      # Drop null values
# Make date a datetime column
df['Date'] = df['Year'].astype(int).astype(str) + '-' + df['Month'].astype(int).astype(str) + '-'  + df['Day'].astype(int).astype(str)
df['Date'] = pd.to_datetime(df['Date'])

# Generating a Product_ID
df['Product_ID'] = df['Product'].fillna('Cigarrettes') + df['Brand'].fillna('no_brand') + df['Sub_Brand'].fillna('no_sub_brand')
product_ids = {product: index for (index, product) in enumerate(df['Product_ID'].unique())}
df['Product_ID'] = df['Product_ID'].map(product_ids)

# Fixing Fieldworker_ids
df['Fieldworker_ID'] = df['Fieldworker_Code'].fillna('no_worker_code') + df['Store_ID'].fillna('no_store_id')
fieldworkers_ids = {worker: index for (index, worker) in enumerate(df['Fieldworker_ID'].unique())}
df['Fieldworker_ID'] = df['Fieldworker_ID'].map(fieldworkers_ids)
df = df.drop(columns=['Fieldworker_Code'])

# Fixing Store_ID
# df['Store_ID2'] = df['Province'].fillna('no_province') + df['City'].fillna('no_city') + df['Suburb'].fillna('no_suburb') + df['Outlet_Type'].fillna('no_outlet_type') + df['Retail_Subtype'].fillna('no_oretail_subtype')
df['Store_ID2'] = df['Province'].fillna('no_province') + df['City'].fillna('no_city') + df['Suburb'].fillna('no_suburb') + df['Outlet_Type'].fillna('no_outlet_type')
store_ids = {store_id: index for (index, store_id) in enumerate(df['Store_ID2'].unique())}
df['Store_ID2'] = df['Store_ID2'].map(store_ids)

# -------------
# Writing Data
# -------------
df.to_csv(f'{current_directory}/../datasets/cigarettes_treated.csv', index=False)
df.to_feather(f'{current_directory}/../datasets/cigarettes_treated.feather')
