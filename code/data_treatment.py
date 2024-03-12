import pandas as pd

file_name = '../datasets/acp-r1-r12-2016-2022-v1.6.csv'


df = pd.read_csv(file_name, encoding='latin-1', low_memory=False)

# Drop everything which has a incorrect Year
df = df.query('Year >= 1000')
df = df.dropna(subset=['Year', 'Month', 'Day'])

df['Date'] = df['Year'].astype(int).astype(str) + '-' + df['Month'].astype(int).astype(str) + '-'  + df['Day'].astype(int).astype(str)
df['Date'] = pd.to_datetime(df['Date'])


df.to_csv('../datasets/cigarettes_treated.csv', index=False)