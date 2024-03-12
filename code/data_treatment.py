import pandas as pd

# Obtaining current working directory (necessary for when running with another folder as "Project" in VSCode)
current_directory = __file__.split("\\")[:-1]
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

# -------------
# Writing Data
# -------------
df.to_csv(f'{current_directory}/../datasets/cigarettes_treated.csv', index=False)