import pandas as pd
import os

def main():
    current_directory = os.getcwd()

    countries_ds = f'{current_directory}/../datasets/b0bab1c0-d1c7-485c-b639-ee59bc2293f3_Data.csv'
    main_dataset = f'{current_directory}/../datasets/cigarettes_treated.feather'

    df = pd.read_csv(countries_ds)
    df_main = pd.read_feather(main_dataset)

    df.drop('Series Code', axis=1, inplace=True)

    df = pd.melt(df, 
                 id_vars=['Country Name', 'Country Code', 'Series Name'], 
                 value_vars=['2016', '2017', '2018','2019', '2020', '2021', '2022'], 
                 var_name='Year', 
                 value_name='Value')

    df = df.pivot(index=['Country Name', 'Country Code', 'Year'], 
                  columns='Series Name',
                  values='Value')

    df = df.reset_index()

    uniques = df_main['Country'].unique()

    df_filtered = df[df['Country Name'].isin(uniques)]

    df_filtered = df_filtered.rename(columns={'Population ages 15-64, female': '15_64_female'}) 
    df_filtered = df_filtered.rename(columns={'Population ages 15-64, male': '15_64_male'}) 
    df_filtered = df_filtered.rename(columns={'Population ages 65 and above, female': 'above_64_female'}) 
    df_filtered = df_filtered.rename(columns={'Population ages 65 and above, male': 'above_64_male'}) 
    df_filtered = df_filtered.rename(columns={'Population, female': 'pop_female'}) 
    df_filtered = df_filtered.rename(columns={'Population, male': 'pop_male'}) 
    df_filtered = df_filtered.rename(columns={'Population, total': 'total_pop'}) 

    df_filtered = df_filtered.rename_axis(None, axis=1)

    df_filtered.to_csv(f'{current_directory}/../datasets/countries.csv')

if __name__ == '__main__':
    main()