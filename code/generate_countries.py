import pandas as pd
import os
import utils


@utils.log_wrapper
def main():
    current_directory = '\\'.join(__file__.split("\\")[:-1])

    
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

    #create countries df
    merged_df = pd.merge(df_filtered, df_main[['Country', 'Country_ID']], left_on='Country Name', right_on='Country', how='left')
    merged_df.drop_duplicates(subset=['Country Name', 'Country Code', 'Year'], inplace=True)
    
    #remove duplicates
    merged_df.drop('Country', axis=1, inplace=True)

    # Put Country_ID on first column
    merged_df = merged_df[['Country_ID', 'Year', 
                           '15_64_female', '15_64_male', 'above_64_female', 'above_64_male', 
                           'pop_female', 'pop_male', 'total_pop']]
    
    merged_df.to_csv(f'{current_directory}/../datasets/countries.csv', index=False)
    merged_df.to_feather(f'{current_directory}/../datasets/countries.feaather')

if __name__ == '__main__':
    main()

