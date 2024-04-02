import pandas as pd
import utils


@utils.log_wrapper
def main():
    current_directory = '\\'.join(__file__.split("\\")[:-1])
    
    # Load datasets
    population_ds = f'{current_directory}/../datasets/yearly_population_data.csv'
    main_dataset = f'{current_directory}/../datasets/cigarettes_treated.feather'
    dim_time = f'{current_directory}/../datasets/dim_time.feather'

    df = pd.read_csv(population_ds)
    df_main = pd.read_feather(main_dataset)
    df_time = pd.read_feather(dim_time)

    # Drop Series Code
    df.drop('Series Code', axis=1, inplace=True)

    # Made column Year from the columns 2016...2022
    df = pd.melt(df, 
                 id_vars=['Country Name', 'Country Code', 'Series Name'], 
                 value_vars=['2016', '2017', '2018','2019', '2020', '2021', '2022'], 
                 var_name='Year', 
                 value_name='Value')

    # Made new columns based on the values of the Series Name column
    df = df.pivot(index=['Country Name', 'Country Code', 'Year'], 
                  columns='Series Name',
                  values='Value')

    df = df.reset_index()

    uniques = df_main['Country'].unique()

    
    df_filtered = df[df['Country Name'].isin(uniques)]

    # Rename columns
    df_filtered = df_filtered.rename(columns={'Population ages 15-64, female': '15_64_female'}) 
    df_filtered = df_filtered.rename(columns={'Population ages 15-64, male': '15_64_male'}) 
    df_filtered = df_filtered.rename(columns={'Population ages 65 and above, female': 'above_64_female'}) 
    df_filtered = df_filtered.rename(columns={'Population ages 65 and above, male': 'above_64_male'}) 
    df_filtered = df_filtered.rename(columns={'Population, female': 'pop_female'}) 
    df_filtered = df_filtered.rename(columns={'Population, male': 'pop_male'}) 
    df_filtered = df_filtered.rename(columns={'Population, total': 'total_pop'}) 

    df_filtered = df_filtered.rename_axis(None, axis=1)

    #create population df
    merged_df = pd.merge(df_filtered, df_main[['Country', 'Country_ID']], left_on='Country Name', right_on='Country', how='left')
    merged_df.drop_duplicates(subset=['Country Name', 'Country Code', 'Year'], inplace=True)
    
    #remove duplicates
    merged_df.drop('Country', axis=1, inplace=True)

    # Add Year_ID to merged_df
    merged_df['Year'] = merged_df['Year'].astype(int)
    merged_df = merged_df.merge(df_time[['Year', 'Year_ID']].drop_duplicates())
    
    # Put Country_ID + Year_ID on first columns
    merged_df = merged_df[['Country_ID', 'Year_ID', '15_64_female', '15_64_male', 'above_64_female', 'above_64_male', 'pop_female', 'pop_male', 'total_pop']]
    
    merged_df.to_csv(f'{current_directory}/../datasets/population.csv', index=False)
    merged_df.to_feather(f'{current_directory}/../datasets/population.feather')

if __name__ == '__main__':
    main()

