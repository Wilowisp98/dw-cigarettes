import pandas as pd

from utils import log_wrapper, generate_id


@log_wrapper
def main():
    # Obtaining current working directory (necessary for when running with another folder as "Project" in VSCode)
    current_directory = '\\'.join(__file__.split("\\")[:-1])
    file_name = f'{current_directory}/../datasets/africa_cigarettes_sales.csv'

    # -------------
    # Loading Data
    # -------------
    df = pd.read_csv(file_name, encoding='latin-1', low_memory=False)

    # -------------
    # Treating Data
    # -------------
    df = df.query('Year >= 1000')   # Drop everything which has a incorrect Year
    df = df.dropna(subset=['Year', 'Month', 'Day', 'Dollar_Price', 'Quantity']).copy()      # Drop null values

    # Changing the names of some columns
    df = df.rename(columns = {
        'Outlet_Type': 'Store_Type',
        'Retail_Subtype': 'Store_Subtype',
        'Product': 'Category'
    })

    # Make date a datetime column
    df['Date'] = df['Year'].astype(int).astype(str) + '-' + df['Month'].astype(int).astype(str) + '-'  + df['Day'].astype(int).astype(str)
    df['Date'] = pd.to_datetime(df['Date'])

    # Generating a Product_ID
    df['Category'] = df['Category'].fillna('Cigarette')
    df['Product_ID'] = generate_id(df, ['Category', 'Brand', 'Sub_Brand'])
    df['Store_ID'] = generate_id(df, ['Province', 'Store_ID', 'City', 'Suburb', 'Store_Type', 'Store_Subtype'])  # Generating Store_ID
    df['Fieldworker_ID'] = generate_id(df, ['Fieldworker_Code', 'Store_ID'])   # Generating Fieldworker_ids


    # Rounding every float value to 4 decimal places
    for col in df.columns:
        if df[col].dtype.name.lower().startswith('float'):
            df[col] = df[col].round(4)

    # Generating DIM_TIME
    dim_time = pd.date_range(df['Date'].min().replace(month=1, day=1), df['Date'].max().replace(month=12, day=31))
    dim_time = pd.DataFrame({'Date': dim_time})
    dim_time['Year']  = dim_time['Date'].dt.year
    dim_time['Month'] = dim_time['Date'].dt.month
    dim_time['Day']   = dim_time['Date'].dt.day

    # Month Name + Weekday
    dim_time['Month_Name'] = dim_time['Date'].dt.strftime('%B')
    dim_time['weekday'] = pd.to_datetime(dim_time[['Year', 'Month', 'Day']]).dt.weekday + 1

    # ----------------------------------------------------
    #                   Dimension Tables
    # ----------------------------------------------------
    
    # Generating ID's
    df['Suburb_ID'] = generate_id(df, ['Suburb', 'Province', 'City', 'Country'])    # Creating Suburb IDs
    df['Province_ID'] = generate_id(df, ['Province', 'City', 'Country'])            # Creating Province IDs
    df['City_ID'] = generate_id(df, ['City', 'Country'])                            # Creating City IDs
    df['Country_ID'] = generate_id(df, ['Country'])                                 # Creating Country IDs
    df['Sub_Brand_ID'] = generate_id(df, ['Sub_Brand', 'Brand'])                    # Creating Sub-Brand IDs
    df['Brand_ID'] = generate_id(df, ['Brand'])                                     # Creating Brand Ids
    # For dim_time
    dim_time['Day_ID'] = generate_id(dim_time, ['Date'])                            # Creating Day IDs
    dim_time['Month_ID'] = generate_id(dim_time, ['Month', 'Year'])                 # Creating Month IDs
    dim_time['Year_ID'] = generate_id(dim_time, ['Year'])                                 # Creating Year IDs

    # ------------------
    # Add Day_ID to df
    # ------------------
    df = df.merge(dim_time[['Date', 'Day_ID']], how='left', on='Date')


    # -------------
    # Writing Data
    # -------------
    # df.to_csv(f'{current_directory}/../datasets/cigarettes_treated.csv', index=False)
    df.to_feather(f'{current_directory}/../datasets/cigarettes_treated.feather')
    dim_time.to_feather(f'{current_directory}/../datasets/dim_time.feather')


if __name__ == '__main__':
    main()