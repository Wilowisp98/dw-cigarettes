import pandas as pd

def main():
    # Obtaining current working directory (necessary for when running with another folder as "Project" in VSCode)
    current_directory = '\\'.join(__file__.split("\\")[:-1])
    file_name = f'{current_directory}/../datasets/cigarettes_treated.feather'

    # -------------
    # Loading Data
    # -------------
    df = pd.read_feather(file_name)
    df['Date'] = pd.to_datetime(df['Date'])

    currency_exchange = df.groupby(by=['Date', 'Currency'], as_index=False)['Dollar_Exchange_Rate'].mean().pivot(index='Date', columns=['Currency'], values=['Dollar_Exchange_Rate'])
    currency_exchange.columns = [col[-1] for col in currency_exchange.columns]
    df = pd.date_range(df['Date'].min(), df['Date'].max())
    df = pd.DataFrame(data={'Date': df})
    df = df.merge(currency_exchange.reset_index())
    for col in currency_exchange.columns.difference(['Date']):
        currency_exchange[col] = currency_exchange[col].ffill()
        currency_exchange[col] = currency_exchange[col].bfill()
    # currency_exchange.to_csv(f'{current_directory}/../datasets/currency_exchange.csv')
    currency_exchange.to_feather(f'{current_directory}/../datasets/currency_exchange.feather')


if __name__ == '__main__':
    main()