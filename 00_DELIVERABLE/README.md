# Purpose

The purpose of this report is to build and populate a database with data related to [cigarettes sales in Africa](https://www.kaggle.com/datasets/waalbannyantudre/african-cigarette-prices?resource=download) as well as [population data](https://databank.worldbank.org/source/population-estimates-and-projections).

# How to

This project can be successfuly executed by only running the script [run.py](./code/run.py). By opening a comand prompt in the current directory, the following comands should reproduce the wanted results
```cmd
cd ./code
python run.py
```

After the code has finished execution, the MySQL database should be populated with the following databases and tables:
- Database: dw_cigarettes
    - dim_store
    - dim_country
    - dim_location
    - dim_year
    - dim_time
    - dim_product
    - sales
    - purchases
    - stocks
    - population
- Database: dw_cigarettes_analysis
    - sales
    - purchases

Please make sure you follow the necessary requirements.
## Requirements

### MySQL

A MySQL server must be running on your machine (localhost) with a user with the credentials `user=root password=root`.

If this is to be tested with a different database, please change the script [run.py](./code/run.py) and provide the correct host, user and password.

### code
In order to execute this project successfuly, it is necessary to have `python3` installed with the following requirements:
```txt
sqlalchemy
pyarrow
pymysql
pandas
numpy
tqdm
```

