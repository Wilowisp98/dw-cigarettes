import data_treatment
import generate_currency
import generate_purchases_and_stocks
import generate_sql_queries
import insert_sql_queries_into_db
import utils

# Setting the random seed to a fixed value so we can reproduce the same results
generate_purchases_and_stocks.np.random.seed(0)


# Execute all scripts
@utils.log_wrapper
def main():
    data_treatment.main()
    generate_currency.main()
    generate_purchases_and_stocks.main()
    generate_sql_queries.main() 
    insert_sql_queries_into_db.main(
        sql_queries_dir='./sql_queries',
        user='root',
        password='root',
        host='localhost'
    )


if __name__ == '__main__':
    main()