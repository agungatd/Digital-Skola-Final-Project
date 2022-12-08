import pandas as pd
from sqlalchemy import create_engine
import logging

if __name__ == '__main__':
    engine = create_engine("postgresql://postgres:Sukses37@localhost:5432/postgres")

    # Extract Data
    companies = pd.read_sql(f"select * from companies", con=engine)
    dim_country = pd.read_sql(f"select * from dwh.dim_country", con=engine)

    # Transform Data
    cols = ['state_code', 'country_code']
    dim_state = companies[cols].groupby(cols).count().reset_index().reset_index()
    dim_state = dim_state.rename(columns={"index":"state_id"})
    dim_state = dim_state[dim_state.country_code != '']
    dim_state = dim_state.merge(dim_country, on='country_code').drop(columns='country_code')
    dim_state = dim_state[dim_state.state_code != '']
    dim_state.head(2)

    # Load Data
    try:
        res = dim_state.to_sql('dim_state', con=engine, schema='dwh', index=False, if_exists='replace')
        logging.info(f'success insert data to table: dim_state, inserted {res} data')
    except Exception as e:
        logging.info('Failed to insert data to table: dim_state')
        logging.error(e)