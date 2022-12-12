import pandas as pd
from sqlalchemy import create_engine
import logging

if __name__ == '__main__':
    engine = create_engine("postgresql://postgres:password123@localhost:5432/postgres")

    # Extract Data
    companies = pd.read_sql(f"select * from companies", con=engine)

    # Transform Data
    # set dim_country data
    cols = ['office_country_code', 'office_state_code']
    filt_empty_country = companies['office_country_code'] != ''
    dim_country = companies[filt_empty_country][cols].groupby(cols).count().reset_index().reset_index()
    dim_country = dim_country.rename(columns={"index":"country_id"})
    dim_country['country_id'] = dim_country['country_id'] + 1 
    
    # set dim_state data
    cols = ['office_state_code']
    filt_empty_state = companies['office_state_code'] != ''
    dim_state = companies[filt_empty_state][cols].groupby(cols).count().reset_index().reset_index()
    dim_state = dim_state.rename(columns={"index":"state_id"})
    dim_state['state_id'] = dim_state['state_id'] + 1 
    dim_state = dim_state.merge(dim_country, on='office_state_code').drop(columns=['office_country_code'])

    # Load Data
    try:
        res = dim_country.to_sql('dim_country', con=engine, schema='dwh', index=False, if_exists='replace')
        logging.info(f'success insert data to table: dim_country, inserted {res} data')
        res = dim_state.to_sql('dim_state', con=engine, schema='dwh', index=False, if_exists='replace')
        logging.info(f'success insert data to table: dim_state, inserted {res} data')
    except Exception as e:
        logging.error('Failed to insert data to table: dim_state')
        logging.error(e)