import pandas as pd
from datetime import datetime, timedelta, date
from sqlalchemy import create_engine
import logging

engine = create_engine("postgresql://postgres:Sukses37@localhost:5432/postgres")

if __name__ == '__main__':
    companies = pd.read_sql(f"select * from companies", con=engine)

    fct_agg_state = companies.groupby('office_state_code').agg({'office_city': 'count', 'office': 'count'}).reset_index()
    fct_agg_state = fct_agg_state[fct_agg_state['office_state_code'] != '']

    try:
        res = fct_agg_state.to_sql('fct_city_office_per_state', con=engine, schema='dwh', index=False, if_exists='replace')
        print(f'success insert data to table: fct_agg_state, inserted {res} data')
    except Exception as e:
        print('Failed to insert data to table: fct_agg_state')
        print(f'ERROR: {e}')