from pymongo import MongoClient
import pandas as pd
import numpy as np
from sqlalchemy import create_engine
import configparser
import logging

config = configparser.ConfigParser()
config.read('./config.ini')

if __name__ == '__main__':
    # Initiate Mongo cursor
    # user = config['mongo']['user']
    user = 'agung'
    password = 'passwordmongo'
    CONNECTION_STRING = f"mongodb+srv://{user}:{password}@cluster1.4jmnd4n.mongodb.net/?retryWrites=true&w=majority"

    client = MongoClient(CONNECTION_STRING)

    # Get the database and collection
    sample_training_mongo = client['sample_training']

    zip_collection = sample_training_mongo['zips']
    companies_collection = sample_training_mongo['companies']

    # Zips
    cursor_zips = zip_collection.find({}, {'_id': 0})
    zips_df = pd.DataFrame.from_dict(list(cursor_zips))

    logging.info(f"[DATA ZIPS]: {zips_df.head(3)}")

    # flatten loc column to x=long and y=lat
    temp = pd.DataFrame(zips_df['loc'].tolist())
    zips_df = pd.concat([zips_df, temp], axis=1)
    zips_df.drop(['loc'], axis=1, inplace=True)
    zips_df.rename(columns={'x': 'long', 'y': 'lat'}, inplace=True)
    zips_df.head()

    # Companies
    # Excludes all nested columns except offices
    exc_cols = [
        '_id','offices','image', 'products', 'relationships', 'competitions', 'providerships', 
        'funding_rounds', 'investments','acquisition','acquisitions','milestones','video_embeds',
        'screenshots','external_links','partners', 'ipo'
    ]

    # get only the first office
    cursor_comp = companies_collection.aggregate([
        {"$addFields": {
            "office": {"$first": "$offices"}
        }},
        {"$unset" : exc_cols}
    ], allowDiskUse=True)
    companies_df = pd.DataFrame.from_dict(list(cursor_comp))

    # fill empty office value with default empty office dict
    empty_comp = {
        'description': '',
        'address1': '',
        'address2': '',
        'zip_code': '',
        'city': '',
        'state_code': '',
        'country_code': '',
        'latitude': None,
        'longitude': None
    }
    companies_df['office'] = np.where(companies_df['office'].notna(), companies_df['office'], empty_comp)

    # flatten office column
    temp = pd.DataFrame(companies_df['office'].tolist(), )
    companies_df = pd.concat([companies_df, temp], axis=1, )
    companies_df.drop(['office'], axis=1, inplace=True)
    companies_df.head()

    # Load to Postgres
    #connection
    # password = config['postgres']['password']
    password = 'Sukses37'
    url = f'postgresql+psycopg2://postgres:{password}@localhost:5432/postgres'
    engine = create_engine(url)

    try:
        companies_df.to_sql('companies', index=False, con=engine, if_exists='replace')
        zips_df.to_sql('zips', index=False, con=engine, if_exists='replace')
        logging.info(f"Mongo Data has been load to Postgres Database Successfully")
    except Exception as e:
        logging.info(f"Mongo ETL is Failed")
        logging.error(e)