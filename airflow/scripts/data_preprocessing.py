import pandas as pd
from sqlalchemy import create_engine

from sklearn.preprocessing import MinMaxScaler

from sklearn.linear_model import LogisticRegression

import logging

engine = create_engine("postgresql://postgres:password123@localhost:5432/postgres")

if __name__ == '__main__':
    # Read the data
    application_train = pd.read_sql("select * from home_credit_default_risk_application_train", con=engine)
    application_test = pd.read_sql("select * from home_credit_default_risk_application_test", con=engine)

    # drop column that has > 60% null values
    def drop_isna_cols(df):
        isna_cols = df.isna().sum().reset_index()
        isna_cols.rename(columns={0:'isna_sum'}, inplace=True)
        isna_cols = isna_cols[isna_cols.isna_sum > (0.6 * len(df))]['index'].tolist()
        df.drop(columns=isna_cols, inplace=True)
        return df

    application_train = drop_isna_cols(application_train)
    application_test = drop_isna_cols(application_test)

    # one hot encoding categorical columns
    application_train = pd.get_dummies(application_train)
    application_test = pd.get_dummies(application_test)

    # load clean data to database
    try:
        res_train = application_train.to_sql('home_credit_default_risk_application_train_clean', con=engine, if_exists='replace')
        res_test = application_test.to_sql('home_credit_default_risk_application_test_clean', con=engine, if_exists='replace')
        logging.info(f'success insert data to table: home_credit_default_risk_application_train_clean, inserted {res_train} data')
        logging.info(f'success insert data to table: home_credit_default_risk_application_test_clean, inserted {res_test} data')
    except Exception as e:
        logging.info('Failed to insert data to table: fct_currency_daily')
        logging.error(e)

    # Modeling
    train_labels = application_train['TARGET']

    # Align the training and testing data, keep only columns present in both dataframes
    application_train, application_test = application_train.align(application_test, join = 'inner', axis = 1)

    # drop target column
    application_train = application_train.drop(columns = ['TARGET'])

    # Scale each feature to 0-1
    scaler = MinMaxScaler(feature_range = (0, 1))
    scaler.fit(application_train)
    train = scaler.transform(application_train)
    test = scaler.transform(application_test)

    # Create Model
    model_logr = LogisticRegression()

    # Train on the training data
    model_logr.fit(train, train_labels)

    # Make predictions
    # Make sure to select the second column only
    log_reg_pred = model_logr.predict_proba(test)[:, 1]

    # Prediction dataframe
    log_reg_pred_df = application_test[['SK_ID_CURR']]
    log_reg_pred_df['TARGET'] = log_reg_pred

    # Load prediction data to database
    try:
        res = log_reg_pred_df.to_sql('home_credit_default_risk_log_reg_pred_df_clean', con=engine, if_exists='replace')
        logging.info(f'success insert data to table: home_credit_default_risk_log_reg_pred_df_clean, inserted {res} data')
    except Exception as e:
        logging.info('Failed to insert data to table: log_reg_pred_df')
        logging.error(e)