from datetime import datetime
import os
import numpy as np
import pandas as pd
import pickle
from sqlalchemy import create_engine, text as sql_text

from airflow.decorators import dag, task

POSTGRES_USER = 'postgres'
POSTGRES_PASSWORD = 'postgres'
POSTGRES_DB = 'airflow'
POSTGRES_PORT = 5432
POSTGRES_HOST = 'postgres'


def transform_loaded_data(df: pd.DataFrame) -> pd.DataFrame:
    if 'RateCodeID' in df.columns:
        df = df.drop('RateCodeID', axis=1)
    if 'RatecodeID' in df.columns:
        df = df.drop('RatecodeID', axis=1)

    df = df.drop(['tpep_pickup_datetime', 'tpep_dropoff_datetime'], axis=1)
    df['improvement_surcharge'] = df['improvement_surcharge'].fillna(np.mean(df['improvement_surcharge']))

    df = pd.concat([df, pd.get_dummies(df['store_and_fwd_flag'])], axis=1)
    df = df.drop('store_and_fwd_flag', axis=1)

    return df


@dag(
    schedule='@once',
    start_date=datetime(2023, 5, 1),
    catchup=False,
    tags=["ftmodel"],
)
def ftmodel_api():
    @task()
    def tune_model() -> None:
        engine = create_engine(f"postgresql://{POSTGRES_USER}:{POSTGRES_PASSWORD}@{POSTGRES_HOST}:{5432}/{POSTGRES_DB}")
        conn = engine.connect()

        data = pd.concat(
            [
                pd.read_sql('SELECT * FROM dds.taxi_trips_2015_01', conn),
                pd.read_sql('SELECT * FROM dds.taxi_trips_2016_01', conn),
                pd.read_sql('SELECT * FROM dds.taxi_trips_2016_02', conn),
                pd.read_sql('SELECT * FROM dds.taxi_trips_2016_03', conn)
            ],
            axis=0
        )

        data = transform_loaded_data(data)

        X = data.drop('total_amount', axis=1)
        y = data['total_amount']
        del data
        model_name = os.listdir('./model/')[0]
        model_name = f'./model/{model_name}'

        date_now = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")

        with open(model_name, 'rb') as f:
            model = pickle.load(f)

        model.best_estimator_.fit(X, y)

        with open(f'./model/pkl_model_{date_now}.pkl', 'wb') as f:  # ИЗНАЧАЛЬНО ВСЕ СОХРАНЯЕТСЯ В ПАПКУ model
            pickle.dump(model, f)

        return

    tune_model()


ftmodel_api()
