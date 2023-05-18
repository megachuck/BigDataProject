from datetime import datetime
import pandas as pd
from airflow import DAG
from airflow.providers.apache.hdfs.hooks.hdfs_cli import HdfsCliHook
from airflow.operators.bash_operator import BashOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.hive_operator import HiveOperator
from airflow.operators.python_operator import PythonOperator
from airflow.operators.postgres_operator import PostgresOperator
from airflow.operators.bash import BashOperator
from sqlalchemy import create_engine

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2023, 4, 20),
    'retries': 1
}

POSTGRES_USER = 'postgres'
POSTGRES_PASSWORD = 'postgres'
POSTGRES_DB = 'airflow'
POSTGRES_PORT = 5432
POSTGRES_HOST = 'postgres'


dag = DAG('project', default_args=default_args, schedule_interval=None)

hdfs_hook = HdfsCliHook()

start_step = BashOperator(
    task_id='start_step',
    bash_command='echo "С ЛЮБОВЬЮ ОТ БПИ-10-20"',
    dag=dag,
)

postgres_airflow = BashOperator(
    task_id='postgres_airflow',
    bash_command='echo postgres_airflow',
    dag=dag,
)

hdfs_hive_airflow = BashOperator(
    task_id='hdfs_hive_airflow',
    bash_command='echo hdfs_hive_airflow',
    dag=dag,
)


create_schema = PostgresOperator(
    task_id='create_schema',
    sql="CREATE SCHEMA IF NOT EXISTS raw;"
        "CREATE SCHEMA IF NOT EXISTS dds;"
        "CREATE SCHEMA IF NOT EXISTS mart;"
        "CREATE SCHEMA IF NOT EXISTS etl;",
    postgres_conn_id='postgres_default',
    database='airflow',
    dag=dag
)

weather_data = PostgresOperator(
    task_id='weather_data',
    sql="""
        CREATE TABLE IF NOT EXISTS raw.weather_data ( pickup_datetime TIMESTAMP, tempm FLOAT, tempi FLOAT, dewptm FLOAT, dewpti FLOAT, hum FLOAT, wspdm FLOAT, wspdi FLOAT, wgustm FLOAT, wgusti FLOAT, wdird INT, wdire VARCHAR(10), vism FLOAT, visi FLOAT, pressurem FLOAT, pressurei FLOAT, windchillm FLOAT, windchilli FLOAT, heatindexm FLOAT, heatindexi FLOAT, precipm FLOAT, precipi FLOAT, conds VARCHAR(50), icon VARCHAR(50), fog BOOLEAN, rain BOOLEAN, snow BOOLEAN, hail BOOLEAN, thunder BOOLEAN, tornado BOOLEAN);
        CREATE TABLE IF NOT EXISTS dds.weather_data ( pickup_datetime TIMESTAMP, tempm FLOAT, tempi FLOAT, dewptm FLOAT, dewpti FLOAT, hum FLOAT, wspdm FLOAT, wspdi FLOAT, wgustm FLOAT, wgusti FLOAT, wdird INT, wdire VARCHAR(10), vism FLOAT, visi FLOAT, pressurem FLOAT, pressurei FLOAT, windchillm FLOAT, windchilli FLOAT, heatindexm FLOAT, heatindexi FLOAT, precipm FLOAT, precipi FLOAT, conds VARCHAR(50), icon VARCHAR(50), fog BOOLEAN, rain BOOLEAN, snow BOOLEAN, hail BOOLEAN, thunder BOOLEAN, tornado BOOLEAN);
        TRUNCATE TABLE raw.weather_data;
        TRUNCATE TABLE dds.weather_data;
        copy raw.weather_data from '/tmp/datasets/Weather.csv' DELIMITER ',' CSV HEADER;
    """,
    postgres_conn_id='postgres_default',
    database='airflow',
    dag=dag
)

taxi_trips_2015_01 = PostgresOperator(
    task_id='taxi_trips_2015-01',
    sql="""
        CREATE TABLE IF NOT EXISTS raw.taxi_trips_2015_01 (VendorID SMALLINT,tpep_pickup_datetime TIMESTAMP,tpep_dropoff_datetime TIMESTAMP,passenger_count SMALLINT,trip_distance REAL,pickup_longitude REAL,pickup_latitude REAL,RateCodeID SMALLINT,store_and_fwd_flag VARCHAR(1),dropoff_longitude REAL,dropoff_latitude REAL,payment_type SMALLINT,fare_amount REAL,extra REAL,mta_tax REAL,tip_amount REAL,tolls_amount REAL,improvement_surcharge REAL,total_amount REAL);    
        CREATE TABLE IF NOT EXISTS dds.taxi_trips_2015_01 (VendorID SMALLINT,tpep_pickup_datetime TIMESTAMP,tpep_dropoff_datetime TIMESTAMP,passenger_count SMALLINT,trip_distance REAL,pickup_longitude REAL,pickup_latitude REAL,RateCodeID SMALLINT,store_and_fwd_flag VARCHAR(1),dropoff_longitude REAL,dropoff_latitude REAL,payment_type SMALLINT,fare_amount REAL,extra REAL,mta_tax REAL,tip_amount REAL,tolls_amount REAL,improvement_surcharge REAL,total_amount REAL);    
        TRUNCATE TABLE raw.taxi_trips_2015_01;
        TRUNCATE TABLE dds.taxi_trips_2015_01;
        copy raw.taxi_trips_2015_01 from '/tmp/datasets/yellow_tripdata_2015-01.csv' DELIMITER ',' CSV HEADER;
    """,
    postgres_conn_id='postgres_default',
    database='airflow',
    dag=dag
)

taxi_trips_2016_01 = PostgresOperator(
    task_id='taxi_trips_2016_01',
    sql="""
        CREATE TABLE IF NOT EXISTS raw.taxi_trips_2016_01 (VendorID SMALLINT,tpep_pickup_datetime TIMESTAMP,tpep_dropoff_datetime TIMESTAMP,passenger_count SMALLINT,trip_distance REAL,pickup_longitude REAL,pickup_latitude REAL,RateCodeID SMALLINT,store_and_fwd_flag VARCHAR(1),dropoff_longitude REAL,dropoff_latitude REAL,payment_type SMALLINT,fare_amount REAL,extra REAL,mta_tax REAL,tip_amount REAL,tolls_amount REAL,improvement_surcharge REAL,total_amount REAL);    
        CREATE TABLE IF NOT EXISTS dds.taxi_trips_2016_01 (VendorID SMALLINT,tpep_pickup_datetime TIMESTAMP,tpep_dropoff_datetime TIMESTAMP,passenger_count SMALLINT,trip_distance REAL,pickup_longitude REAL,pickup_latitude REAL,RateCodeID SMALLINT,store_and_fwd_flag VARCHAR(1),dropoff_longitude REAL,dropoff_latitude REAL,payment_type SMALLINT,fare_amount REAL,extra REAL,mta_tax REAL,tip_amount REAL,tolls_amount REAL,improvement_surcharge REAL,total_amount REAL);    
        TRUNCATE TABLE raw.taxi_trips_2016_01;
        TRUNCATE TABLE dds.taxi_trips_2016_01;
        copy raw.taxi_trips_2016_01 from '/tmp/datasets/yellow_tripdata_2016-01.csv' DELIMITER ',' CSV HEADER;
        """,
    postgres_conn_id='postgres_default',
    database='airflow',
    dag=dag
)


taxi_trips_2016_02 = PostgresOperator(
    task_id='taxi_trips_2016_02',
    sql="""
        CREATE TABLE IF NOT EXISTS raw.taxi_trips_2016_02 (VendorID SMALLINT,tpep_pickup_datetime TIMESTAMP,tpep_dropoff_datetime TIMESTAMP,passenger_count SMALLINT,trip_distance REAL,pickup_longitude REAL,pickup_latitude REAL,RateCodeID SMALLINT,store_and_fwd_flag VARCHAR(1),dropoff_longitude REAL,dropoff_latitude REAL,payment_type SMALLINT,fare_amount REAL,extra REAL,mta_tax REAL,tip_amount REAL,tolls_amount REAL,improvement_surcharge REAL,total_amount REAL);    
        CREATE TABLE IF NOT EXISTS dds.taxi_trips_2016_02 (VendorID SMALLINT,tpep_pickup_datetime TIMESTAMP,tpep_dropoff_datetime TIMESTAMP,passenger_count SMALLINT,trip_distance REAL,pickup_longitude REAL,pickup_latitude REAL,RateCodeID SMALLINT,store_and_fwd_flag VARCHAR(1),dropoff_longitude REAL,dropoff_latitude REAL,payment_type SMALLINT,fare_amount REAL,extra REAL,mta_tax REAL,tip_amount REAL,tolls_amount REAL,improvement_surcharge REAL,total_amount REAL);    
        TRUNCATE TABLE raw.taxi_trips_2016_02;
        TRUNCATE TABLE dds.taxi_trips_2016_02;
        copy raw.taxi_trips_2016_02 from '/tmp/datasets/yellow_tripdata_2016-02.csv' DELIMITER ',' CSV HEADER;
        """,
    postgres_conn_id='postgres_default',
    database='airflow',
    dag=dag
)

taxi_trips_2016_03 = PostgresOperator(
    task_id='taxi_trips_2016_03',
    sql="""
        CREATE TABLE IF NOT EXISTS raw.taxi_trips_2016_03 (VendorID SMALLINT,tpep_pickup_datetime TIMESTAMP,tpep_dropoff_datetime TIMESTAMP,passenger_count SMALLINT,trip_distance REAL,pickup_longitude REAL,pickup_latitude REAL,RateCodeID SMALLINT,store_and_fwd_flag VARCHAR(1),dropoff_longitude REAL,dropoff_latitude REAL,payment_type SMALLINT,fare_amount REAL,extra REAL,mta_tax REAL,tip_amount REAL,tolls_amount REAL,improvement_surcharge REAL,total_amount REAL);    
        CREATE TABLE IF NOT EXISTS dds.taxi_trips_2016_03 (VendorID SMALLINT,tpep_pickup_datetime TIMESTAMP,tpep_dropoff_datetime TIMESTAMP,passenger_count SMALLINT,trip_distance REAL,pickup_longitude REAL,pickup_latitude REAL,RateCodeID SMALLINT,store_and_fwd_flag VARCHAR(1),dropoff_longitude REAL,dropoff_latitude REAL,payment_type SMALLINT,fare_amount REAL,extra REAL,mta_tax REAL,tip_amount REAL,tolls_amount REAL,improvement_surcharge REAL,total_amount REAL);    
        TRUNCATE TABLE raw.taxi_trips_2016_03;
        TRUNCATE TABLE dds.taxi_trips_2016_03;
        copy raw.taxi_trips_2016_03 from '/tmp/datasets/yellow_tripdata_2016-03.csv' DELIMITER ',' CSV HEADER;
        """,
    postgres_conn_id='postgres_default',
    database='airflow',
    dag=dag
)

traffic_volume_counts = PostgresOperator(
    task_id='traffic_volume_counts',
    sql="""
        CREATE TABLE IF NOT EXISTS raw.traffic_volume_counts ( ID INTEGER, SegmentID INTEGER, "Roadway Name" VARCHAR(255), "From" VARCHAR(255), "To" VARCHAR(255), Direction VARCHAR(255), Date DATE, "12:00-1:00 AM" FLOAT, "1:00-2:00AM" FLOAT, "2:00-3:00AM" FLOAT, "3:00-4:00AM" FLOAT, "4:00-5:00AM" FLOAT, "5:00-6:00AM" FLOAT, "6:00-7:00AM" FLOAT, "7:00-8:00AM" FLOAT, "8:00-9:00AM" FLOAT, "9:00-10:00AM" FLOAT, "10:00-11:00AM" FLOAT, "11:00-12:00PM" FLOAT, "12:00-1:00PM" FLOAT, "1:00-2:00PM" FLOAT, "2:00-3:00PM" FLOAT, "3:00-4:00PM" FLOAT, "4:00-5:00PM" FLOAT, "5:00-6:00PM" FLOAT, "6:00-7:00PM" FLOAT, "7:00-8:00PM" FLOAT, "8:00-9:00PM" FLOAT, "9:00-10:00PM" FLOAT, "10:00-11:00PM" FLOAT, "11:00-12:00AM" FLOAT);
        CREATE TABLE IF NOT EXISTS dds.traffic_volume_counts ( ID INTEGER, SegmentID INTEGER, "Roadway Name" VARCHAR(255), "From" VARCHAR(255), "To" VARCHAR(255), Direction VARCHAR(255), Date DATE, "12:00-1:00 AM" FLOAT, "1:00-2:00AM" FLOAT, "2:00-3:00AM" FLOAT, "3:00-4:00AM" FLOAT, "4:00-5:00AM" FLOAT, "5:00-6:00AM" FLOAT, "6:00-7:00AM" FLOAT, "7:00-8:00AM" FLOAT, "8:00-9:00AM" FLOAT, "9:00-10:00AM" FLOAT, "10:00-11:00AM" FLOAT, "11:00-12:00PM" FLOAT, "12:00-1:00PM" FLOAT, "1:00-2:00PM" FLOAT, "2:00-3:00PM" FLOAT, "3:00-4:00PM" FLOAT, "4:00-5:00PM" FLOAT, "5:00-6:00PM" FLOAT, "6:00-7:00PM" FLOAT, "7:00-8:00PM" FLOAT, "8:00-9:00PM" FLOAT, "9:00-10:00PM" FLOAT, "10:00-11:00PM" FLOAT, "11:00-12:00AM" FLOAT);
        TRUNCATE TABLE raw.traffic_volume_counts;
        TRUNCATE TABLE dds.traffic_volume_counts;
        copy raw.traffic_volume_counts from '/tmp/datasets/Traffic_Volume_Counts.csv' DELIMITER ',' CSV HEADER;
        """,
    postgres_conn_id='postgres_default',
    database='airflow',
    dag=dag
)


load_data_to_dds = PostgresOperator(
    task_id='load_data_to_dds',
    sql="""CREATE OR REPLACE FUNCTION etl.load_data_to_dds(raw_table_name text, dds_table_name text) RETURNS void 
            LANGUAGE plpgsql AS $function$ BEGIN EXECUTE 'INSERT INTO dds.' || dds_table_name || ' SELECT * FROM raw.' || 
            raw_table_name; END; $function$;
        """,
    postgres_conn_id='postgres_default',
    database='airflow',
    dag=dag
)

load_weather_data_dds = PostgresOperator(
    task_id='load_weather_data_dds',
    sql="SELECT etl.load_data_to_dds('weather_data', 'weather_data');",
    postgres_conn_id='postgres_default',
    database='airflow',
    dag=dag
)


load_taxi_trips_2015_01_dds = PostgresOperator(
    task_id='load_taxi_trips_2015_01_dds',
    sql="SELECT etl.load_data_to_dds('taxi_trips_2015_01', 'taxi_trips_2015_01');",
    postgres_conn_id='postgres_default',
    database='airflow',
    dag=dag
)


load_taxi_trips_2016_01_dds = PostgresOperator(
    task_id='load_taxi_trips_2016_01_dds',
    sql="SELECT etl.load_data_to_dds('taxi_trips_2016_01', 'taxi_trips_2016_01');",
    postgres_conn_id='postgres_default',
    database='airflow',
    dag=dag
)


load_taxi_trips_2016_02_dds = PostgresOperator(
    task_id='load_taxi_trips_2016_02_dds',
    sql="SELECT etl.load_data_to_dds('taxi_trips_2016_02', 'taxi_trips_2016_02');",
    postgres_conn_id='postgres_default',
    database='airflow',
    dag=dag
)


load_taxi_trips_2016_03_dds = PostgresOperator(
    task_id='load_taxi_trips_2016_03_dds',
    sql="SELECT etl.load_data_to_dds('taxi_trips_2016_03', 'taxi_trips_2016_03');",
    postgres_conn_id='postgres_default',
    database='airflow',
    dag=dag
)


load_traffic_volume_counts_dds = PostgresOperator(
    task_id='load_traffic_volume_counts_dds',
    sql="SELECT etl.load_data_to_dds('traffic_volume_counts', 'traffic_volume_counts');",
    postgres_conn_id='postgres_default',
    database='airflow',
    dag=dag
)


engine = create_engine(f"postgresql://{POSTGRES_USER}:{POSTGRES_PASSWORD}@{POSTGRES_HOST}:{5432}/{POSTGRES_DB}")
conn = engine.connect()


def load_datasets():
    traffic_df = pd.read_sql('SELECT * FROM dds.traffic_volume_counts', conn)
    weather_df = pd.read_sql('SELECT * FROM dds.weather_data', conn)

    tripdata = pd.concat(
        [
            pd.read_sql('SELECT * FROM dds.taxi_trips_2015_01', conn),
            pd.read_sql('SELECT * FROM dds.taxi_trips_2016_01', conn),
            pd.read_sql('SELECT * FROM dds.taxi_trips_2016_02', conn),
            pd.read_sql('SELECT * FROM dds.taxi_trips_2016_03', conn)
        ],
        axis=0
    )
    tripdata = tripdata[['tpep_pickup_datetime', 'total_amount']]

    return traffic_df, weather_df, tripdata


def traffic_df_edit(traffic_df: pd.DataFrame) -> pd.DataFrame:
    dates = traffic_df['Date']
    traffic_df['Date'] = [datetime(int(dates[x][-4:]),
                                   int(dates[x][:2]),
                                   int(dates[x][3:5])
                                   ).date() for x in range(traffic_df.shape[0])]

    traffic_res = pd.concat([traffic_df['Date'], traffic_df.iloc[:, 7:].sum(axis=1)], axis=1)
    traffic_res.columns = ['date', 'sum_car']

    return traffic_res


def weather_df_edit(weather_df: pd.DataFrame) -> pd.DataFrame:
    dates = weather_df['pickup_datetime']
    dates = [datetime(int(dates[x][:4]),  # YYYY
                      int(dates[x][5:7]),  # MM
                      int(dates[x][8:10])  # DD
                      ).date() for x in range(weather_df.shape[0])]

    weather_df['pickup_datetime'] = dates
    weather_res = weather_df.dropna(subset='precipm')[['pickup_datetime', 'precipm', 'tempm']]
    weather_res.columns = ['date', 'precipm', 'average_temp']

    return weather_res


def join_weather_traffic(weather_res: pd.DataFrame, traffic_res: pd.DataFrame) -> pd.DataFrame:
    joined_df = weather_res.join(traffic_res.set_index('date'), on='date'). \
        dropna(). \
        reset_index(drop=True). \
        set_index('date')

    joined_df = pd.concat(
        [
            joined_df['precipm'].groupby('date').mean(),
            joined_df['average_temp'].groupby('date').mean(),
            joined_df['sum_car'].groupby('date').sum()
        ],
        axis=1
    )

    return joined_df


def tripdata_edit(tripdata: pd.DataFrame) -> pd.DataFrame:
    dates = tripdata['tpep_pickup_datetime'].values
    dates = [datetime(int(dates[x][:4]),  # YYYY
                      int(dates[x][5:7]),  # MM
                      int(dates[x][8:10])  # DD
                      ).date() for x in range(tripdata.shape[0])]

    tripdata['tpep_pickup_datetime'] = dates
    tripdata.columns = ['date', 'sum_trip']
    tripdata = tripdata.groupby('date').count()

    return tripdata


def get_mart() -> None:
    traffic_df, weather_df, tripdata = load_datasets()

    traffic_df = traffic_df_edit(traffic_df)
    weather_df = weather_df_edit(weather_df)
    joined_df = join_weather_traffic(traffic_df, weather_df)
    tripdata = tripdata_edit(tripdata)

    final_df = joined_df.join(tripdata, on='date').dropna(subset='sum_trip')
    final_df.to_sql('mart', engine, if_exists='replace')


load_mart = PythonOperator(
    task_id='load_data_to_mart',
    python_callable=get_mart,
    dag=dag
)

hive_create = HiveOperator(
    task_id='hive_create',
    hql="""
        CREATE DATABASE IF NOT EXISTS ods;
        CREATE DATABASE IF NOT EXISTS archive;
    """,
    dag=dag,
)

hive_weather_data = HiveOperator(
    task_id='hive_weather_data',
    hql="""
    CREATE TABLE IF NOT EXISTS ods.weather_data ( pickup_datetime TIMESTAMP, tempm FLOAT, tempi FLOAT, dewptm FLOAT, dewpti FLOAT, hum FLOAT, wspdm FLOAT, wspdi FLOAT, wgustm FLOAT, wgusti FLOAT, wdird INT, wdire STRING, vism FLOAT, visi FLOAT, pressurem FLOAT, pressurei FLOAT, windchillm FLOAT, windchilli FLOAT, heatindexm FLOAT, heatindexi FLOAT, precipm FLOAT, precipi FLOAT, conds STRING, icon STRING, fog BOOLEAN, rain BOOLEAN, snow BOOLEAN, hail BOOLEAN, thunder BOOLEAN, tornado BOOLEAN )
    row format delimited
    fields terminated by ','
    lines terminated by '\n'
    stored as textfile location 'hdfs://namenode:8020/user/hive/warehouse/ods.db/weather_data';
    CREATE TABLE IF NOT EXISTS archive.weather_data ( pickup_datetime TIMESTAMP, tempm FLOAT, tempi FLOAT, dewptm FLOAT, dewpti FLOAT, hum FLOAT, wspdm FLOAT, wspdi FLOAT, wgustm FLOAT, wgusti FLOAT, wdird INT, wdire STRING, vism FLOAT, visi FLOAT, pressurem FLOAT, pressurei FLOAT, windchillm FLOAT, windchilli FLOAT, heatindexm FLOAT, heatindexi FLOAT, precipm FLOAT, precipi FLOAT, conds STRING, icon STRING, fog BOOLEAN, rain BOOLEAN, snow BOOLEAN, hail BOOLEAN, thunder BOOLEAN, tornado BOOLEAN )
    row format delimited
    fields terminated by ','
    lines terminated by '\n'
    stored as textfile location 'hdfs://namenode:8020/user/hive/warehouse/archive.db/weather_data';
    """,
    dag=dag,
)

hive_taxi_trips_2015_01 = HiveOperator(
    task_id='hive_taxi_trips_2015_01',
    hql="""
    CREATE TABLE IF NOT EXISTS ods.taxi_trips_2015_01 ( VendorID SMALLINT, tpep_pickup_datetime TIMESTAMP, tpep_dropoff_datetime TIMESTAMP, passenger_count SMALLINT, trip_distance FLOAT, pickup_longitude FLOAT, pickup_latitude FLOAT, RateCodeID SMALLINT, store_and_fwd_flag STRING, dropoff_longitude FLOAT, dropoff_latitude FLOAT, payment_type SMALLINT, fare_amount FLOAT, extra FLOAT, mta_tax FLOAT, tip_amount FLOAT, tolls_amount FLOAT, improvement_surcharge FLOAT, total_amount FLOAT )
    row format delimited
    fields terminated by ','
    lines terminated by '\n'
    stored as textfile location 'hdfs://namenode:8020/user/hive/warehouse/ods.db/taxi_trips_2015_01';
    CREATE TABLE IF NOT EXISTS archive.taxi_trips_2015_01 ( VendorID SMALLINT, tpep_pickup_datetime TIMESTAMP, tpep_dropoff_datetime TIMESTAMP, passenger_count SMALLINT, trip_distance FLOAT, pickup_longitude FLOAT, pickup_latitude FLOAT, RateCodeID SMALLINT, store_and_fwd_flag STRING, dropoff_longitude FLOAT, dropoff_latitude FLOAT, payment_type SMALLINT, fare_amount FLOAT, extra FLOAT, mta_tax FLOAT, tip_amount FLOAT, tolls_amount FLOAT, improvement_surcharge FLOAT, total_amount FLOAT )
    row format delimited
    fields terminated by ','
    lines terminated by '\n'
    stored as textfile location 'hdfs://namenode:8020/user/hive/warehouse/archive.db/taxi_trips_2015_01';
    """,
    dag=dag,
)

hive_taxi_trips_2016_01 = HiveOperator(
    task_id='hive_taxi_trips_2016_01',
    hql="""
    CREATE TABLE IF NOT EXISTS ods.taxi_trips_2016_01 ( VendorID SMALLINT, tpep_pickup_datetime TIMESTAMP, tpep_dropoff_datetime TIMESTAMP, passenger_count SMALLINT, trip_distance FLOAT, pickup_longitude FLOAT, pickup_latitude FLOAT, RateCodeID SMALLINT, store_and_fwd_flag STRING, dropoff_longitude FLOAT, dropoff_latitude FLOAT, payment_type SMALLINT, fare_amount FLOAT, extra FLOAT, mta_tax FLOAT, tip_amount FLOAT, tolls_amount FLOAT, improvement_surcharge FLOAT, total_amount FLOAT )
    row format delimited
    fields terminated by ','
    lines terminated by '\n'
    stored as textfile location 'hdfs://namenode:8020/user/hive/warehouse/ods.db/taxi_trips_2016_01';
    CREATE TABLE IF NOT EXISTS archive.taxi_trips_2016_01 ( VendorID SMALLINT, tpep_pickup_datetime TIMESTAMP, tpep_dropoff_datetime TIMESTAMP, passenger_count SMALLINT, trip_distance FLOAT, pickup_longitude FLOAT, pickup_latitude FLOAT, RateCodeID SMALLINT, store_and_fwd_flag STRING, dropoff_longitude FLOAT, dropoff_latitude FLOAT, payment_type SMALLINT, fare_amount FLOAT, extra FLOAT, mta_tax FLOAT, tip_amount FLOAT, tolls_amount FLOAT, improvement_surcharge FLOAT, total_amount FLOAT )
    row format delimited
    fields terminated by ','
    lines terminated by '\n'
    stored as textfile location 'hdfs://namenode:8020/user/hive/warehouse/archive.db/taxi_trips_2016_01';
    """,
    dag=dag,
)

hive_taxi_trips_2016_02 = HiveOperator(
    task_id='hive_taxi_trips_2016_02',
    hql="""
    CREATE TABLE IF NOT EXISTS ods.taxi_trips_2016_02 ( VendorID SMALLINT, tpep_pickup_datetime TIMESTAMP, tpep_dropoff_datetime TIMESTAMP, passenger_count SMALLINT, trip_distance FLOAT, pickup_longitude FLOAT, pickup_latitude FLOAT, RateCodeID SMALLINT, store_and_fwd_flag STRING, dropoff_longitude FLOAT, dropoff_latitude FLOAT, payment_type SMALLINT, fare_amount FLOAT, extra FLOAT, mta_tax FLOAT, tip_amount FLOAT, tolls_amount FLOAT, improvement_surcharge FLOAT, total_amount FLOAT )
    row format delimited
    fields terminated by ','
    lines terminated by '\n'
    stored as textfile location 'hdfs://namenode:8020/user/hive/warehouse/ods.db/taxi_trips_2016_02';
    CREATE TABLE IF NOT EXISTS archive.taxi_trips_2016_02 ( VendorID SMALLINT, tpep_pickup_datetime TIMESTAMP, tpep_dropoff_datetime TIMESTAMP, passenger_count SMALLINT, trip_distance FLOAT, pickup_longitude FLOAT, pickup_latitude FLOAT, RateCodeID SMALLINT, store_and_fwd_flag STRING, dropoff_longitude FLOAT, dropoff_latitude FLOAT, payment_type SMALLINT, fare_amount FLOAT, extra FLOAT, mta_tax FLOAT, tip_amount FLOAT, tolls_amount FLOAT, improvement_surcharge FLOAT, total_amount FLOAT )
    row format delimited
    fields terminated by ','
    lines terminated by '\n'
    stored as textfile location 'hdfs://namenode:8020/user/hive/warehouse/archive.db/taxi_trips_2016_02';
    """,
    dag=dag,
)

hive_taxi_trips_2016_03 = HiveOperator(
    task_id='hive_taxi_trips_2016_03',
    hql="""
    CREATE TABLE IF NOT EXISTS ods.taxi_trips_2016_03 ( VendorID SMALLINT, tpep_pickup_datetime TIMESTAMP, tpep_dropoff_datetime TIMESTAMP, passenger_count SMALLINT, trip_distance FLOAT, pickup_longitude FLOAT, pickup_latitude FLOAT, RateCodeID SMALLINT, store_and_fwd_flag STRING, dropoff_longitude FLOAT, dropoff_latitude FLOAT, payment_type SMALLINT, fare_amount FLOAT, extra FLOAT, mta_tax FLOAT, tip_amount FLOAT, tolls_amount FLOAT, improvement_surcharge FLOAT, total_amount FLOAT )
    row format delimited
    fields terminated by ','
    lines terminated by '\n'
    stored as textfile location 'hdfs://namenode:8020/user/hive/warehouse/ods.db/taxi_trips_2016_03';
    CREATE TABLE IF NOT EXISTS archive.taxi_trips_2016_03 ( VendorID SMALLINT, tpep_pickup_datetime TIMESTAMP, tpep_dropoff_datetime TIMESTAMP, passenger_count SMALLINT, trip_distance FLOAT, pickup_longitude FLOAT, pickup_latitude FLOAT, RateCodeID SMALLINT, store_and_fwd_flag STRING, dropoff_longitude FLOAT, dropoff_latitude FLOAT, payment_type SMALLINT, fare_amount FLOAT, extra FLOAT, mta_tax FLOAT, tip_amount FLOAT, tolls_amount FLOAT, improvement_surcharge FLOAT, total_amount FLOAT )
    row format delimited
    fields terminated by ','
    lines terminated by '\n'
    stored as textfile location 'hdfs://namenode:8020/user/hive/warehouse/archive.db/taxi_trips_2016_03';
    """,
    dag=dag,
)

hive_traffic_volume_counts = HiveOperator(
    task_id='hive_traffic_volume_counts',
    hql="""
    CREATE TABLE IF NOT EXISTS ods.traffic_volume_counts ( ID INT, SegmentID INT, `Roadway Name` STRING, `From` STRING, `To` STRING, Direction STRING, Date DATE, `12:00-1:00` AM FLOAT, `1:00-2:00AM` FLOAT, `2:00-3:00AM` FLOAT, `3:00-4:00AM` FLOAT, `4:00-5:00AM` FLOAT, `5:00-6:00AM` FLOAT, `6:00-7:00AM` FLOAT, `7:00-8:00AM` FLOAT, `8:00-9:00AM` FLOAT, `9:00-10:00AM` FLOAT, `10:00-11:00AM` FLOAT, `11:00-12:00PM` FLOAT, `12:00-1:00PM` FLOAT, `1:00-2:00PM` FLOAT, `2:00-3:00PM` FLOAT, `3:00-4:00PM` FLOAT, `4:00-5:00PM` FLOAT, `5:00-6:00PM` FLOAT, `6:00-7:00PM` FLOAT, `7:00-8:00PM` FLOAT, `8:00-9:00PM` FLOAT, `9:00-10:00PM` FLOAT, `10:00-11:00PM` FLOAT, `11:00-12:00AM` FLOAT )
    row format delimited
    fields terminated by ','
    lines terminated by '\n'
    stored as textfile location 'hdfs://namenode:8020/user/hive/warehouse/ods.db/traffic_volume_counts';
    CREATE TABLE IF NOT EXISTS archive.traffic_volume_counts ( ID INT, SegmentID INT, `Roadway Name` STRING, `From` STRING, `To` STRING, Direction STRING, Date DATE, `12:00-1:00` AM FLOAT, `1:00-2:00AM` FLOAT, `2:00-3:00AM` FLOAT, `3:00-4:00AM` FLOAT, `4:00-5:00AM` FLOAT, `5:00-6:00AM` FLOAT, `6:00-7:00AM` FLOAT, `7:00-8:00AM` FLOAT, `8:00-9:00AM` FLOAT, `9:00-10:00AM` FLOAT, `10:00-11:00AM` FLOAT, `11:00-12:00PM` FLOAT, `12:00-1:00PM` FLOAT, `1:00-2:00PM` FLOAT, `2:00-3:00PM` FLOAT, `3:00-4:00PM` FLOAT, `4:00-5:00PM` FLOAT, `5:00-6:00PM` FLOAT, `6:00-7:00PM` FLOAT, `7:00-8:00PM` FLOAT, `8:00-9:00PM` FLOAT, `9:00-10:00PM` FLOAT, `10:00-11:00PM` FLOAT, `11:00-12:00AM` FLOAT )
    row format delimited
    fields terminated by ','
    lines terminated by '\n'
    stored as textfile location 'hdfs://namenode:8020/user/hive/warehouse/archive.db/traffic_volume_counts';
    """,
    dag=dag,
)

hdfs_weather_data = BashOperator(
    task_id='hdfs_weather_data',
    bash_command=hdfs_hook.run('hdfs dfs -copyFromLocal /tmp/datasets/Weather.csv /user/hive/warehouse/ods.db/weather_data'),
    dag=dag
)

hdfs_taxi_trips_2015_01 = BashOperator(
    task_id='hdfs_taxi_trips_2015_01',
    bash_command=hdfs_hook.run('hdfs dfs -copyFromLocal /tmp/datasets/yellow_tripdata_2015_01.csv /user/hive/warehouse/ods.db/taxi_trips_2015_01'),
    dag=dag
)

hdfs_taxi_trips_2016_01 = BashOperator(
    task_id='hdfs_taxi_trips_2016_01',
    bash_command=hdfs_hook.run('hdfs dfs -copyFromLocal /tmp/datasets/yellow_tripdata_2016_01.csv /user/hive/warehouse/ods.db/taxi_trips_2016_01'),
    dag=dag
)

hdfs_taxi_trips_2016_02 = BashOperator(
    task_id='hdfs_taxi_trips_2016_02',
    bash_command=hdfs_hook.run('hdfs dfs -copyFromLocal /tmp/datasets/yellow_tripdata_2016_02.csv /user/hive/warehouse/ods.db/taxi_trips_2016_02'),
    dag=dag
)

hdfs_hdfs_taxi_trips_2016_03 = BashOperator(
    task_id='hdfs_hdfs_taxi_trips_2016_03',
    bash_command=hdfs_hook.run('hdfs dfs -copyFromLocal /tmp/datasets/yellow_tripdata_2016_03.csv /user/hive/warehouse/ods.db/taxi_trips_2016_03'),
    dag=dag
)

hdfs_traffic_volume_counts = BashOperator(
    task_id='hdfs_traffic_volume_counts',
    bash_command=hdfs_hook.run('hdfs dfs -copyFromLocal /tmp/datasets/Traffic_Volume_Counts.csv /user/hive/warehouse/ods.db/traffic_volume_counts'),
    dag=dag
)

hive_insert_weather_data = HiveOperator(
    task_id='hive_insert_weather_data',
    hql="""
    INSERT INTO archive.weather_data SELECT * FROM ods.weather_data;
    """,
    dag=dag,
)

hive_insert_taxi_trips_2015_01 = HiveOperator(
    task_id='hive_insert_taxi_trips_2015_01',
    hql="""
    INSERT INTO archive.taxi_trips_2015_01 SELECT * FROM ods.taxi_trips_2015_01;
    """,
    dag=dag,
)

hive_insert_hive_taxi_trips_2016_01 = HiveOperator(
    task_id='hive_insert_hive_taxi_trips_2016_01',
    hql="""
    INSERT INTO archive.taxi_trips_2016_01 SELECT * FROM ods.taxi_trips_2016_01;
    """,
    dag=dag,
)

hive_insert_hive_taxi_trips_2016_02 = HiveOperator(
    task_id='hive_insert_hive_taxi_trips_2016_02',
    hql="""
    INSERT INTO archive.taxi_trips_2016_02 SELECT * FROM ods.taxi_trips_2016_02;
    """,
    dag=dag,
)

hive_insert_taxi_trips_2016_03 = HiveOperator(
    task_id='hive_insert_taxi_trips_2016_03',
    hql="""
    INSERT INTO archive.taxi_trips_2016_03 SELECT * FROM ods.taxi_trips_2016_03;
    """,
    dag=dag,
)

hive_insert_traffic_volume_counts = HiveOperator(
    task_id='hive_insert_traffic_volume_counts',
    hql="""
    INSERT INTO archive.traffic_volume_counts SELECT * FROM ods.traffic_volume_counts;
    """,
    dag=dag,
)


start_step >> postgres_airflow >> create_schema >> weather_data >> load_data_to_dds >> load_weather_data_dds >> load_mart
start_step >> postgres_airflow >> create_schema >> taxi_trips_2015_01 >> load_data_to_dds >> load_taxi_trips_2015_01_dds >> load_mart
start_step >> postgres_airflow >> create_schema >> taxi_trips_2016_01 >> load_data_to_dds >> load_taxi_trips_2016_01_dds >> load_mart
start_step >> postgres_airflow >> create_schema >> taxi_trips_2016_02 >> load_data_to_dds >> load_taxi_trips_2016_02_dds >> load_mart
start_step >> postgres_airflow >> create_schema >> taxi_trips_2016_03 >> load_data_to_dds >> load_taxi_trips_2016_03_dds >> load_mart
start_step >> postgres_airflow >> create_schema >> traffic_volume_counts >> load_data_to_dds >> load_traffic_volume_counts_dds >> load_mart
start_step >> hdfs_hive_airflow >> hive_create >> hive_weather_data >> hdfs_weather_data >> hive_insert_weather_data
start_step >> hdfs_hive_airflow >> hive_create >> hive_taxi_trips_2015_01 >> hdfs_taxi_trips_2015_01 >> hive_insert_taxi_trips_2015_01
start_step >> hdfs_hive_airflow >> hive_create >> hive_taxi_trips_2016_01 >> hdfs_taxi_trips_2016_01 >> hive_insert_hive_taxi_trips_2016_01
start_step >> hdfs_hive_airflow >> hive_create >> hive_taxi_trips_2016_02 >> hdfs_taxi_trips_2016_02 >> hive_insert_hive_taxi_trips_2016_02
start_step >> hdfs_hive_airflow >> hive_create >> hive_taxi_trips_2016_03 >> hdfs_hdfs_taxi_trips_2016_03 >> hive_insert_taxi_trips_2016_03
start_step >> hdfs_hive_airflow >> hive_create >> hive_traffic_volume_counts >> hdfs_traffic_volume_counts >> hive_insert_traffic_volume_counts