from weather_data.data_getter import get_raw_data
from weather_data.data_processor import make_unique
from config import config
from database_client import SqliteClient

df = get_raw_data()

df = make_unique(df)

df.to_sql(config.weather_data.RAW_WEATHER_DATA_TABLE, SqliteClient.db_engine, if_exists='append', index=False)