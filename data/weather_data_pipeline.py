from weather_data.data_getter import get_raw_data
from weather_data.data_processor import make_unique
from config import config
from database_client import SqliteClient


def start_pipeline():
    df = get_raw_data()

    df = make_unique(df)

    print("Saving into sqlite")
    df.to_sql(config.weather_data.RAW_WEATHER_DATA_TABLE, SqliteClient.db_engine, if_exists='append', index=False)

if __name__ == "__main__":
    start_pipeline()